from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

try:
    from pymodbus.client import ModbusTcpClient
except ModuleNotFoundError:  # pragma: no cover
    ModbusTcpClient = None

from daemon_services import call_bits


@dataclass(frozen=True)
class PassthroughEntry:
    group: str
    address: int
    source: Optional[str] = None


def build_vehicle_sources(vehicle) -> Dict[str, str]:
    """Map passthrough group name -> human-readable source for one vehicle."""
    out: Dict[str, str] = {}
    for ctrl in vehicle.controllers:
        cname = ctrl.name
        for p in ctrl.points:
            if not p.passthrough:
                continue
            out[p.passthrough] = f"{cname} / {p.label} (ref {p.ref})"
        for ref, group in ctrl.passthrough_gears.items():
            label = ctrl.gear_points.get(ref, str(ref))
            out[group] = f"{cname} / gear {label} (ref {ref})"
        for ref, group in ctrl.passthrough_extra.items():
            label = ctrl.extra_points.get(ref, str(ref))
            out[group] = f"{cname} / {label} (ref {ref})"
    return out


def _format_modbus_error(rr: object, func: str, start: int, count: int) -> str:
    code = getattr(rr, "exception_code", None)
    if code == 1:
        meaning = "illegal function"
    elif code == 2:
        meaning = "illegal data address"
    elif code == 3:
        meaning = "illegal data value"
    else:
        meaning = "modbus exception"
    return f"{func} read @{start} count={count}: {meaning} (code={code})"


def _is_transient_error(err: Optional[str]) -> bool:
    if not err:
        return False
    transient_markers = (
        "No response received",
        "ModbusIOException",
        "Connection",
        "timed out",
        "Broken pipe",
        "reset by peer",
    )
    lowered = err.lower()
    return any(marker.lower() in lowered for marker in transient_markers)


class PassthroughReader:
    READ_MODE_MULTI = "multi"
    READ_MODE_SINGLE = "single"

    def __init__(
        self,
        host: str,
        port: int,
        unit: int,
        timeout: float,
        *,
        coils_fallback: bool = True,
        unit_candidates: Optional[List[int]] = None,
        verbose: bool = False,
        read_retries: int = 3,
        read_pause_sec: float = 0.03,
        read_mode: str = READ_MODE_MULTI,
    ):
        if ModbusTcpClient is None:
            raise RuntimeError("pymodbus is required; install with: python3 -m pip install -r requirements.txt")
        if read_mode not in {self.READ_MODE_MULTI, self.READ_MODE_SINGLE}:
            raise ValueError(f"unsupported passthrough read mode: {read_mode}")
        self.host = host
        self.port = int(port)
        self.unit = int(unit)
        self.timeout = float(timeout)
        self.coils_fallback = bool(coils_fallback)
        self.unit_candidates = list(unit_candidates or [unit])
        self.verbose = bool(verbose)
        self.read_retries = max(1, int(read_retries))
        self.read_pause_sec = max(0.0, float(read_pause_sec))
        self.read_mode = read_mode
        self.client = ModbusTcpClient(host, port=self.port, timeout=self.timeout)
        self.connected = False
        self.func: Optional[str] = None
        self.last_error: Optional[str] = None

    def connect(self) -> bool:
        try:
            self.connected = bool(self.client.connect())
            if not self.connected:
                self.last_error = "connect() failed"
            return self.connected
        except Exception as e:
            self.connected = False
            self.last_error = f"connect error: {e.__class__.__name__}: {e}"
            return False

    def _reconnect(self) -> bool:
        try:
            self.client.close()
        except Exception:
            pass
        self.connected = False
        return self.connect()

    def close(self) -> None:
        try:
            self.client.close()
        finally:
            self.connected = False

    def _read_bits(self, start: int, count: int, func: str) -> Tuple[Optional[List[bool]], Optional[str]]:
        method = self.client.read_discrete_inputs if func == "di" else self.client.read_coils
        rr, err = call_bits(method, start, count, self.unit)
        if rr is None:
            return None, err or "read failed"
        if hasattr(rr, "isError") and rr.isError():  # type: ignore[attr-defined]
            return None, _format_modbus_error(rr, func, start, count)
        return list(rr.bits[:count]), None  # type: ignore[attr-defined]

    def _read_one(self, addr: int, func: str) -> Tuple[Optional[bool], Optional[str]]:
        last_err: Optional[str] = None
        for attempt in range(self.read_retries):
            if not self.connected and not self.connect():
                return None, self.last_error
            bits, err = self._read_bits(addr, 1, func)
            if bits is not None:
                return bool(bits[0]), None
            last_err = err
            if _is_transient_error(err) and attempt + 1 < self.read_retries:
                if self.verbose:
                    print(
                        f"retry @{addr} attempt {attempt + 2}/{self.read_retries}: {err}",
                        flush=True,
                    )
                self._reconnect()
                time.sleep(self.read_pause_sec * (attempt + 1))
                continue
            break
        return None, last_err

    def _read_range(self, start: int, count: int, func: str) -> Tuple[Optional[List[bool]], Optional[str]]:
        last_err: Optional[str] = None
        for attempt in range(self.read_retries):
            if not self.connected and not self.connect():
                return None, self.last_error
            bits, err = self._read_bits(start, count, func)
            if bits is not None:
                return bits, None
            last_err = err
            if _is_transient_error(err) and attempt + 1 < self.read_retries:
                if self.verbose:
                    print(
                        f"retry @{start} count={count} attempt {attempt + 2}/{self.read_retries}: {err}",
                        flush=True,
                    )
                self._reconnect()
                time.sleep(self.read_pause_sec * (attempt + 1))
                continue
            break
        return None, last_err

    def probe(self, addresses: List[int]) -> bool:
        if not addresses:
            self.func = "di"
            return True
        if not self.connected and not self.connect():
            return False

        probe_addr = sorted(set(addresses))[0]
        for uid in self.unit_candidates:
            self.unit = int(uid)
            for func in ("di", "coils"):
                if func == "coils" and not self.coils_fallback:
                    continue
                value, err = self._read_one(probe_addr, func)
                if value is not None:
                    self.func = func
                    self.last_error = None
                    if self.verbose:
                        print(f"probe ok: unit={self.unit} func={func} address={probe_addr}", flush=True)
                    return True
                if self.verbose:
                    print(f"probe failed: unit={self.unit} func={func} address={probe_addr}: {err}", flush=True)

        self.last_error = err or "probe failed (no response)"
        return False

    def read_addresses(self, addresses: List[int]) -> Dict[int, Optional[bool]]:
        out: Dict[int, Optional[bool]] = {int(a): None for a in addresses}
        if not addresses:
            return out
        if self.func is None and not self.probe(addresses):
            return out
        assert self.func is not None

        unique_addresses = sorted(set(addresses))
        if self.read_mode == self.READ_MODE_SINGLE:
            for addr in unique_addresses:
                value, err = self._read_one(addr, self.func)
                if value is not None:
                    out[addr] = value
                elif err:
                    self.last_error = err
                    if self.verbose:
                        print(f"read failed @{addr}: {err}", flush=True)
                if self.read_pause_sec:
                    time.sleep(self.read_pause_sec)
            return out

        start = unique_addresses[0]
        count = unique_addresses[-1] - start + 1
        bits, err = self._read_range(start, count, self.func)
        if bits is None:
            if err:
                self.last_error = err
                if self.verbose:
                    print(f"range read failed @{start} count={count}: {err}", flush=True)
            return out

        for addr in unique_addresses:
            out[addr] = bool(bits[addr - start])
        return out

    def read_entries(self, entries: List[PassthroughEntry]) -> List[Tuple[PassthroughEntry, Optional[bool]]]:
        values = self.read_addresses([e.address for e in entries])
        return [(entry, values.get(entry.address)) for entry in entries]
