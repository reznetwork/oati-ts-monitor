from __future__ import annotations

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


def chunk_addresses(addresses: List[int], max_span: int = 100) -> List[Tuple[int, int]]:
    """Split sorted unique addresses into (start, count) spans for Modbus reads."""
    uniq = sorted(set(addresses))
    if not uniq:
        return []
    spans: List[Tuple[int, int]] = []
    start = uniq[0]
    prev = uniq[0]
    for addr in uniq[1:]:
        if addr - start + 1 <= max_span and addr == prev + 1:
            prev = addr
            continue
        spans.append((start, prev - start + 1))
        start = addr
        prev = addr
    spans.append((start, prev - start + 1))
    return spans


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


class PassthroughReader:
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
    ):
        if ModbusTcpClient is None:
            raise RuntimeError("pymodbus is required; install with: python3 -m pip install -r requirements.txt")
        self.host = host
        self.port = int(port)
        self.unit = int(unit)
        self.timeout = float(timeout)
        self.coils_fallback = bool(coils_fallback)
        self.unit_candidates = list(unit_candidates or [unit])
        self.verbose = bool(verbose)
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
                bits, err = self._read_bits(probe_addr, 1, func)
                if bits is not None:
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

        for start, count in chunk_addresses(addresses):
            bits, err = self._read_bits(start, count, self.func)
            if bits is not None:
                for offset, bit in enumerate(bits):
                    addr = start + offset
                    if addr in out:
                        out[addr] = bool(bit)
                continue

            # Fall back to single-address reads for sparse gaps.
            self.last_error = err
            if self.verbose:
                print(f"span read failed, retrying individually @{start} count={count}: {err}", flush=True)
            for addr in range(start, start + count):
                if addr not in out:
                    continue
                one, one_err = self._read_bits(addr, 1, self.func)
                if one is not None:
                    out[addr] = bool(one[0])
                elif one_err:
                    self.last_error = one_err
        return out

    def read_entries(self, entries: List[PassthroughEntry]) -> List[Tuple[PassthroughEntry, Optional[bool]]]:
        values = self.read_addresses([e.address for e in entries])
        return [(entry, values.get(entry.address)) for entry in entries]
