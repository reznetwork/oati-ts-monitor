"""Modbus client helpers and connection guard."""
from __future__ import annotations

import logging
import time
from collections import deque
from typing import Dict, List, Optional, Tuple

try:
    from pymodbus.client import ModbusTcpClient
except ModuleNotFoundError:  # pragma: no cover
    ModbusTcpClient = None

from core.config import MAX_MODBUS_REF_SPAN
from services.system import check_tcp

CLIENT_CONNECT_WINDOW_SEC = 60.0
CLIENT_CONNECT_MAX_PER_WINDOW = 5

class ConnectionGuard:
    """
    Prevent controller session exhaustion caused by fast reconnect loops.

    In addition to the existing Poller-level rebuild limiter, this guard rate-limits
    low-level TCP connect attempts per controller host.
    """

    def __init__(
        self,
        *,
        host: str,
        modbus_port: int,
        window_sec: float = CLIENT_CONNECT_WINDOW_SEC,
        max_per_window: int = CLIENT_CONNECT_MAX_PER_WINDOW,
        http_port: int = 80,
        http_probe_timeout: float = 0.8,
    ):
        self.host = host
        self.modbus_port = int(modbus_port)
        self.window_sec = float(window_sec)
        self.max_per_window = int(max_per_window)
        self.http_port = int(http_port)
        self.http_probe_timeout = float(http_probe_timeout)
        self._connect_times: deque = deque()

    def allow_connect(self) -> bool:
        now = time.monotonic()
        cutoff = now - self.window_sec
        while self._connect_times and self._connect_times[0] < cutoff:
            self._connect_times.popleft()
        if len(self._connect_times) >= self.max_per_window:
            return False
        self._connect_times.append(now)
        return True

    def log_connect_failure(self, logger: logging.Logger, detail: str) -> None:
        """
        If the controller's HTTP UI is reachable while Modbus connect fails, emit a WARNING
        (common cause: session limit reached while web UI is open). Otherwise emit ERROR.
        """

        try:
            ok_http, _lat_ms, _why = check_tcp(self.host, self.http_port, self.http_probe_timeout)
        except Exception:
            ok_http = False

        if ok_http:
            logger.warning(
                "Modbus connect failed to %s:%s (%s) while HTTP %s:%s is reachable; "
                "controller session limit may be exhausted (web UI open?)",
                self.host,
                self.modbus_port,
                detail,
                self.host,
                self.http_port,
            )
        else:
            logger.error(
                "Modbus connect failed to %s:%s (%s)",
                self.host,
                self.modbus_port,
                detail,
            )



def refs_to_block(refs: List[int], base: int) -> Tuple[int, int]:
    addresses = sorted([ref - base for ref in refs])
    return addresses[0], addresses[-1] - addresses[0] + 1


def chunk_refs_by_span(refs: List[int], max_span: int) -> List[List[int]]:
    """
    Split refs into chunks where (max(ref) - min(ref) + 1) <= max_span.
    This bounds the Modbus 'count' computed by refs_to_block().
    """
    uniq = sorted(set(refs))
    if not uniq:
        return []
    chunks: List[List[int]] = []
    start = uniq[0]
    cur: List[int] = [start]
    for r in uniq[1:]:
        if r - start + 1 <= max_span:
            cur.append(r)
        else:
            chunks.append(cur)
            start = r
            cur = [r]
    chunks.append(cur)
    return chunks


def logical_state(invert: bool, raw: Optional[bool]) -> Optional[bool]:
    if raw is None:
        return None
    return (not raw) if invert else raw


def call_bits(method, address: int, count: int, unit: int):
    attempts = (
        ("device_id", lambda: method(address, count=count, device_id=unit)),
        ("slave", lambda: method(address, count=count, slave=unit)),
        ("unit", lambda: method(address, count=count, unit=unit)),
        ("default", lambda: method(address, count=count)),
    )
    errors: List[str] = []
    for label, call in attempts:
        try:
            return call(), None
        except TypeError as e:
            errors.append(f"{label}({e})")
        except Exception as e:
            return None, f"{label} call error: {e.__class__.__name__}: {e}"
    return None, "kw variants failed: " + "; ".join(errors)


def call_write_coil(method, address: int, value: bool, unit: int):
    try:
        return method(address, value=value, slave=unit), None
    except TypeError as e1:
        try:
            return method(address, value=value, unit=unit), None
        except TypeError as e2:
            try:
                return method(address, value=value), None
            except Exception as e3:
                return None, f"kw variants failed: slave({e1}); unit({e2}); no-unit({e3.__class__.__name__}: {e3})"
    except Exception as e:
        return None, f"call error: {e.__class__.__name__}: {e}"


class MBClient:
    def __init__(
        self,
        host: str,
        port: int,
        timeout: float,
        unit_candidates: List[int],
        use_coils_fallback: bool,
        *,
        guard: Optional[ConnectionGuard] = None,
        logger: Optional[logging.Logger] = None,
    ):
        if ModbusTcpClient is None:
            raise RuntimeError("pymodbus is required to run Modbus polling")
        self.host = host
        self.port = port
        self.timeout = timeout
        self.unit_candidates = unit_candidates[:]
        self.use_coils_fallback = use_coils_fallback
        self.client = ModbusTcpClient(host, port=port, timeout=timeout)
        self.connected = False
        self.unit: Optional[int] = None
        self.func: str = "di"
        self.last_error: Optional[str] = None
        self.guard = guard or ConnectionGuard(host=str(host), modbus_port=int(port))
        self.logger = logger or logging.getLogger("modbus")

    def connect(self) -> bool:
        if not self.guard.allow_connect():
            self.last_error = (
                f"connect rate-limited ({self.guard.max_per_window} per {int(self.guard.window_sec)}s)"
            )
            self.logger.warning(
                "Modbus connect rate-limited for %s:%s (%s)",
                self.host,
                self.port,
                self.last_error,
            )
            self.connected = False
            return False
        try:
            self.connected = self.client.connect()
            if not self.connected:
                self.last_error = "connect() failed"
                self.guard.log_connect_failure(self.logger, self.last_error)
            return self.connected
        except Exception as e:
            self.connected = False
            self.last_error = f"connect error: {e.__class__.__name__}: {e}"
            self.guard.log_connect_failure(self.logger, self.last_error)
            return False

    def ensure_connected(self) -> bool:
        return self.connected or self.connect()

    def close(self) -> None:
        try:
            self.client.close()
        finally:
            self.connected = False

    def _read_bits(self, start: int, count: int, unit: int, func: str) -> Optional[List[bool]]:
        try:
            rr, err = call_bits(self.client.read_discrete_inputs if func == "di" else self.client.read_coils, start, count, unit)
            if rr is None:
                self.last_error = err or "unknown read error"
                return None
            if hasattr(rr, "isError") and rr.isError():  # type: ignore
                self.last_error = "server returned error"
                return None
            return list(rr.bits[:count])  # type: ignore
        except Exception as e:
            self.last_error = f"{func} read exception: {e.__class__.__name__}: {e}"
            return None

    def probe(self, base: int, refs: List[int]) -> bool:
        if not refs:
            self.unit = self.unit or self.unit_candidates[0]
            self.func = "di"
            return True
        start, count = refs_to_block(refs, base)
        if not self.ensure_connected():
            return False
        for uid in self.unit_candidates:
            bits = self._read_bits(start, count, uid, "di")
            if bits is not None:
                self.unit = uid
                self.func = "di"
                return True
        if self.use_coils_fallback:
            for uid in self.unit_candidates:
                bits = self._read_bits(start, count, uid, "coils")
                if bits is not None:
                    self.unit = uid
                    self.func = "coils"
                    return True
        self.last_error = self.last_error or "probe failed (no response)"
        return False

    def read_refs(self, base: int, refs: List[int]) -> Dict[int, Optional[bool]]:
        out: Dict[int, Optional[bool]] = {ref: None for ref in refs}
        if not refs:
            return out
        if not self.ensure_connected():
            return out
        if self.unit is None and not self.probe(base, refs[: min(4, len(refs))]):
            return out
        start, count = refs_to_block(refs, base)
        bits = self._read_bits(start, count, self.unit, self.func)  # type: ignore
        if bits is None:
            self.client.close()
            self.connected = False
            if not self.connect():
                return out
            bits = self._read_bits(start, count, self.unit, self.func)  # type: ignore
            if bits is None:
                return out
        for ref in refs:
            idx = (ref - base) - start
            if 0 <= idx < len(bits):
                out[ref] = bool(bits[idx])
        return out

    def write_coil(self, address: int, value: bool) -> bool:
        if not self.ensure_connected():
            return False
        if self.unit is None:
            # Probe didn't run yet. Default to the first candidate.
            self.unit = self.unit_candidates[0] if self.unit_candidates else 1
        try:
            rq, err = call_write_coil(self.client.write_coil, int(address), bool(value), int(self.unit))
            if rq is None:
                self.last_error = err or "unknown write error"
                return False
            if hasattr(rq, "isError") and rq.isError():  # type: ignore
                self.last_error = "server returned error"
                return False
            return True
        except Exception as e:
            self.last_error = f"coil write exception: {e.__class__.__name__}: {e}"
            return False
