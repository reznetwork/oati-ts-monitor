"""Modbus DI passthrough mirror server."""
from __future__ import annotations

import asyncio
import logging
import threading
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from pymodbus.datastore import ModbusDeviceContext, ModbusServerContext, ModbusSparseDataBlock
    from pymodbus.pdu.device import ModbusDeviceIdentification
    from pymodbus.server import StartAsyncTcpServer
except Exception:  # pragma: no cover
    ModbusDeviceContext = None
    ModbusDeviceIdentification = None
    ModbusServerContext = None
    ModbusSparseDataBlock = None
    StartAsyncTcpServer = None

from core.config import build_passthrough_ref_map
from core.state import AppState

class MirrorPassthroughDataBlock:
    """Thread-safe sparse passthrough bit store."""

    def __init__(self, seed: Optional[Dict[int, bool]] = None):
        if ModbusSparseDataBlock is None:
            raise RuntimeError("pymodbus server components are required to run DI mirror server")
        self._lock = threading.RLock()
        self._values: Dict[int, int] = {0: 0}
        block_values: Dict[int, int] = {self._block_address(0): 0}
        if seed:
            for addr, value in seed.items():
                a = int(addr)
                v = 1 if bool(value) else 0
                self._values[a] = v
                block_values[self._block_address(a)] = v
        self._block = ModbusSparseDataBlock(block_values)

    @property
    def block(self):
        return self._block

    @staticmethod
    def _block_address(address: int) -> int:
        # ModbusDeviceContext increments request addresses before datastore access.
        return int(address) + 1

    def snapshot(self) -> Dict[int, bool]:
        with self._lock:
            return {addr: bool(value) for addr, value in self._values.items()}

    def set_bits(self, values: Dict[int, bool]) -> Tuple[Dict[int, bool], List[int]]:
        """Return changed values and any newly seen addresses."""
        changed: Dict[int, bool] = {}
        new_addrs: List[int] = []
        with self._lock:
            for addr, value in values.items():
                a = int(addr)
                v = 1 if bool(value) else 0
                if a not in self._values:
                    self._values[a] = v
                    changed[a] = bool(v)
                    new_addrs.append(a)
                elif self._values[a] != v:
                    self._values[a] = v
                    changed[a] = bool(v)
                if a in changed:
                    self._block.setValues(self._block_address(a), [v])
        return changed, new_addrs


class ModbusDiscreteInputsMirrorServer:
    """
    Expose collected Modbus boolean refs as Modbus/TCP passthrough bits.

    Addressing policy:
    - Passthrough address == configured group DI (e.g. handbrake -> 1).
    - Values with None are skipped (last known value is kept).
    - FC02 discrete inputs are primary; FC01 coils mirror the same bits for compatibility.
    """

    def __init__(
        self,
        *,
        state: "AppState",
        bind_host: str = "0.0.0.0",
        port: int = 502,
        unit_id: int = 1,
        refresh_sec: float = 0.2,
        ref_map: Optional[Dict[Tuple[str, int], int]] = None,
        seed_addresses: Optional[Iterable[int]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        if StartAsyncTcpServer is None or ModbusDeviceContext is None or ModbusServerContext is None:
            raise RuntimeError("pymodbus server components are required to run DI mirror server")
        self.state = state
        self.bind_host = str(bind_host)
        self.port = int(port)
        self.unit_id = int(unit_id)
        self.refresh_sec = float(refresh_sec)
        self._ref_map = dict(ref_map or {})
        self.logger = logger or logging.getLogger("modbus_di_mirror")

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        seed_addrs = {0: False}
        for addr in seed_addresses or ():
            seed_addrs[int(addr)] = False
        for addr in self._ref_map.values():
            seed_addrs[int(addr)] = False
        if len(seed_addrs) > 1:
            # Fill gaps so FC02/FC01 range reads do not hit illegal-address holes (e.g. DI 16).
            max_addr = max(seed_addrs)
            for addr in range(max_addr + 1):
                seed_addrs.setdefault(addr, False)
        self._data_block = MirrorPassthroughDataBlock(seed_addrs)
        self._store = ModbusDeviceContext(di=self._data_block.block, co=self._data_block.block)
        self._context = ModbusServerContext(devices=self._store, single=True)
        self._identity = ModbusDeviceIdentification()
        self._identity.VendorName = "oati-ts-monitor"
        self._identity.ProductName = "DI mirror"
        self._identity.MajorMinorRevision = "1.0"

    def _publish_bits(self, values: Dict[int, bool]) -> None:
        self._data_block.set_bits(values)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._thread_main, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self.state.update_event.set()
        if self._loop:
            try:
                self._loop.call_soon_threadsafe(lambda: None)
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=2)

    def _lookup_ref_value(self, ctrl: Dict[str, Any], ref: int) -> Optional[bool]:
        for section in ("points", "gears", "extra"):
            vals = (ctrl or {}).get(section) or {}
            if not isinstance(vals, dict):
                continue
            v = vals.get(str(ref))
            if v is None and ref in vals:
                v = vals.get(ref)
            if v is not None:
                return bool(v)
        return None

    def _collect_bits(self) -> Dict[int, bool]:
        snap = self.state.snapshot()
        out: Dict[int, bool] = {}
        ctrls = snap.get("controllers", {}) or {}
        if self._ref_map:
            for (ctrl_name, ref), addr in self._ref_map.items():
                ctrl = ctrls.get(ctrl_name) or {}
                v = self._lookup_ref_value(ctrl, ref)
                if v is not None:
                    out[int(addr)] = v
            return out
        for _ctrl_name, ctrl in ctrls.items():
            for section in ("points", "gears", "extra"):
                vals = (ctrl or {}).get(section) or {}
                if not isinstance(vals, dict):
                    continue
                for ref_str, v in vals.items():
                    if v is None:
                        continue
                    try:
                        ref = int(ref_str)
                    except Exception:
                        continue
                    out[ref] = bool(v)
        return out

    async def _refresher(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            try:
                # Prefer event-driven updates, but wake up periodically too.
                await loop.run_in_executor(None, self.state.update_event.wait, self.refresh_sec)
            finally:
                self.state.update_event.clear()
            try:
                bits = self._collect_bits()
                if bits:
                    self._publish_bits(bits)
            except Exception as e:
                self.logger.warning("DI mirror refresh failed: %s", e)

    async def _run_async(self) -> None:
        refresher = asyncio.create_task(self._refresher())
        try:
            await StartAsyncTcpServer(
                context=self._context,
                identity=self._identity,
                address=(self.bind_host, self.port),
            )
        finally:
            self._stop_event.set()
            self.state.update_event.set()
            refresher.cancel()
            try:
                await refresher
            except asyncio.CancelledError:
                pass

    def _thread_main(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self.logger.info("Starting DI mirror Modbus/TCP on %s:%s unit=%s", self.bind_host, self.port, self.unit_id)
            self._loop.run_until_complete(self._run_async())
        except OSError as e:
            self.logger.error("DI mirror Modbus/TCP bind failed on %s:%s (%s)", self.bind_host, self.port, e)
        except Exception as e:
            self.logger.error("DI mirror server stopped (%s)", e)
        finally:
            try:
                self._loop.close()
            except Exception:
                pass
