from __future__ import annotations

import asyncio
import ast
import gzip
import hashlib
import json
import json as jsonlib
import logging
import logging.handlers
import os
import queue
import re
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import jinja2
except ModuleNotFoundError:  # pragma: no cover
    jinja2 = None
try:
    from aiohttp import WSMsgType, web
except ModuleNotFoundError:  # pragma: no cover
    WSMsgType = None
    web = None
try:
    from pymodbus.client import ModbusTcpClient
except ModuleNotFoundError:  # pragma: no cover
    ModbusTcpClient = None
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

DEFAULT_MODBUS_PORT = 502
DEFAULT_TIMEOUT = 2.5
DEFAULT_POLL_INTERVAL_SEC = 1.0
DEFAULT_UNIT_CANDIDATES = [1, 255, 0]
DEFAULT_COILS_FALLBACK = True
MAX_MODBUS_REF_SPAN = 100  # Modbus read 'count' upper bound per request
HISTORY_WINDOW_SEC = 600
TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"

EXTERNAL_LOG_SOURCES: Dict[str, Dict[str, Any]] = {
    "networkmanager": {
        "title": "NetworkManager",
        "subtitle": "Live journal tail (NetworkManager.service)",
        "tail_cmd": ["journalctl", "-u", "NetworkManager", "-n", "50", "--no-pager", "--output=short-iso"],
        "follow_cmd": ["journalctl", "-u", "NetworkManager", "-f", "-n", "0", "--no-pager", "--output=short-iso"],
    },
    "exam-vehicle": {
        "title": "exam-vehicle",
        "subtitle": "Live docker logs (exam-vehicle container)",
        "tail_cmd": ["docker", "logs", "--tail", "50", "exam-vehicle"],
        "follow_cmd": ["docker", "logs", "-f", "--tail", "0", "exam-vehicle"],
    },
    "wlan-watchdog": {
        "title": "wlan0_watchdog",
        "subtitle": "/var/log/wlan_watchdog/wlan_watchdog.log",
        "tail_cmd": ["tail", "-n", "50", "/var/log/wlan_watchdog/wlan_watchdog.log"],
        "follow_cmd": ["tail", "-f", "-n", "0", "/var/log/wlan_watchdog/wlan_watchdog.log"],
    },
}

# Full-fidelity log schema
FULL_LOG_SCHEMA_VERSION = 1
FULL_LOG_COMPRESSED_SUFFIX = ".jsonl.gz"

# Reconnect rate limiting (prevents controller-side session buildup if reconnect is spammed)
RECONNECT_WINDOW_SEC = 30.0
RECONNECT_MAX_PER_WINDOW = 3

# Per-controller connect() spam guard (protects controller's 12-session hard limit)
CLIENT_CONNECT_WINDOW_SEC = 60.0
CLIENT_CONNECT_MAX_PER_WINDOW = 5


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


@dataclass
class PointCfg:
    ref: int
    label: str
    invert: bool = False
    style: Optional[str] = None
    passthrough: Optional[str] = None


@dataclass
class ControllerCfg:
    name: str
    host: str
    base: int
    points: List[PointCfg] = field(default_factory=list)
    gear_points: Dict[int, str] = field(default_factory=dict)
    extra_points: Dict[int, str] = field(default_factory=dict)
    passthrough_gears: Dict[int, str] = field(default_factory=dict)
    passthrough_extra: Dict[int, str] = field(default_factory=dict)
    model: Optional[str] = None


@dataclass
class PassthroughCfg:
    enabled: bool = False
    bind: str = "0.0.0.0"
    port: int = 502
    unit_id: int = 1
    groups: Dict[str, int] = field(default_factory=dict)


@dataclass
class VehicleCfg:
    name: str
    controllers: List[ControllerCfg]
    short_name: Optional[str] = None
    external_ip: Optional[str] = None
    gnss_host: Optional[str] = None
    gnss_port: Optional[int] = None
    gnss_admin_host: Optional[str] = None
    gnss_admin_port: Optional[int] = None
    bridge_mappings: List[BridgeMappingCfg] = field(default_factory=list)


@dataclass
class BridgeInput:
    controller: str
    ref: int
    source_type: str = "di"
    name: Optional[str] = None


@dataclass
class BridgeOutput:
    controller: str
    address: int


@dataclass
class BridgeMappingCfg:
    name: str
    inputs: List[BridgeInput]
    output: BridgeOutput
    logic: Optional[str] = None
    invert: bool = False
    debounce_ms: int = 0
    on_error: str = "hold"  # hold | force_off | force_on


def derive_short_vehicle_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:48] if len(s) > 48 else s


@dataclass
class AppCfg:
    port: int = DEFAULT_MODBUS_PORT
    timeout: float = DEFAULT_TIMEOUT
    poll_interval_sec: float = DEFAULT_POLL_INTERVAL_SEC
    unit_candidates: List[int] = field(default_factory=lambda: DEFAULT_UNIT_CANDIDATES[:])
    coils_fallback: bool = DEFAULT_COILS_FALLBACK
    gnss_host: Optional[str] = None
    gnss_port: Optional[int] = None
    wifi_iface: Optional[str] = None
    wifi_refresh: Optional[float] = None
    detailed_wifi_log_file: Optional[str] = None
    app_log_file: Optional[str] = None
    app_log_level: Optional[str] = None
    # Full-fidelity always-on logging (JSONL)
    full_log_enabled: bool = False
    full_log_dir: str = "logs/full"
    full_log_snapshot_interval_sec: float = 1.0
    full_log_rotate_bytes: int = 5 * 1024 * 1024
    # HTTP upload (compressed whole-file transfer)
    upload_enabled: bool = False
    upload_url: Optional[str] = None
    upload_device_id: Optional[str] = None
    upload_chunk_bytes: int = 256 * 1024
    upload_state_file: str = "logs/upload_state.json"
    display_latency_hosts: List[Tuple[str, str, int]] = field(default_factory=list)
    vehicles: List[VehicleCfg] = field(default_factory=list)
    passthrough: PassthroughCfg = field(default_factory=PassthroughCfg)


def build_passthrough_ref_map(vehicle: VehicleCfg, groups: Dict[str, int]) -> Dict[Tuple[str, int], int]:
    """Map (controller name, source ref) -> unified passthrough DI address."""
    out: Dict[Tuple[str, int], int] = {}
    if not groups:
        return out
    for ctrl in vehicle.controllers:
        cname = ctrl.name
        for p in ctrl.points:
            if not p.passthrough:
                continue
            addr = groups.get(p.passthrough)
            if addr is not None:
                out[(cname, p.ref)] = int(addr)
        for ref, group in ctrl.passthrough_gears.items():
            addr = groups.get(group)
            if addr is not None:
                out[(cname, int(ref))] = int(addr)
        for ref, group in ctrl.passthrough_extra.items():
            addr = groups.get(group)
            if addr is not None:
                out[(cname, int(ref))] = int(addr)
    return out


def infer_style(label: str, explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    l = label.lower()
    if "ручник" in l:
        return "handbrake"
    if "ремень" in l:
        return "seatbelt"
    if "двигател" in l:
        return "engine"
    if "поворотник" in l:
        return "indicator"
    return "default"


class AppState:
    def __init__(self, vehicle: VehicleCfg, appcfg: AppCfg):
        self.vehicle = vehicle
        self.appcfg = appcfg
        self.lock = threading.RLock()
        self.update_event = threading.Event()
        self.log_event = threading.Event()
        self.app_log_event = threading.Event()
        self.modbus_enabled: bool = True
        self._app_log_seq = 0
        self._app_logs: deque = deque(maxlen=5000)
        self.controllers: Dict[str, Dict[str, Any]] = {}
        self.gnss: Optional[Dict[str, Any]] = None
        self.wifi: Optional[Dict[str, Any]] = None
        self.cpu_load: Optional[float] = None
        self.display_latency: Dict[str, Dict[str, Any]] = {}
        self.detailed_wifi_logging: Dict[str, Any] = {"enabled": False, "file": None, "last_error": None}
        self.last_update: float = 0.0
        self.history: Dict[str, Any] = {
            "modbus_latency": {},
            "gateway_latency": {},
            "wifi": {
                "signal_dbm": deque(),
                "tx_rate_mbps": deque(),
                "rx_rate_mbps": deque(),
                "tx_bytes": deque(),
                "rx_bytes": deque(),
            },
            "cpu_load": deque(),
            "cpu_temp_c": deque(),
        }
        for cfg in self.vehicle.controllers:
            point_meta = {}
            for p in cfg.points:
                point_meta[str(p.ref)] = {"label": p.label, "style": infer_style(p.label, p.style), "di": p.ref - cfg.base}
            self.controllers[cfg.name] = {
                "host": cfg.host, "base": cfg.base, "model": cfg.model, "status": "INIT", "latency": None, "debug": "",
                "points": {}, "points_meta": point_meta, "gears": {}, "extra": {},
            }

    def append_app_log(self, *, line: str, level: str = "INFO", logger: str = "app") -> int:
        line = (line or "").rstrip("\n")
        with self.lock:
            self._app_log_seq += 1
            seq = self._app_log_seq
            self._app_logs.append(
                {
                    "seq": seq,
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "level": str(level),
                    "logger": str(logger),
                    "line": line,
                }
            )
            self.app_log_event.set()
            return seq

    def app_log_tail(self, limit: int = 50) -> List[Dict[str, Any]]:
        lim = max(1, min(int(limit or 50), 500))
        with self.lock:
            return list(self._app_logs)[-lim:]

    def app_logs_since(self, seq: int) -> List[Dict[str, Any]]:
        try:
            seq_int = int(seq or 0)
        except Exception:
            seq_int = 0
        with self.lock:
            return [item for item in self._app_logs if int(item.get("seq") or 0) > seq_int]

    def _append_history(self, series: deque, value: Optional[float], now: Optional[float] = None) -> None:
        if value is None:
            return
        ts = now if now is not None else time.time()
        series.append({"ts": ts, "value": value})
        cutoff = ts - HISTORY_WINDOW_SEC
        while series and series[0]["ts"] < cutoff:
            series.popleft()

    def _history_snapshot(self) -> Dict[str, Any]:
        return {
            "modbus_latency": {n: list(s) for n, s in self.history["modbus_latency"].items()},
            "gateway_latency": {n: list(s) for n, s in self.history["gateway_latency"].items()},
            "wifi": {k: list(v) for k, v in self.history["wifi"].items()},
            "cpu_load": list(self.history["cpu_load"]),
            "cpu_temp_c": list(self.history["cpu_temp_c"]),
        }

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            fleet = []
            for v in (self.appcfg.vehicles or []):
                fleet.append(
                    {
                        "name": v.name,
                        "short_name": v.short_name,
                        "external_ip": v.external_ip,
                    }
                )
            return {
                "controllers": jsonlib.loads(jsonlib.dumps(self.controllers)),
                "gnss": jsonlib.loads(jsonlib.dumps(self.gnss)) if self.gnss is not None else None,
                "wifi": jsonlib.loads(jsonlib.dumps(self.wifi)) if self.wifi is not None else None,
                "cpu_load": self.cpu_load,
                "display_latency": jsonlib.loads(jsonlib.dumps(self.display_latency)),
                "detailed_wifi_logging": jsonlib.loads(jsonlib.dumps(self.detailed_wifi_logging)),
                "modbus": {"enabled": bool(self.modbus_enabled)},
                "history": self._history_snapshot(),
                "last_update": self.last_update,
                "vehicle": self.vehicle.name,
                "vehicle_short": self.vehicle.short_name,
                "fleet": fleet,
            }

    def set_controller(self, name: str, **kwargs):
        with self.lock:
            if name not in self.controllers:
                self.controllers[name] = {}
            self.controllers[name].update(kwargs)
            if "latency" in kwargs:
                self._append_history(self.history["modbus_latency"].setdefault(name, deque()), kwargs.get("latency"))
            self.last_update = time.time()
            self.update_event.set()
            self.log_event.set()

    def set_gnss(self, snap: Optional[Dict[str, Any]]):
        with self.lock:
            self.gnss = snap
            self.last_update = time.time()
            self.update_event.set()
            self.log_event.set()

    def set_wifi(self, snap: Optional[Dict[str, Any]]):
        with self.lock:
            self.wifi = snap
            if snap:
                self._append_history(self.history["wifi"]["signal_dbm"], snap.get("signal_dbm"))
                self._append_history(self.history["wifi"]["tx_rate_mbps"], snap.get("tx_rate_mbps"))
                self._append_history(self.history["wifi"]["rx_rate_mbps"], snap.get("rx_rate_mbps"))
                self._append_history(self.history["wifi"]["tx_bytes"], snap.get("tx_bytes"))
                self._append_history(self.history["wifi"]["rx_bytes"], snap.get("rx_bytes"))
            self.last_update = time.time()
            self.update_event.set()
            self.log_event.set()

    def set_display_latency(self, data: Dict[str, Dict[str, Any]]):
        with self.lock:
            self.display_latency = data
            now = time.time()
            for name, info in data.items():
                self._append_history(self.history["gateway_latency"].setdefault(name, deque()), info.get("latency_ms"), now=now)
            self.last_update = time.time()
            self.update_event.set()
            self.log_event.set()

    def display_latency_snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self.lock:
            return jsonlib.loads(jsonlib.dumps(self.display_latency))

    def set_cpu_load(self, load: Optional[float]):
        with self.lock:
            self.cpu_load = load
            self._append_history(self.history["cpu_load"], load)
            self.last_update = time.time()
            self.update_event.set()
            self.log_event.set()

    def set_cpu_temp(self, temp_c: Optional[float]):
        with self.lock:
            self._append_history(self.history["cpu_temp_c"], temp_c)
            self.last_update = time.time()
            self.update_event.set()
            self.log_event.set()

    def set_detailed_wifi_logging(self, *, enabled: bool, file: Optional[str], last_error: Optional[str]):
        with self.lock:
            self.detailed_wifi_logging = {"enabled": bool(enabled), "file": file, "last_error": last_error}
            self.last_update = time.time()
            self.update_event.set()

    def set_modbus_enabled(self, enabled: bool) -> None:
        with self.lock:
            self.modbus_enabled = bool(enabled)
            self.last_update = time.time()
            self.update_event.set()

    def gnss_snapshot(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            return jsonlib.loads(jsonlib.dumps(self.gnss)) if self.gnss is not None else None


class DataLogger:
    def __init__(self, state: AppState, vehicle: VehicleCfg, log_file: Optional[str], interval: Optional[float]):
        self.state = state
        self.vehicle = vehicle
        self.log_file = Path(log_file).expanduser() if log_file else None
        self.interval = interval if interval and interval > 0 else None
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.last_error: Optional[str] = None
        self.active = False

    def start(self):
        if not self.log_file:
            return
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.state.log_event.set()
        if self.thread:
            self.thread.join(timeout=2)

    def _build_entry(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        entry: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "vehicle": snap.get("vehicle"),
            "controllers": [],
            "gnss": snap.get("gnss"),
            "wifi": snap.get("wifi"),
        }
        ctrls = snap.get("controllers", {}) or {}
        for cfg in self.vehicle.controllers:
            ctrl_snap = ctrls.get(cfg.name, {}) or {}
            ctrl_entry = {
                "name": cfg.name,
                "host": ctrl_snap.get("host") or cfg.host,
                "status": ctrl_snap.get("status"),
                "latency_ms": ctrl_snap.get("latency"),
                "points": [],
            }
            points_state = ctrl_snap.get("points", {}) or {}
            for p in cfg.points:
                val = points_state.get(str(p.ref))
                ctrl_entry["points"].append({"ref": p.ref, "label": p.label, "state": "ON" if val else "OFF" if val is False else None})
            entry["controllers"].append(ctrl_entry)
        return entry

    def _loop(self):
        if not self.log_file:
            return
        with self.log_file.open("a", encoding="utf-8") as fh:
            self.active = True
            while not self.stop_event.is_set():
                triggered = self.state.log_event.wait(self.interval) if self.interval is not None else self.state.log_event.wait()
                self.state.log_event.clear()
                if self.stop_event.is_set():
                    break
                if triggered or self.interval is not None:
                    fh.write(jsonlib.dumps(self._build_entry(self.state.snapshot()), ensure_ascii=False) + "\n")
                    fh.flush()


def _epoch_ms() -> int:
    return int(time.time() * 1000)


def _default_device_id() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "unknown"


def make_full_log_record(*, record_type: str, ts_ms: int, source: Dict[str, Any], data: Any) -> Dict[str, Any]:
    """
    Full-fidelity JSONL record (one object per line).
    """
    return {
        "v": FULL_LOG_SCHEMA_VERSION,
        "ts_ms": int(ts_ms),
        "source": source,
        "type": str(record_type),
        "data": data,
    }


class FullFidelityLogger:
    """
    Always-on full-fidelity logging into rotated JSONL segment files.
    """

    def __init__(self, state: AppState, *, enabled: bool, base_dir: str, snapshot_interval_sec: float, rotate_bytes: int):
        self.state = state
        self.enabled = bool(enabled)
        self.base_dir = str(base_dir or "logs/full")
        self.snapshot_interval_sec = max(0.1, float(snapshot_interval_sec or 1.0))
        self.rotate_bytes = max(64 * 1024, int(rotate_bytes or (5 * 1024 * 1024)))
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=16384)
        self.last_error: Optional[str] = None

        self._current_path: Optional[Path] = None
        self._current_start_ts_ms: Optional[int] = None
        self._current_seq: int = 0
        self._bytes_written: int = 0
        self._last_snap_for_events: Optional[Dict[str, Any]] = None

    def start(self) -> None:
        if not self.enabled:
            return
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.state.log_event.set()
        if self.thread:
            self.thread.join(timeout=2)

    def current_segment_path(self) -> Optional[Path]:
        return self._current_path

    def enqueue(self, record: Dict[str, Any]) -> None:
        if not self.enabled:
            return
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            self.last_error = "full log queue overflow"

    def enqueue_snapshot(self, snap: Dict[str, Any], ts_ms: Optional[int] = None) -> None:
        self.enqueue(
            make_full_log_record(
                record_type="snapshot",
                ts_ms=int(ts_ms or _epoch_ms()),
                source=self._source(),
                data=snap,
            )
        )

    def enqueue_event(self, kind: str, payload: Dict[str, Any], ts_ms: Optional[int] = None) -> None:
        self.enqueue(
            make_full_log_record(
                record_type="event",
                ts_ms=int(ts_ms or _epoch_ms()),
                source=self._source(),
                data={"kind": str(kind), "payload": payload},
            )
        )

    def _source(self) -> Dict[str, Any]:
        return {
            "vehicle": self.state.vehicle.name,
            "vehicle_short": self.state.vehicle.short_name or derive_short_vehicle_name(self.state.vehicle.name),
            "device_id": self.state.appcfg.upload_device_id or _default_device_id(),
            "pid": os.getpid(),
        }

    def _segment_dir(self, ts_ms: int) -> Path:
        dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        day = dt.strftime("%Y%m%d")
        vehicle_short = self.state.vehicle.short_name or derive_short_vehicle_name(self.state.vehicle.name)
        return Path(self.base_dir).expanduser() / vehicle_short / day

    def _maybe_rotate(self, ts_ms: int) -> None:
        seg_dir = self._segment_dir(ts_ms)
        if self._current_path is None or self._current_start_ts_ms is None:
            self._current_start_ts_ms = ts_ms
            self._current_seq = 0
            seg_dir.mkdir(parents=True, exist_ok=True)
            self._current_path = seg_dir / f"segment_{self._current_start_ts_ms}_{self._current_seq}.jsonl"
            try:
                self._bytes_written = self._current_path.stat().st_size if self._current_path.exists() else 0
            except Exception:
                self._bytes_written = 0
            return

        # Day rollover
        try:
            if self._current_path.parent != seg_dir:
                self._current_start_ts_ms = ts_ms
                self._current_seq = 0
                seg_dir.mkdir(parents=True, exist_ok=True)
                self._current_path = seg_dir / f"segment_{self._current_start_ts_ms}_{self._current_seq}.jsonl"
                self._bytes_written = self._current_path.stat().st_size if self._current_path.exists() else 0
                return
        except Exception:
            pass

        # Size rotation
        if self._bytes_written >= self.rotate_bytes:
            self._current_seq += 1
            seg_dir.mkdir(parents=True, exist_ok=True)
            self._current_path = seg_dir / f"segment_{self._current_start_ts_ms}_{self._current_seq}.jsonl"
            try:
                self._bytes_written = self._current_path.stat().st_size if self._current_path.exists() else 0
            except Exception:
                self._bytes_written = 0

    def _write_one(self, rec: Dict[str, Any]) -> None:
        ts_ms = int(rec.get("ts_ms") or _epoch_ms())
        self._maybe_rotate(ts_ms)
        if self._current_path is None:
            return
        line = (jsonlib.dumps(rec, ensure_ascii=False) + "\n").encode("utf-8")
        try:
            self._current_path.parent.mkdir(parents=True, exist_ok=True)
            with self._current_path.open("ab") as fh:
                fh.write(line)
            self._bytes_written += len(line)
        except Exception as e:
            self.last_error = f"full log write failed: {e}"

    def _derive_events(self, prev: Dict[str, Any], cur: Dict[str, Any], ts_ms: int) -> None:
        # Modbus enabled toggle
        try:
            prev_en = bool(((prev.get("modbus") or {}) or {}).get("enabled"))
            cur_en = bool(((cur.get("modbus") or {}) or {}).get("enabled"))
            if prev_en != cur_en:
                self.enqueue_event("modbus_enabled", {"enabled": cur_en}, ts_ms=ts_ms)
        except Exception:
            pass

        # Controller status and point changes
        prev_ctrls = prev.get("controllers", {}) or {}
        cur_ctrls = cur.get("controllers", {}) or {}
        for ctrl_name, cur_ctrl in cur_ctrls.items():
            prev_ctrl = prev_ctrls.get(ctrl_name, {}) or {}
            if (prev_ctrl.get("status") or None) != (cur_ctrl.get("status") or None):
                self.enqueue_event(
                    "controller_status",
                    {"controller": ctrl_name, "from": prev_ctrl.get("status"), "to": cur_ctrl.get("status")},
                    ts_ms=ts_ms,
                )
            prev_points = (prev_ctrl.get("points", {}) or {}) if isinstance(prev_ctrl, dict) else {}
            cur_points = (cur_ctrl.get("points", {}) or {}) if isinstance(cur_ctrl, dict) else {}
            for ref, cur_val in (cur_points or {}).items():
                prev_val = (prev_points or {}).get(ref)
                if prev_val != cur_val:
                    self.enqueue_event(
                        "modbus_point",
                        {"controller": ctrl_name, "ref": ref, "from": prev_val, "to": cur_val},
                        ts_ms=ts_ms,
                    )

        # WiFi BSSID changes
        try:
            prev_wifi = prev.get("wifi") or {}
            cur_wifi = cur.get("wifi") or {}
            if isinstance(prev_wifi, dict) and isinstance(cur_wifi, dict):
                if (prev_wifi.get("bssid") or None) != (cur_wifi.get("bssid") or None):
                    self.enqueue_event(
                        "wifi_bssid",
                        {"from": prev_wifi.get("bssid"), "to": cur_wifi.get("bssid"), "channel": cur_wifi.get("channel")},
                        ts_ms=ts_ms,
                    )
        except Exception:
            pass

        # GNSS fix label changes
        try:
            prev_g = prev.get("gnss") or {}
            cur_g = cur.get("gnss") or {}
            if isinstance(prev_g, dict) and isinstance(cur_g, dict):
                if (prev_g.get("fix") or None) != (cur_g.get("fix") or None):
                    self.enqueue_event("gnss_fix", {"from": prev_g.get("fix"), "to": cur_g.get("fix")}, ts_ms=ts_ms)
        except Exception:
            pass

    def _loop(self) -> None:
        self.enqueue_event("lifecycle", {"event": "startup"})
        next_snapshot_deadline = time.monotonic()
        while not self.stop_event.is_set():
            now = time.monotonic()
            if now >= next_snapshot_deadline:
                ts_ms = _epoch_ms()
                snap = self.state.snapshot()
                self.enqueue_snapshot(snap, ts_ms=ts_ms)
                self._last_snap_for_events = snap
                next_snapshot_deadline = now + self.snapshot_interval_sec

            try:
                triggered = self.state.log_event.wait(timeout=0.2)
            finally:
                self.state.log_event.clear()
            if triggered:
                ts_ms = _epoch_ms()
                cur = self.state.snapshot()
                prev = self._last_snap_for_events
                if isinstance(prev, dict):
                    self._derive_events(prev, cur, ts_ms)
                self._last_snap_for_events = cur

            while True:
                try:
                    item = self.queue.get_nowait()
                except queue.Empty:
                    break
                self._write_one(item)

        self.enqueue_event("lifecycle", {"event": "shutdown"})
        while True:
            try:
                item = self.queue.get_nowait()
            except queue.Empty:
                break
            self._write_one(item)


class HttpLogUploader:
    """
    Whole-file HTTP uploader for compressed full-fidelity logs.

    - Compresses inactive `segment_*.jsonl` files to `segment_*.jsonl.gz`.
    - Uploads complete gzip files and marks each file complete after HTTP success.
    """

    def __init__(
        self,
        *,
        enabled: bool,
        base_dir: str,
        url: Optional[str],
        device_id: Optional[str],
        chunk_bytes: int,
        state_file: str,
        vehicle: str,
        vehicle_short: str,
        active_segment_getter: Optional[Any] = None,
    ):
        self.enabled = bool(enabled)
        self.base_dir = str(base_dir or "logs/full")
        self.url = str(url).strip() if url else None
        self.device_id = str(device_id).strip() if device_id else _default_device_id()
        self.chunk_bytes = max(16 * 1024, int(chunk_bytes or (256 * 1024)))
        self.state_file = Path(state_file or "logs/upload_state.json").expanduser()
        self.vehicle = str(vehicle or "")
        self.vehicle_short = str(vehicle_short or "")
        self.active_segment_getter = active_segment_getter
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.last_error: Optional[str] = None
        self._lock = threading.Lock()
        self._state: Dict[str, Any] = {"v": 2, "uploaded": {}, "files": {}}

    def start(self) -> None:
        if not self.enabled or not self.url:
            return
        self._load_state()
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=2)
        self._save_state()

    def _load_state(self) -> None:
        try:
            if self.state_file.exists():
                raw = json.loads(self.state_file.read_text(encoding="utf-8"))
                if isinstance(raw, dict) and isinstance(raw.get("files"), dict):
                    self._state = raw
        except Exception:
            # ignore corrupt state
            pass

    def _save_state(self) -> None:
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_file.with_suffix(self.state_file.suffix + ".tmp")
            tmp.write_text(json.dumps(self._state, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self.state_file)
        except Exception:
            pass

    def _list_segments(self) -> List[Path]:
        base = Path(self.base_dir).expanduser()
        if not base.exists():
            return []
        return sorted(base.rglob(f"segment_*{FULL_LOG_COMPRESSED_SUFFIX}"), key=lambda p: (str(p.parent), p.name))

    def _active_segment(self) -> Optional[Path]:
        getter = self.active_segment_getter
        if getter is None:
            return None
        try:
            active = getter()
            return Path(active).expanduser().resolve() if active else None
        except Exception:
            return None

    def _list_uncompressed_segments(self) -> List[Path]:
        base = Path(self.base_dir).expanduser()
        if not base.exists():
            return []
        return sorted(base.rglob("segment_*.jsonl"), key=lambda p: (str(p.parent), p.name))

    def _legacy_offset(self, path: Path) -> int:
        with self._lock:
            files = self._state.setdefault("files", {})
            try:
                return int(files.get(str(path), 0))
            except Exception:
                return 0

    def _mark_uploaded(self, path: Path) -> None:
        try:
            st = path.stat()
            entry = {"size": int(st.st_size), "mtime": float(st.st_mtime)}
        except OSError:
            entry = {"size": 0, "mtime": 0.0}
        with self._lock:
            uploaded = self._state.setdefault("uploaded", {})
            uploaded[str(path)] = entry

    def _is_uploaded(self, path: Path) -> bool:
        try:
            st = path.stat()
            size = int(st.st_size)
            mtime = float(st.st_mtime)
        except OSError:
            return False
        with self._lock:
            entry = self._state.setdefault("uploaded", {}).get(str(path))
        if entry is True:
            return True
        if isinstance(entry, dict):
            try:
                return int(entry.get("size", -1)) == size and abs(float(entry.get("mtime", -1.0)) - mtime) < 0.001
            except Exception:
                return False
        return False

    def _compress_segment(self, path: Path) -> Optional[Path]:
        if path.name.endswith(FULL_LOG_COMPRESSED_SUFFIX):
            return path
        try:
            resolved = path.expanduser().resolve()
            active = self._active_segment()
            if active is not None and resolved == active:
                return None
            st = path.stat()
            if st.st_size <= 0:
                return None
            # Avoid racing the logger before it has published its active path.
            if time.time() - float(st.st_mtime) < 2.0:
                return None
            gz_path = path.with_name(path.name + ".gz")
            tmp_path = gz_path.with_suffix(gz_path.suffix + ".tmp")
            legacy_fully_uploaded = self._legacy_offset(path) >= int(st.st_size)
            with path.open("rb") as src, gzip.open(tmp_path, "wb", compresslevel=6) as dst:
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)
            tmp_path.replace(gz_path)
            path.unlink()
            if legacy_fully_uploaded:
                self._mark_uploaded(gz_path)
            return gz_path
        except OSError as e:
            self.last_error = str(e)
            return None

    def _compress_backlog(self) -> None:
        for path in self._list_uncompressed_segments():
            if self.stop_event.is_set():
                break
            self._compress_segment(path)

    def _sha256_file(self, path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as fh:
            while True:
                data = fh.read(1024 * 1024)
                if not data:
                    break
                h.update(data)
        return h.hexdigest()

    def _post_file(self, segment: Path) -> None:
        assert self.url is not None
        payload = segment.read_bytes()
        req = urllib.request.Request(self.url, method="POST", data=payload)
        req.add_header("Content-Type", "application/gzip")
        req.add_header("X-Log-Compression", "gzip")
        req.add_header("X-Schema-Version", str(FULL_LOG_SCHEMA_VERSION))
        req.add_header("X-Device-Id", self.device_id)
        req.add_header("X-Vehicle", self.vehicle)
        req.add_header("X-Vehicle-Short", self.vehicle_short)
        req.add_header("X-Log-Segment", segment.name)
        req.add_header("X-File-Bytes", str(len(payload)))
        req.add_header("X-File-Sha256", self._sha256_file(segment))
        with urllib.request.urlopen(req, timeout=10) as resp:
            code = int(getattr(resp, "status", 200))
            if code < 200 or code >= 300:
                raise RuntimeError(f"upload failed: HTTP {code}")

    def _loop(self) -> None:
        backoff = 0.5
        while not self.stop_event.is_set():
            try:
                did_work = False
                self._compress_backlog()
                for p in self._list_segments():
                    if self.stop_event.is_set():
                        break
                    try:
                        if self._is_uploaded(p):
                            continue
                        self._post_file(p)
                        self._mark_uploaded(p)
                        did_work = True
                        backoff = 0.5
                        self.last_error = None
                        # Persist after every confirmed file so retries stay per-file.
                        self._save_state()
                    except (OSError, urllib.error.URLError, urllib.error.HTTPError, RuntimeError) as e:
                        self.last_error = str(e)
                        break
                if not did_work:
                    if self.stop_event.wait(1.0):
                        break
            except Exception as e:
                self.last_error = str(e)
            # Backoff on errors
            if self.last_error:
                if self.stop_event.wait(backoff):
                    break
                backoff = min(backoff * 2.0, 30.0)


class DetailedWifiLogger:
    def __init__(self, state: AppState, log_file: Optional[str]):
        self.state = state
        configured = Path(log_file).expanduser() if log_file else Path("wifilogs")
        self.log_dir = (configured.parent / "wifilogs") if configured.suffix else configured
        self.enabled = False
        self.last_error: Optional[str] = None
        self.stop_event = threading.Event()
        self.queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=4096)
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()
        self.current_session_file: Optional[Path] = None

    def start(self):
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        self.state.set_detailed_wifi_logging(enabled=False, file=None, last_error=None)

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=2)

    def set_enabled(self, enabled: bool):
        enabled = bool(enabled)
        with self.lock:
            if enabled and not self.enabled:
                stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
                self.current_session_file = self.log_dir / f"wifi_capture_{stamp}.jsonl"
            if not enabled:
                self.current_session_file = None
            self.enabled = enabled
            current_file = str(self.current_session_file) if self.current_session_file else None
        self.state.set_detailed_wifi_logging(enabled=self.enabled, file=current_file, last_error=self.last_error)

    def enqueue_wifi_sample(
        self,
        wifi: Optional[Dict[str, Any]],
        gnss: Optional[Dict[str, Any]],
        ts_ms: int,
        *,
        gateways: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        with self.lock:
            enabled = self.enabled
            session_file = str(self.current_session_file) if self.current_session_file else None
        if not enabled or not session_file or not wifi:
            return
        gateways_out: Dict[str, Dict[str, Any]] = {}
        if gateways and isinstance(gateways, dict):
            # Keep only the fields needed for visualization/analysis.
            for name, info in gateways.items():
                try:
                    if not isinstance(info, dict):
                        continue
                    gateways_out[str(name)] = {
                        "latency_ms": info.get("latency_ms"),
                        "status": info.get("status"),
                        "host": info.get("host"),
                        "port": info.get("port"),
                    }
                except Exception:
                    continue
        self._put(
            {
                "type": "wifi_sample",
                "ts_ms": ts_ms,
                "_session_file": session_file,
                "vehicle": self.state.vehicle.name,
                "vehicle_short": self.state.vehicle.short_name,
                "wifi": {
                    "tx_rate_mbps": wifi.get("tx_rate_mbps"),
                    "rx_rate_mbps": wifi.get("rx_rate_mbps"),
                    "tx_bytes": wifi.get("tx_bytes"),
                    "rx_bytes": wifi.get("rx_bytes"),
                    "rssi_dbm": wifi.get("rssi_dbm"),
                    "signal": wifi.get("signal"),
                    "signal_avg_dbm": wifi.get("signal_avg_dbm"),
                    "noise_dbm": wifi.get("noise_dbm"),
                    "bssid": wifi.get("bssid"),
                    "channel": wifi.get("channel"),
                    "beacon_loss_count": wifi.get("beacon_loss_count"),
                    "max_probe_tries": wifi.get("max_probe_tries"),
                    "bssid_change_ms": wifi.get("bssid_change_ms"),
                },
                "gnss": gnss,
                "gateways": gateways_out or None,
            }
        )

    def enqueue_roaming_event(self, event_type: str, details: Dict[str, Any], gnss: Optional[Dict[str, Any]], ts_ms: int):
        with self.lock:
            enabled = self.enabled
            session_file = str(self.current_session_file) if self.current_session_file else None
        if not enabled or not session_file:
            return
        self._put(
            {
                "type": "roaming_event",
                "event": event_type,
                "ts_ms": ts_ms,
                "_session_file": session_file,
                "vehicle": self.state.vehicle.name,
                "vehicle_short": self.state.vehicle.short_name,
                "details": details,
                "gnss": gnss,
            }
        )

    def _put(self, payload: Dict[str, Any]) -> None:
        try:
            self.queue.put_nowait(payload)
        except queue.Full:
            self.last_error = "detailed wifi log queue overflow"
            with self.lock:
                current_file = str(self.current_session_file) if self.current_session_file else None
                enabled = self.enabled
            self.state.set_detailed_wifi_logging(enabled=enabled, file=current_file, last_error=self.last_error)

    def _loop(self):
        while not self.stop_event.is_set():
            try:
                item = self.queue.get(timeout=0.5)
            except queue.Empty:
                continue
            session_file = item.pop("_session_file", None)
            if not session_file:
                continue
            try:
                with Path(session_file).open("a", encoding="utf-8") as fh:
                    fh.write(jsonlib.dumps(item, ensure_ascii=False) + "\n")
            except Exception as e:
                self.last_error = f"write failed: {e}"
                with self.lock:
                    current_file = str(self.current_session_file) if self.current_session_file else None
                    enabled = self.enabled
                self.state.set_detailed_wifi_logging(enabled=enabled, file=current_file, last_error=self.last_error)


class RoamingEventWatcher:
    def __init__(self, interface: str, on_event):
        self.interface = interface
        self.on_event = on_event
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None

    def start(self):
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=2)

    def _detect_event(self, line: str) -> Optional[str]:
        l = line.lower()
        if self.interface and self.interface not in l and "wlan" in l:
            return None
        if "search" in l or "scan started" in l:
            return "search"
        if "selection" in l or "selected bss" in l:
            return "selection"
        if "attach" in l or "connected to" in l or "associated" in l:
            return "attachment"
        if "cold reconnection" in l or ("disconnect" in l and "reconnect" in l):
            return "cold_reconnection"
        return None

    def _loop(self):
        while not self.stop_event.is_set():
            proc: Optional[subprocess.Popen[str]] = None
            try:
                proc = subprocess.Popen(
                    ["iw", "event", "-t"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    bufsize=1,
                )
                assert proc.stdout is not None
                while not self.stop_event.is_set():
                    line = proc.stdout.readline()
                    if not line:
                        break
                    event_type = self._detect_event(line.strip())
                    if event_type:
                        self.on_event(event_type, {"line": line.strip()}, int(time.time() * 1000))
            except Exception:
                pass
            finally:
                if proc is not None:
                    try:
                        proc.terminate()
                    except Exception:
                        pass
            if self.stop_event.wait(1.0):
                break


def _as_int_keys(d: Dict[str, Any]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for k, v in d.items():
        try:
            out[int(k)] = str(v)
        except Exception:
            continue
    return out


def _as_positive_float(value: Any, default: float) -> float:
    try:
        parsed = float(value)
        if parsed > 0:
            return parsed
    except Exception:
        pass
    return float(default)


def load_config(path: str) -> AppCfg:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    modbus_cfg = raw.get("pymodbus", {}) or {}
    modbus_port = int(modbus_cfg.get("port", DEFAULT_MODBUS_PORT))
    timeout = float(modbus_cfg.get("timeout", DEFAULT_TIMEOUT))
    poll_interval_sec = _as_positive_float(
        modbus_cfg.get("pollIntervalSec", modbus_cfg.get("pollInterval", modbus_cfg.get("pollSeconds"))),
        DEFAULT_POLL_INTERVAL_SEC,
    )
    unit_candidates = list(modbus_cfg.get("unitCandidates", DEFAULT_UNIT_CANDIDATES))
    coils_fallback = bool(modbus_cfg.get("coilsFallback", DEFAULT_COILS_FALLBACK))
    gnss_cfg = raw.get("gnss", {}) or {}
    wifi_cfg = raw.get("wifi", {}) or {}
    display_cfg = raw.get("display", {}) or {}
    latency_hosts_cfg = display_cfg.get("latencyHosts", []) or []
    latency_hosts: List[Tuple[str, str, int]] = []
    for entry in latency_hosts_cfg:
        try:
            name = str(entry.get("name") or entry.get("label") or entry.get("host") or "host")
            host = str(entry["host"])
            host_port = int(entry.get("port", modbus_port))
            latency_hosts.append((name, host, host_port))
        except Exception:
            continue
    vehicles: List[VehicleCfg] = []
    for v in raw.get("vehicles", []):
        v_gnss = v.get("gnss", {}) or {}
        v_gnss_port = v_gnss.get("port")
        v_admin_port = v_gnss.get("adminPort")
        v_short = v.get("shortName") or v.get("short_name") or None
        v_external_ip = v.get("externalIp") or v.get("external_ip") or None
        v_name = str(v.get("name", "vehicle"))
        ctrls: List[ControllerCfg] = []
        for c in v.get("controllers", []):
            points = [
                PointCfg(
                    ref=int(p["ref"]),
                    label=str(p["label"]),
                    invert=bool(p.get("invert", False)),
                    style=p.get("style"),
                    passthrough=str(p["passthrough"]).strip() if p.get("passthrough") else None,
                )
                for p in c.get("points", [])
            ]
            ctrls.append(
                ControllerCfg(
                    name=str(c.get("name", c.get("host", "ctrl"))),
                    host=str(c["host"]),
                    base=int(c["base"]),
                    points=points,
                    gear_points=_as_int_keys(c.get("gear_points", {})),
                    extra_points=_as_int_keys(c.get("extra_points", {})),
                    passthrough_gears=_as_int_keys(c.get("passthrough_gears", {})),
                    passthrough_extra=_as_int_keys(c.get("passthrough_extra", {})),
                    model=c.get("model"),
                )
            )
        bridge_mappings: List[BridgeMappingCfg] = []
        bridge_raw = v.get("bridge", {}) or {}
        for m in (bridge_raw.get("mappings", []) or []):
            try:
                name = str(m.get("name") or "mapping")
                on_error = str(m.get("on_error") or m.get("onError") or "hold").lower()
                invert = bool(m.get("invert", False))
                debounce_ms = int(m.get("debounce_ms") or m.get("debounceMs") or 0)
                logic = m.get("logic")
                if logic is not None:
                    logic = str(logic)

                raw_inputs = None
                if "inputs" in m and m.get("inputs") is not None:
                    raw_inputs = m.get("inputs") or []
                elif "input" in m and m.get("input") is not None:
                    raw_inputs = [m.get("input")]
                else:
                    raw_inputs = []

                inputs: List[BridgeInput] = []
                for idx, inp in enumerate(raw_inputs):
                    if not isinstance(inp, dict):
                        continue
                    inputs.append(
                        BridgeInput(
                            controller=str(inp.get("controller") or inp.get("device") or ""),
                            ref=int(inp.get("ref")),
                            source_type=str(inp.get("source_type") or inp.get("sourceType") or "di"),
                            name=str(inp.get("name")) if inp.get("name") is not None else None,
                        )
                    )
                out_raw = m.get("output") or {}
                output = BridgeOutput(
                    controller=str(out_raw.get("controller") or out_raw.get("device") or ""),
                    address=int(out_raw.get("address")),
                )
                if not inputs or not output.controller:
                    continue
                bridge_mappings.append(
                    BridgeMappingCfg(
                        name=name,
                        inputs=inputs,
                        output=output,
                        logic=logic,
                        invert=invert,
                        debounce_ms=debounce_ms,
                        on_error=on_error,
                    )
                )
            except Exception:
                continue
        vehicles.append(
            VehicleCfg(
                name=v_name,
                controllers=ctrls,
                short_name=str(v_short) if v_short else derive_short_vehicle_name(v_name),
                external_ip=str(v_external_ip) if v_external_ip else None,
                gnss_host=str(v_gnss.get("host")) if v_gnss.get("host") else None,
                gnss_port=int(v_gnss_port) if v_gnss_port is not None else None,
                gnss_admin_host=str(v_gnss.get("adminHost")) if v_gnss.get("adminHost") else None,
                gnss_admin_port=int(v_admin_port) if v_admin_port is not None else None,
                bridge_mappings=bridge_mappings,
            )
        )
    wifi_refresh_raw = wifi_cfg.get("refreshSeconds")
    try:
        wifi_refresh = float(wifi_refresh_raw) if wifi_refresh_raw is not None else None
    except Exception:
        wifi_refresh = None
    if wifi_refresh is not None and wifi_refresh <= 0:
        wifi_refresh = None
    gnss_port = gnss_cfg.get("port")
    logging_cfg = raw.get("logging", {}) or {}
    app_log_file = logging_cfg.get("appLogFile") or logging_cfg.get("app_log_file") or None
    app_log_level = logging_cfg.get("appLogLevel") or logging_cfg.get("app_log_level") or logging_cfg.get("level") or None
    full_cfg = logging_cfg.get("full") or logging_cfg.get("fullLog") or {}
    if not isinstance(full_cfg, dict):
        full_cfg = {}
    upload_cfg = raw.get("upload", {}) or {}
    if not isinstance(upload_cfg, dict):
        upload_cfg = {}

    def _as_bool(x: Any, default: bool = False) -> bool:
        if x is None:
            return bool(default)
        if isinstance(x, bool):
            return x
        s = str(x).strip().lower()
        if s in ("1", "true", "yes", "y", "on"):
            return True
        if s in ("0", "false", "no", "n", "off"):
            return False
        return bool(default)

    def _as_int(x: Any, default: int) -> int:
        try:
            return int(x)
        except Exception:
            return int(default)

    def _as_float(x: Any, default: float) -> float:
        try:
            return float(x)
        except Exception:
            return float(default)

    pt_raw = raw.get("passthrough", {}) or {}
    pt_groups: Dict[str, int] = {}
    for k, v in (pt_raw.get("groups", {}) or {}).items():
        try:
            pt_groups[str(k)] = int(v)
        except Exception:
            continue

    return AppCfg(
        port=modbus_port,
        timeout=timeout,
        poll_interval_sec=poll_interval_sec,
        unit_candidates=unit_candidates,
        coils_fallback=coils_fallback,
        gnss_host=str(gnss_cfg.get("host")) if gnss_cfg.get("host") else None,
        gnss_port=int(gnss_port) if gnss_port is not None else None,
        wifi_iface=str(wifi_cfg.get("interface")).strip() if wifi_cfg.get("interface") else None,
        wifi_refresh=wifi_refresh,
        detailed_wifi_log_file=str(wifi_cfg.get("detailedLogFile")).strip() if wifi_cfg.get("detailedLogFile") else None,
        app_log_file=str(app_log_file).strip() if app_log_file else None,
        app_log_level=str(app_log_level).strip() if app_log_level else None,
        full_log_enabled=_as_bool(full_cfg.get("enabled"), False),
        full_log_dir=str(full_cfg.get("dir") or full_cfg.get("baseDir") or "logs/full"),
        full_log_snapshot_interval_sec=_as_float(full_cfg.get("snapshotIntervalSec"), 1.0),
        full_log_rotate_bytes=_as_int(full_cfg.get("rotateBytes"), 5 * 1024 * 1024),
        upload_enabled=_as_bool(upload_cfg.get("enabled"), False),
        upload_url=str(upload_cfg.get("url")).strip() if upload_cfg.get("url") else None,
        upload_device_id=str(upload_cfg.get("deviceId") or upload_cfg.get("device_id")).strip() if (upload_cfg.get("deviceId") or upload_cfg.get("device_id")) else None,
        upload_chunk_bytes=_as_int(upload_cfg.get("chunkBytes") or upload_cfg.get("chunk_bytes"), 256 * 1024),
        upload_state_file=str(upload_cfg.get("stateFile") or upload_cfg.get("state_file") or "logs/upload_state.json"),
        display_latency_hosts=latency_hosts,
        vehicles=vehicles,
        passthrough=PassthroughCfg(
            enabled=_as_bool(pt_raw.get("enabled"), False),
            bind=str(pt_raw.get("bind") or pt_raw.get("host") or "0.0.0.0"),
            port=_as_int(pt_raw.get("port"), 502),
            unit_id=_as_int(pt_raw.get("unitId") or pt_raw.get("unit_id"), 1),
            groups=pt_groups,
        ),
    )


def write_default_config(path: str) -> None:
    default = {
        "pymodbus": {
            "port": DEFAULT_MODBUS_PORT,
            "timeout": DEFAULT_TIMEOUT,
            "pollIntervalSec": DEFAULT_POLL_INTERVAL_SEC,
            "unitCandidates": DEFAULT_UNIT_CANDIDATES,
            "coilsFallback": DEFAULT_COILS_FALLBACK,
        },
        "gnss": {"host": "192.168.1.50", "port": 2947},
        "wifi": {"interface": "wlp2s0", "refreshSeconds": 2, "detailedLogFile": "wifilogs"},
        "logging": {
            "appLogFile": "logs/app.log",
            "appLogLevel": "WARNING",
            "full": {"enabled": True, "dir": "logs/full", "snapshotIntervalSec": 1.0, "rotateBytes": 5242880},
        },
        "upload": {"enabled": False, "url": "http://collector:9000/ingest", "deviceId": "device-001", "chunkBytes": 262144, "stateFile": "logs/upload_state.json"},
        "display": {"latencyHosts": [{"name": "gateway", "host": "192.168.1.1"}]},
        "vehicles": [
            {
                "name": "Tractor A",
                "controllers": [
                    {
                        "name": "MB_IO_1",
                        "model": "ICPDAS ET-7002",
                        "host": "172.16.102.4",
                        "base": 10000,
                        "points": [
                            {"ref": 10001, "label": "Ручник", "invert": False, "style": "handbrake"},
                            {"ref": 10002, "label": "Ремень", "invert": True, "style": "seatbelt"},
                            {"ref": 10003, "label": "Двигатель", "invert": True, "style": "engine"},
                        ],
                    },
                    {
                        "name": "MB_IO_2",
                        "model": "ICPDAS ET-7051",
                        "host": "172.16.102.5",
                        "base": 10000,
                        "points": [
                            {"ref": 10000, "label": "тормоз"},
                            {"ref": 10002, "label": "левый поворотник", "style": "indicator"},
                            {"ref": 10003, "label": "правый поворотник", "style": "indicator"},
                            {"ref": 10005, "label": "ближний свет"},
                            {"ref": 10009, "label": "Гудок"},
                        ],
                    },
                ],
            }
        ],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(default, f, ensure_ascii=False, indent=2)


def silence_lib_logs(verbose: bool) -> None:
    root_level = logging.WARNING if verbose else logging.ERROR
    logging.basicConfig(level=root_level)
    for name in ("pymodbus", "pymodbus.client", "pymodbus.transaction", "pymodbus.framer", "pymodbus.factory"):
        lg = logging.getLogger(name)
        lg.propagate = False
        try:
            lg.handlers.clear()
        except Exception:
            lg.handlers = []
        lg.addHandler(logging.NullHandler())
        lg.setLevel(logging.CRITICAL if not verbose else logging.ERROR)


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


def _evaluate_expression(expr: str, values: Dict[str, bool]) -> bool:
    """
    Evaluate a simple boolean expression using provided values.
    Supports AND/OR/NOT and XOR (using ^) operators.
    """

    def _eval(node: ast.AST) -> bool:
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.BoolOp):
            vals = [_eval(v) for v in node.values]
            if isinstance(node.op, ast.And):
                return all(vals)
            if isinstance(node.op, ast.Or):
                return any(vals)
            raise ValueError("Only AND/OR boolean operators are supported")
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitXor):
            return _eval(node.left) ^ _eval(node.right)
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return not _eval(node.operand)
        if isinstance(node, ast.Name):
            if node.id not in values:
                raise ValueError(f"Unknown variable '{node.id}' in logic expression")
            return bool(values[node.id])
        if isinstance(node, ast.Constant) and isinstance(node.value, bool):
            return bool(node.value)
        raise ValueError("Unsupported expression; use AND, OR, NOT, XOR (^), and parentheses")

    parsed = ast.parse(expr, mode="eval")
    return bool(_eval(parsed))


class IOBridgeEvaluator:
    def __init__(self, mappings: List[BridgeMappingCfg]):
        self.mappings = list(mappings or [])
        self._last_written: Dict[str, Optional[bool]] = {m.name: None for m in self.mappings}
        self._candidate_state: Dict[str, Optional[bool]] = {m.name: None for m in self.mappings}
        self._candidate_since: Dict[str, Optional[float]] = {m.name: None for m in self.mappings}
        self._logger = logging.getLogger("io_bridge")

    def extra_refs_for(self, controller_name: str) -> List[int]:
        refs: List[int] = []
        for m in self.mappings:
            for inp in m.inputs:
                if inp.controller == controller_name:
                    refs.append(int(inp.ref))
        return refs

    def evaluate(
        self,
        ref_values: Dict[str, Dict[int, Optional[bool]]],
        clients: Dict[str, MBClient],
    ) -> None:
        now = time.monotonic()
        for m in self.mappings:
            desired = self._compute_desired(m, ref_values)
            debounce_s = max(0.0, float(m.debounce_ms or 0) / 1000.0)

            if debounce_s <= 0:
                self._maybe_write(m, desired, clients)
                continue

            cand = self._candidate_state.get(m.name)
            since = self._candidate_since.get(m.name)
            if cand is None or cand != desired:
                self._candidate_state[m.name] = desired
                self._candidate_since[m.name] = now
                continue

            if since is not None and (now - since) >= debounce_s:
                self._maybe_write(m, desired, clients)

    def _compute_desired(
        self,
        m: BridgeMappingCfg,
        ref_values: Dict[str, Dict[int, Optional[bool]]],
    ) -> Optional[bool]:
        vals: Dict[str, bool] = {}
        for idx, inp in enumerate(m.inputs):
            per_ctrl = ref_values.get(inp.controller) or {}
            raw = per_ctrl.get(int(inp.ref))
            if raw is None:
                return self._on_error_value(m.on_error)
            name = inp.name or f"in{idx+1}"
            vals[name] = bool(raw)

        try:
            if m.logic:
                out = _evaluate_expression(m.logic, vals)
            else:
                if len(vals) != 1:
                    raise ValueError(f"mapping '{m.name}' requires logic for multiple inputs")
                out = next(iter(vals.values()))
        except Exception as e:
            self._logger.warning("Bridge mapping '%s' logic error: %s", m.name, e)
            return self._on_error_value(m.on_error)

        out = (not out) if m.invert else out
        return bool(out)

    def _on_error_value(self, on_error: str) -> Optional[bool]:
        mode = str(on_error or "hold").lower()
        if mode == "force_off":
            return False
        if mode == "force_on":
            return True
        return None  # hold

    def _maybe_write(self, m: BridgeMappingCfg, desired: Optional[bool], clients: Dict[str, MBClient]) -> None:
        if desired is None:
            return
        last = self._last_written.get(m.name)
        if last is not None and last == desired:
            return
        client = clients.get(m.output.controller)
        if client is None:
            self._logger.warning("Bridge mapping '%s' output controller not found: %s", m.name, m.output.controller)
            return
        ok = client.write_coil(int(m.output.address), bool(desired))
        if ok:
            self._last_written[m.name] = bool(desired)
        else:
            self._logger.warning(
                "Bridge mapping '%s' coil write failed (%s:%s desired=%s): %s",
                m.name,
                m.output.controller,
                m.output.address,
                int(bool(desired)),
                client.last_error,
            )


def check_tcp(host: str, port: int, timeout: float) -> Tuple[bool, Optional[float], Optional[str]]:
    start = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            end = time.perf_counter()
            return True, (end - start) * 1000.0, None
    except Exception as e:
        if isinstance(e, TimeoutError):
            return False, None, "timeout"
        if isinstance(e, ConnectionRefusedError):
            return False, None, "refused"
        if isinstance(e, OSError) and "No route" in str(e):
            return False, None, "no route"
        return False, None, f"{e.__class__.__name__}: {e}"


def _freq_to_channel(freq_mhz: Optional[int]) -> Optional[int]:
    if freq_mhz is None:
        return None
    if 2412 <= freq_mhz <= 2472:
        return (freq_mhz - 2407) // 5
    if freq_mhz == 2484:
        return 14
    if 5000 <= freq_mhz <= 5900:
        return (freq_mhz - 5000) // 5
    return None


def get_wifi_status(interface: str) -> Dict[str, Optional[Any]]:
    cmd = ["iw", "dev", interface, "link"]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=3)
    except Exception as e:
        return {"status": "error", "error": f"exec failed: {e}"}
    stdout = (res.stdout or "").strip()
    stderr = (res.stderr or "").strip()
    if res.returncode != 0:
        return {"status": "down", "error": stderr or stdout or "iw failed"}
    if not stdout or "Not connected" in stdout:
        return {"status": "no link"}
    ssid = bssid = signal = None
    freq = None
    signal_dbm = tx_rate_mbps = rx_rate_mbps = None
    noise_dbm: Optional[float] = None
    beacon_loss_count: Optional[int] = None
    max_probe_tries: Optional[int] = None
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("Connected to"):
            parts = line.split()
            if len(parts) >= 3:
                bssid = parts[2]
        elif line.startswith("SSID:"):
            ssid = line.split(":", 1)[1].strip() or None
        elif line.startswith("freq:"):
            try:
                freq = int(line.split(":", 1)[1].strip())
            except Exception:
                freq = None
        elif line.startswith("signal:"):
            match = re.search(r"(-?\d+\s*dBm)", line)
            if match:
                signal = match.group(1)
                try:
                    signal_dbm = float(match.group(1).split()[0])
                except Exception:
                    signal_dbm = None
            noise_match = re.search(r"noise:\s*(-?\d+)\s*dBm", line)
            if noise_match:
                try:
                    noise_dbm = float(noise_match.group(1))
                except Exception:
                    noise_dbm = None
        elif "bitrate:" in line:
            match = re.search(r"^(tx|rx)\s+bitrate:\s*([0-9.]+)\s*MBit/s", line)
            if match:
                rate = float(match.group(2))
                if match.group(1) == "tx":
                    tx_rate_mbps = rate
                else:
                    rx_rate_mbps = rate
        elif "beacon loss count:" in line:
            try:
                beacon_loss_count = int(line.split(":", 1)[1].strip())
            except Exception:
                beacon_loss_count = None
        elif "max probe tries:" in line:
            try:
                max_probe_tries = int(line.split(":", 1)[1].strip())
            except Exception:
                max_probe_tries = None
    chan = _freq_to_channel(freq)
    freq_txt = f"{freq} MHz" if freq is not None else None
    if chan is not None:
        freq_txt = f"{freq_txt} (ch {chan})" if freq_txt else f"ch {chan}"
    return {
        "status": "connected",
        "ssid": ssid,
        "bssid": bssid,
        "signal": signal,
        "signal_dbm": signal_dbm,
        "rssi_dbm": signal_dbm,
        "noise_dbm": noise_dbm,
        "freq": freq_txt,
        "channel": chan,
        "tx_rate_mbps": tx_rate_mbps,
        "rx_rate_mbps": rx_rate_mbps,
        "beacon_loss_count": beacon_loss_count,
        "max_probe_tries": max_probe_tries,
    }


def _run_iw_command(args: List[str], timeout: float = 4.0) -> str:
    try:
        res = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except Exception:
        return ""
    if res.returncode != 0:
        return ""
    return (res.stdout or "").strip()


def parse_station_dump(output: str) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {
        "inactive_time_ms": None,
        "connected_time_ms": None,
        "tx_retries": None,
        "tx_failed": None,
        "signal_dbm": None,
        "signal_avg_dbm": None,
        "rx_bytes": None,
        "tx_bytes": None,
    }
    for raw in output.splitlines():
        line = raw.strip()
        if line.startswith("inactive time:"):
            m = re.search(r"(\d+)\s*ms", line)
            out["inactive_time_ms"] = float(m.group(1)) if m else None
        elif line.startswith("connected time:"):
            m = re.search(r"(\d+)\s*seconds?", line)
            out["connected_time_ms"] = float(m.group(1)) * 1000.0 if m else None
        elif line.startswith("tx retries:"):
            m = re.search(r"(\d+)", line)
            out["tx_retries"] = float(m.group(1)) if m else None
        elif line.startswith("tx failed:"):
            m = re.search(r"(\d+)", line)
            out["tx_failed"] = float(m.group(1)) if m else None
        elif line.startswith("signal avg:"):
            m = re.search(r"(-?\d+)\s*dBm", line)
            out["signal_avg_dbm"] = float(m.group(1)) if m else None
        elif line.startswith("signal:"):
            m = re.search(r"(-?\d+)\s*dBm", line)
            out["signal_dbm"] = float(m.group(1)) if m else None
        elif line.startswith("rx bytes:"):
            m = re.search(r"(\d+)", line)
            out["rx_bytes"] = float(m.group(1)) if m else None
        elif line.startswith("tx bytes:"):
            m = re.search(r"(\d+)", line)
            out["tx_bytes"] = float(m.group(1)) if m else None
    return out


def parse_iw_scan_candidates(output: str, current_ssid: Optional[str]) -> Dict[str, Optional[float]]:
    best_rssi: Optional[float] = None
    count = 0
    cur_ssid: Optional[str] = None
    cur_signal: Optional[float] = None
    for raw in output.splitlines():
        line = raw.strip()
        if line.startswith("BSS "):
            if current_ssid and cur_ssid == current_ssid and cur_signal is not None:
                count += 1
                if best_rssi is None or cur_signal > best_rssi:
                    best_rssi = cur_signal
            cur_ssid = None
            cur_signal = None
        elif line.startswith("SSID:"):
            cur_ssid = line.split(":", 1)[1].strip() or None
        elif line.startswith("signal:"):
            m = re.search(r"(-?\d+(?:\.\d+)?)\s*dBm", line)
            cur_signal = float(m.group(1)) if m else None
    if current_ssid and cur_ssid == current_ssid and cur_signal is not None:
        count += 1
        if best_rssi is None or cur_signal > best_rssi:
            best_rssi = cur_signal
    return {"candidate_count": float(count) if count else None, "top_candidate_rssi": best_rssi}


def get_cpu_load() -> Optional[float]:
    try:
        load1, _, _ = os.getloadavg()
        cores = os.cpu_count() or 1
        return max(0.0, float(load1) / float(cores) * 100.0)
    except Exception:
        return None


def get_soc_temp_c(sensor_name: str = "soc_thermal-virtual-0") -> Optional[float]:
    base = Path("/sys/class/thermal")
    if not base.exists():
        return None
    try:
        for zone in sorted(base.glob("thermal_zone*")):
            type_file = zone / "type"
            temp_file = zone / "temp"
            if not type_file.exists() or not temp_file.exists():
                continue
            sensor = type_file.read_text(encoding="utf-8", errors="ignore").strip()
            if sensor != sensor_name:
                continue
            raw = temp_file.read_text(encoding="utf-8", errors="ignore").strip()
            val = float(raw)
            if abs(val) > 300:
                val = val / 1000.0
            return val
    except Exception:
        return None
    return None


def apply_bssid_transition_timing(
    wifi: Dict[str, Any], last_bssid: Optional[str], last_bssid_seen_ms: Optional[int], ts_ms: int
) -> Tuple[Optional[str], Optional[int]]:
    bssid = wifi.get("bssid")
    if bssid and bssid != last_bssid:
        if last_bssid_seen_ms is not None:
            wifi["bssid_change_ms"] = ts_ms - last_bssid_seen_ms
        return str(bssid), ts_ms
    if bssid and last_bssid_seen_ms is None:
        return str(bssid), ts_ms
    return last_bssid, last_bssid_seen_ms


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


def _nmea_to_deg(raw: str, direction: str) -> Optional[float]:
    if not raw or not direction:
        return None
    try:
        val = float(raw)
        deg = int(val // 100)
        minutes = val - deg * 100
        deg = deg + minutes / 60.0
        if direction in ("S", "W"):
            deg = -deg
        return deg
    except Exception:
        return None


class GNSSClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.state: Optional[Dict[str, Any]] = None
        self.last_gsa_fix: Optional[str] = None

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)

    def snapshot(self) -> Optional[Dict[str, Any]]:
        with self.lock:
            return dict(self.state) if self.state is not None else None

    def _run(self):
        while not self.stop_event.is_set():
            try:
                with socket.create_connection((self.host, self.port), timeout=5) as sock:
                    sock.settimeout(5)
                    self.last_gsa_fix = None
                    fh = sock.makefile("r", encoding="ascii", errors="ignore", newline="\n")
                    while not self.stop_event.is_set():
                        line = fh.readline()
                        if not line:
                            break
                        self._handle_line(line.strip())
            except Exception:
                pass
            if self.stop_event.wait(2.0):
                break

    def _handle_line(self, line: str):
        if "*" in line:
            line = line.split("*", 1)[0]
        parts = line.split(",")
        if not parts or not parts[0].startswith("$"):
            return
        sent = parts[0][1:]
        if sent.endswith("GSA"):
            fix_map = {"1": "NO FIX", "2": "FIX 2D", "3": "FIX 3D"}
            if len(parts) > 2:
                self.last_gsa_fix = fix_map.get(parts[2])
        elif sent.endswith("GGA"):
            if len(parts) < 9:
                return
            try:
                fix_quality = int(parts[6] or 0)
            except Exception:
                return
            if fix_quality <= 0:
                return
            try:
                sats = int(parts[7]) if parts[7] else None
            except Exception:
                sats = None
            try:
                hdop = float(parts[8]) if parts[8] else None
            except Exception:
                hdop = None
            fix_map = {0: "NO FIX", 1: "FIX", 2: "DGPS"}
            solution_map = {0: "none", 1: "single", 2: "dgps"}
            with self.lock:
                self.state = {
                    "fix": self.last_gsa_fix or fix_map.get(fix_quality, "FIX"),
                    "solution_level": solution_map.get(fix_quality, "single"),
                    "sats": sats,
                    "hdop": hdop,
                    "lat": _nmea_to_deg(parts[2], parts[3]),
                    "lon": _nmea_to_deg(parts[4], parts[5]),
                    "ts": time.time(),
                }


class Poller:
    def __init__(
        self,
        args,
        appcfg: AppCfg,
        vehicle: VehicleCfg,
        state: AppState,
        *,
        bridge: Optional[IOBridgeEvaluator] = None,
    ):
        self.args = args
        self.appcfg = appcfg
        self.vehicle = vehicle
        self.state = state
        self.bridge = bridge
        self.stop_event = threading.Event()
        self.reconnect_event = threading.Event()
        self.clients: Dict[str, MBClient] = {}
        self.latency: Dict[str, Optional[float]] = {}
        self.status: Dict[str, str] = {}
        self.debug_msgs: Dict[str, str] = {}
        self.last_net_check_time = 0.0
        self.last_wifi_check_time = 0.0
        self.last_system_check_time = 0.0
        self.system_refresh_interval = 1.0
        self._tcp_check_lock = threading.Lock()
        self._tcp_check_inflight = False
        self.gnss_client: Optional[GNSSClient] = None
        self.detailed_wifi_logger = DetailedWifiLogger(self.state, self.appcfg.detailed_wifi_log_file)
        self.roaming_watcher: Optional[RoamingEventWatcher] = None
        self.last_bssid: Optional[str] = None
        self.last_bssid_seen_ms: Optional[int] = None
        self.last_channel: Optional[int] = None
        self.last_station_stats: Optional[Dict[str, Optional[float]]] = None
        self.last_scan_ts: float = 0.0
        self.last_scan_candidates: Dict[str, Optional[float]] = {"candidate_count": None, "top_candidate_rssi": None}
        self.signal_window: deque = deque(maxlen=24)
        self.last_reason_code: Optional[int] = None
        self.poll_interval = _as_positive_float(getattr(self.args, "poll", None), DEFAULT_POLL_INTERVAL_SEC)
        self.wifi_refresh_interval = self.appcfg.wifi_refresh if self.appcfg.wifi_refresh is not None else self.poll_interval * 2
        self.poll_net_interval = self.args.poll_net
        self._reconnect_times = deque()  # monotonic timestamps
        self._build_clients()
        self._modbus_enabled = True
        self.state.set_modbus_enabled(True)
        gnss_host = self.vehicle.gnss_host or self.appcfg.gnss_host
        gnss_port = self.vehicle.gnss_port or self.appcfg.gnss_port
        if gnss_host and gnss_port:
            self.gnss_client = GNSSClient(str(gnss_host), int(gnss_port))
            self.gnss_client.start()
        if self.appcfg.wifi_iface:
            self.roaming_watcher = RoamingEventWatcher(self.appcfg.wifi_iface, self._on_roaming_event)
            self.roaming_watcher.start()
        self.detailed_wifi_logger.start()

    def _collect_station_stats(self) -> Dict[str, Optional[float]]:
        if not self.appcfg.wifi_iface:
            return {}
        return parse_station_dump(_run_iw_command(["iw", "dev", self.appcfg.wifi_iface, "station", "dump"]))

    def _collect_scan_candidates(self, ssid: Optional[str]) -> Dict[str, Optional[float]]:
        if not self.appcfg.wifi_iface:
            return {"candidate_count": None, "top_candidate_rssi": None}
        return parse_iw_scan_candidates(_run_iw_command(["iw", "dev", self.appcfg.wifi_iface, "scan"], timeout=8.0), ssid)

    def _extract_reason_code(self, line: str) -> Optional[int]:
        m = re.search(r"reason(?:\s*code)?\s*[:=]\s*(\d+)", line.lower())
        return int(m.group(1)) if m else None

    def _on_roaming_event(self, event_type: str, details: Dict[str, Any], ts_ms: int):
        line = str(details.get("line") or "")
        reason_code = self._extract_reason_code(line)
        if reason_code is not None:
            self.last_reason_code = reason_code
        station_now = self._collect_station_stats()
        trend = list(self.signal_window)
        event_details = {
            "reason_code": reason_code if reason_code is not None else self.last_reason_code,
            "old_bssid": self.last_bssid,
            "new_bssid": None,
            "old_channel": self.last_channel,
            "new_channel": None,
            "candidate_count": self.last_scan_candidates.get("candidate_count"),
            "top_candidate_rssi": self.last_scan_candidates.get("top_candidate_rssi"),
            "connected_time_before_roam_ms": (self.last_station_stats or {}).get("connected_time_ms") if self.last_station_stats else None,
            "inactive_time_ms": station_now.get("inactive_time_ms"),
            "tx_retries_delta": None,
            "tx_failed_delta": None,
            "signal_trend": trend,
            "signal": station_now.get("signal_dbm"),
            "signal_avg": station_now.get("signal_avg_dbm"),
        }
        if self.last_station_stats:
            before_retry = self.last_station_stats.get("tx_retries")
            now_retry = station_now.get("tx_retries")
            before_fail = self.last_station_stats.get("tx_failed")
            now_fail = station_now.get("tx_failed")
            if isinstance(before_retry, (int, float)) and isinstance(now_retry, (int, float)):
                event_details["tx_retries_delta"] = now_retry - before_retry
            if isinstance(before_fail, (int, float)) and isinstance(now_fail, (int, float)):
                event_details["tx_failed_delta"] = now_fail - before_fail
        event_details.update(details)
        self.detailed_wifi_logger.enqueue_roaming_event(event_type, event_details, self.state.gnss_snapshot(), ts_ms)

    def set_detailed_wifi_logging(self, enabled: bool) -> None:
        self.detailed_wifi_logger.set_enabled(enabled)

    def set_modbus_enabled(self, enabled: bool) -> None:
        enabled = bool(enabled)
        if enabled == self._modbus_enabled:
            self.state.set_modbus_enabled(enabled)
            return
        self._modbus_enabled = enabled
        self.state.set_modbus_enabled(enabled)
        if not enabled:
            for c in (self.clients or {}).values():
                try:
                    c.close()
                except Exception:
                    pass
        else:
            # Recreate clients to ensure a clean reconnect after being disabled.
            self._build_clients()

    def _build_clients(self):
        # Close old sockets first to avoid controller-side session buildup.
        for c in (self.clients or {}).values():
            try:
                c.close()
            except Exception:
                pass
        self.clients = {}
        self.latency = {}
        self.status = {}
        self.debug_msgs = {}
        for cfg in self.vehicle.controllers:
            self.clients[cfg.name] = MBClient(
                cfg.host,
                self.args.port,
                self.args.timeout,
                self.args.unit_candidates,
                self.args.coils,
                guard=ConnectionGuard(host=str(cfg.host), modbus_port=int(self.args.port)),
                logger=logging.getLogger(f"modbus.{cfg.name}"),
            )

    def request_reconnect(self):
        self.reconnect_event.set()

    def _allow_reconnect_now(self) -> bool:
        now = time.monotonic()
        cutoff = now - RECONNECT_WINDOW_SEC
        while self._reconnect_times and self._reconnect_times[0] < cutoff:
            self._reconnect_times.popleft()
        if len(self._reconnect_times) >= RECONNECT_MAX_PER_WINDOW:
            return False
        self._reconnect_times.append(now)
        return True

    def _refresh_tcp_status_worker(self):
        try:
            timeout = min(float(self.args.timeout or 2.5), 1.5)

            def _check_controller(cfg: ControllerCfg):
                ok, latency_ms, why = check_tcp(cfg.host, self.args.port, timeout)
                return cfg.name, ok, latency_ms, why

            def _check_host(entry: Tuple[str, str, int]):
                name, host, port = entry
                ok, latency_ms, why = check_tcp(host, port, timeout)
                return name, host, port, ok, latency_ms, why

            threads: List[threading.Thread] = []
            ctrl_results: List[Tuple[str, bool, Optional[float], Optional[str]]] = []
            host_results: List[Tuple[str, str, int, bool, Optional[float], Optional[str]]] = []

            def _run_ctrl(cfg: ControllerCfg):
                ctrl_results.append(_check_controller(cfg))

            def _run_host(entry: Tuple[str, str, int]):
                host_results.append(_check_host(entry))

            for cfg in self.vehicle.controllers:
                t = threading.Thread(target=_run_ctrl, args=(cfg,), daemon=True)
                threads.append(t)
                t.start()
            for entry in self.appcfg.display_latency_hosts:
                t = threading.Thread(target=_run_host, args=(entry,), daemon=True)
                threads.append(t)
                t.start()
            for t in threads:
                t.join(timeout=timeout + 0.2)

            for name, ok, latency_ms, why in ctrl_results:
                self.latency[name] = latency_ms
                self.status[name] = "OK" if ok else f"TCP {why or 'fail'}"
                self.debug_msgs[name] = f"TCP ok ({latency_ms:.1f} ms)" if ok and latency_ms is not None else f"TCP check: {why}"
                self.state.set_controller(name, latency=latency_ms, status=self.status[name], debug=self.debug_msgs.get(name, ""))

            display_latency: Dict[str, Dict[str, Any]] = {}
            for name, host, port, ok, latency_ms, why in host_results:
                display_latency[name] = {
                    "latency": f"{latency_ms:.1f} ms" if latency_ms is not None else "-- ms",
                    "latency_ms": latency_ms,
                    "status": "OK" if ok else f"TCP {why or 'fail'}",
                    "host": host,
                    "port": str(port),
                }
            if display_latency:
                self.state.set_display_latency(display_latency)
        finally:
            with self._tcp_check_lock:
                self._tcp_check_inflight = False

    def refresh_tcp_status(self):
        with self._tcp_check_lock:
            if self._tcp_check_inflight:
                return
            self._tcp_check_inflight = True
        threading.Thread(target=self._refresh_tcp_status_worker, daemon=True).start()

    def poll_controller(self, cfg: ControllerCfg) -> Dict[int, Optional[bool]]:
        if not self._modbus_enabled:
            self.state.set_controller(cfg.name, status="DISABLED", debug="Modbus disabled via UI")
            return {}
        client = self.clients[cfg.name]
        point_refs = [p.ref for p in cfg.points]
        gear_refs = sorted(cfg.gear_points.keys())
        extra_refs = sorted(cfg.extra_points.keys())

        bridge_refs = self.bridge.extra_refs_for(cfg.name) if self.bridge else []
        needed_refs = point_refs + gear_refs + extra_refs + bridge_refs
        point_ref_set = set(point_refs)
        ref_values: Dict[int, Optional[bool]] = {}
        last_error_points: Optional[str] = None

        for chunk in chunk_refs_by_span(needed_refs, MAX_MODBUS_REF_SPAN):
            # Each chunk results in exactly one Modbus read_refs() request.
            vals = client.read_refs(cfg.base, chunk)
            ref_values.update(vals)
            if point_ref_set.intersection(chunk):
                last_error_points = client.last_error
        status = self.status.get(cfg.name, "OK")
        debug = self.debug_msgs.get(cfg.name, "")

        if point_refs:
            if all(ref_values.get(r) is None for r in point_refs):
                status = status if status.startswith("TCP") else "READ ERR"
                debug = last_error_points or client.last_error or debug
            elif not status.startswith("TCP"):
                status = "OK"
        elif not status.startswith("TCP"):
            status = "OK"

        point_payload = {str(p.ref): logical_state(p.invert, ref_values.get(p.ref)) for p in cfg.points}
        gear_payload = {str(ref): ref_values.get(ref) for ref in gear_refs}
        extra_payload = {str(ref): ref_values.get(ref) for ref in extra_refs}
        self.state.set_controller(
            cfg.name,
            points=point_payload,
            gears=gear_payload,
            extra=extra_payload,
            status=status,
            debug=debug,
            latency=self.latency.get(cfg.name),
        )
        return ref_values

    def run_once(self):
        now = time.monotonic()
        if self.reconnect_event.is_set():
            if self._allow_reconnect_now():
                self._build_clients()
            else:
                logging.getLogger("poller").warning(
                    "Reconnect rate-limit exceeded (%s per %ss)",
                    RECONNECT_MAX_PER_WINDOW,
                    int(RECONNECT_WINDOW_SEC),
                )
                for cfg in self.vehicle.controllers:
                    self.state.set_controller(
                        cfg.name,
                        debug=f"Reconnect rate-limited ({RECONNECT_MAX_PER_WINDOW} per {int(RECONNECT_WINDOW_SEC)}s)",
                    )
            self.reconnect_event.clear()
        if now - self.last_net_check_time >= self.poll_net_interval:
            self.refresh_tcp_status()
            self.last_net_check_time = now
        bridge_snapshot: Dict[str, Dict[int, Optional[bool]]] = {}
        for cfg in self.vehicle.controllers:
            bridge_snapshot[cfg.name] = self.poll_controller(cfg)
        if self.bridge:
            try:
                self.bridge.evaluate(bridge_snapshot, self.clients)
            except Exception as e:
                logging.getLogger("io_bridge").warning("Bridge evaluator error: %s", e)
        if now - self.last_wifi_check_time >= self.wifi_refresh_interval and self.appcfg.wifi_iface:
            wifi = get_wifi_status(self.appcfg.wifi_iface)
            ts_ms = int(time.time() * 1000)
            prev_bssid = self.last_bssid
            prev_channel = self.last_channel
            station_stats = self._collect_station_stats()
            wifi.update(
                {
                    "inactive_time_ms": station_stats.get("inactive_time_ms"),
                    "connected_time_ms": station_stats.get("connected_time_ms"),
                    "tx_retries": station_stats.get("tx_retries"),
                    "tx_failed": station_stats.get("tx_failed"),
                    "signal_avg_dbm": station_stats.get("signal_avg_dbm"),
                    "tx_bytes": station_stats.get("tx_bytes"),
                    "rx_bytes": station_stats.get("rx_bytes"),
                }
            )
            self.signal_window.append(
                {
                    "ts_ms": ts_ms,
                    "signal": station_stats.get("signal_dbm", wifi.get("signal_dbm")),
                    "signal_avg": station_stats.get("signal_avg_dbm"),
                }
            )
            if time.monotonic() - self.last_scan_ts >= max(self.wifi_refresh_interval * 4.0, 10.0):
                self.last_scan_candidates = self._collect_scan_candidates(wifi.get("ssid"))
                self.last_scan_ts = time.monotonic()
            self.last_bssid, self.last_bssid_seen_ms = apply_bssid_transition_timing(
                wifi, self.last_bssid, self.last_bssid_seen_ms, ts_ms
            )
            if wifi.get("bssid") and prev_bssid and wifi.get("bssid") != prev_bssid:
                details = {
                    "reason_code": self.last_reason_code,
                    "old_bssid": prev_bssid,
                    "new_bssid": wifi.get("bssid"),
                    "old_channel": prev_channel,
                    "new_channel": wifi.get("channel"),
                    "candidate_count": self.last_scan_candidates.get("candidate_count"),
                    "top_candidate_rssi": self.last_scan_candidates.get("top_candidate_rssi"),
                    "connected_time_before_roam_ms": (self.last_station_stats or {}).get("connected_time_ms") if self.last_station_stats else None,
                    "inactive_time_ms": station_stats.get("inactive_time_ms"),
                    "tx_retries_delta": None,
                    "tx_failed_delta": None,
                    "signal_trend": list(self.signal_window),
                    "signal": station_stats.get("signal_dbm", wifi.get("signal_dbm")),
                    "signal_avg": station_stats.get("signal_avg_dbm"),
                }
                if self.last_station_stats:
                    prev_retry = self.last_station_stats.get("tx_retries")
                    prev_fail = self.last_station_stats.get("tx_failed")
                    now_retry = station_stats.get("tx_retries")
                    now_fail = station_stats.get("tx_failed")
                    if isinstance(prev_retry, (int, float)) and isinstance(now_retry, (int, float)):
                        details["tx_retries_delta"] = now_retry - prev_retry
                    if isinstance(prev_fail, (int, float)) and isinstance(now_fail, (int, float)):
                        details["tx_failed_delta"] = now_fail - prev_fail
                self.detailed_wifi_logger.enqueue_roaming_event("attachment", details, self.state.gnss_snapshot(), ts_ms)
            self.last_channel = wifi.get("channel") if isinstance(wifi.get("channel"), int) else self.last_channel
            self.last_station_stats = station_stats
            self.state.set_wifi(wifi)
            self.detailed_wifi_logger.enqueue_wifi_sample(
                wifi,
                self.state.gnss_snapshot(),
                ts_ms,
                gateways=self.state.display_latency_snapshot(),
            )
            self.last_wifi_check_time = now
        if self.gnss_client:
            self.state.set_gnss(self.gnss_client.snapshot())
        if now - self.last_system_check_time >= self.system_refresh_interval:
            self.state.set_cpu_load(get_cpu_load())
            self.state.set_cpu_temp(get_soc_temp_c())
            self.last_system_check_time = now

    def start(self):
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def _loop(self):
        interval = self.poll_interval
        next_deadline = time.monotonic()
        while not self.stop_event.is_set():
            self.run_once()
            next_deadline += interval
            now = time.monotonic()
            if next_deadline <= now:
                next_deadline = now + interval
            if self.stop_event.wait(max(0.0, next_deadline - now)):
                break

    def stop(self):
        self.stop_event.set()
        self.detailed_wifi_logger.stop()
        if self.roaming_watcher:
            self.roaming_watcher.stop()
        if self.gnss_client:
            self.gnss_client.stop()
        if getattr(self, "thread", None):
            self.thread.join(timeout=2)


class WebServer:
    def __init__(self, state: AppState, host: str, port: int, broadcast_interval: float = 1.0, poller: Optional[Poller] = None):
        if jinja2 is None or web is None:
            raise RuntimeError("aiohttp and jinja2 are required for HTTP mode")
        self.state = state
        self.host = host
        self.port = port
        self.broadcast_interval = broadcast_interval
        self.poller = poller
        self.thread: Optional[threading.Thread] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.stop_event = threading.Event()
        self.websockets: set = set()
        self.jinja = jinja2.Environment(loader=jinja2.FileSystemLoader(str(TEMPLATE_DIR)), autoescape=True)
        self.template = self.jinja.get_template("dashboard.html")
        self.combined_template = self.jinja.get_template("combined_dashboard.html")
        self.logs_template = self.jinja.get_template("wifi_logs.html")
        self.app_logs_template = self.jinja.get_template("app_logs.html")
        self.external_logs_template = self.jinja.get_template("external_logs.html")

    async def index(self, _request: web.Request) -> web.Response:
        return web.Response(text=self.template.render(initial_state=jsonlib.dumps(self.state.snapshot())), content_type="text/html")

    async def combined_index(self, _request: web.Request) -> web.Response:
        return web.Response(text=self.combined_template.render(initial_state=jsonlib.dumps(self.state.snapshot())), content_type="text/html")

    async def app_logs_index(self, _request: web.Request) -> web.Response:
        initial = {
            "state": self.state.snapshot(),
            "logs": self.state.app_log_tail(50),
        }
        return web.Response(text=self.app_logs_template.render(initial_state=jsonlib.dumps(initial)), content_type="text/html")

    async def external_logs_index(self, request: web.Request) -> web.Response:
        source = str(request.match_info.get("source") or "").strip().lower()
        spec = EXTERNAL_LOG_SOURCES.get(source)
        if spec is None:
            raise web.HTTPNotFound()
        return web.Response(
            text=self.external_logs_template.render(
                page_title=spec.get("title", source),
                page_subtitle=spec.get("subtitle", ""),
                initial_state=jsonlib.dumps({"source": source}),
            ),
            content_type="text/html",
        )

    async def ping(self, _request: web.Request) -> web.Response:
        # Used by other dashboards to check if this vehicle's web UI is reachable.
        return web.Response(
            status=204,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-store",
            },
        )

    def _wifi_logs_dir(self) -> Path:
        configured = Path(self.state.appcfg.detailed_wifi_log_file).expanduser() if self.state.appcfg.detailed_wifi_log_file else Path("wifilogs")
        return (configured.parent / "wifilogs") if configured.suffix else configured

    def _safe_log_file(self, name: str) -> Optional[Path]:
        if "/" in name or "\\" in name:
            return None
        if not name.endswith(".jsonl"):
            return None
        path = (self._wifi_logs_dir() / name).resolve()
        try:
            path.relative_to(self._wifi_logs_dir().resolve())
        except Exception:
            return None
        return path

    def _safe_generated_map_file(self, name: str) -> Optional[Path]:
        """
        Map HTML is generated from a wifi_capture_*.jsonl and stored under <wifilogs>/out/.
        """
        p = self._safe_log_file(name)
        if p is None:
            return None
        out_dir = (self._wifi_logs_dir() / "out").resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_stem = p.stem.replace(" ", "_")
        out = (out_dir / f"{safe_stem}.map.html").resolve()
        try:
            out.relative_to(out_dir)
        except Exception:
            return None
        return out

    async def wifi_logs_index(self, _request: web.Request) -> web.Response:
        log_dir = self._wifi_logs_dir()
        files = []
        if log_dir.exists():
            for p in sorted(log_dir.glob("*.jsonl"), key=lambda x: x.stat().st_mtime, reverse=True):
                st = p.stat()
                files.append(
                    {
                        "name": p.name,
                        "size": f"{st.st_size} B",
                        "modified": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat(),
                    }
                )
        return web.Response(text=self.logs_template.render(files=files), content_type="text/html")

    async def wifi_logs_view(self, request: web.Request) -> web.Response:
        name = request.match_info.get("name", "")
        p = self._safe_log_file(name)
        if p is None or not p.exists():
            raise web.HTTPNotFound()
        text = p.read_text(encoding="utf-8", errors="replace")
        return web.Response(text=text, content_type="text/plain")

    async def wifi_logs_download(self, request: web.Request) -> web.StreamResponse:
        name = request.match_info.get("name", "")
        p = self._safe_log_file(name)
        if p is None or not p.exists():
            raise web.HTTPNotFound()
        return web.FileResponse(
            path=p,
            headers={"Content-Disposition": f'attachment; filename="{p.name}"'},
        )

    async def wifi_logs_map(self, request: web.Request) -> web.StreamResponse:
        name = request.match_info.get("name", "")
        src = self._safe_log_file(name)
        if src is None or not src.exists():
            raise web.HTTPNotFound()
        out = self._safe_generated_map_file(name)
        if out is None:
            raise web.HTTPNotFound()

        def _parse_int_qs(key: str, default: int) -> int:
            try:
                v = request.query.get(key)
                return int(v) if v is not None else default
            except Exception:
                return default

        def _parse_float_qs(key: str, default: float) -> float:
            try:
                v = request.query.get(key)
                return float(v) if v is not None else default
            except Exception:
                return default

        downsample = max(1, _parse_int_qs("downsample", 1))
        max_jump_m = max(0.0, _parse_float_qs("max_jump_m", 0.0))
        min_db = _parse_float_qs("min_db", -90.0)
        max_db = _parse_float_qs("max_db", -40.0)
        tiles = str(request.query.get("tiles") or "OpenStreetMap")
        force = str(request.query.get("force") or "").lower() in ("1", "true", "yes", "y", "on")

        async def _render_if_needed() -> None:
            if not force and out.exists():
                try:
                    if out.stat().st_mtime >= src.stat().st_mtime:
                        return
                except Exception:
                    pass

            def _do_render():
                try:
                    from wifilog_viewer.viewer import load_wifilog, write_map_html
                except Exception as e:
                    raise RuntimeError(f"wifilog_viewer unavailable: {e}") from e
                samples, events = load_wifilog(src, downsample=downsample, max_jump_m=max_jump_m)
                write_map_html(samples, out, min_db=min_db, max_db=max_db, tiles=tiles, roaming_events=events)

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _do_render)

        try:
            await _render_if_needed()
        except Exception as e:
            return web.Response(
                status=500,
                text=f"Map generation failed: {e}\n\nTip: ensure viewer deps are installed (folium/branca).\n",
                content_type="text/plain",
                headers={"Cache-Control": "no-store"},
            )

        return web.FileResponse(
            path=out,
            headers={"Cache-Control": "no-store"},
        )

    async def websocket_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.websockets.add(ws)
        try:
            await ws.send_json({"type": "state", "data": self.state.snapshot()})
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        body = jsonlib.loads(msg.data)
                    except Exception:
                        body = {}
                    if body.get("type") == "set_detailed_wifi_logging":
                        enabled = bool(body.get("enabled"))
                        if self.poller:
                            self.poller.set_detailed_wifi_logging(enabled)
                        await ws.send_json({"type": "ack", "command": "set_detailed_wifi_logging", "ok": True, "enabled": enabled})
                    if body.get("type") == "set_modbus_enabled":
                        enabled = bool(body.get("enabled"))
                        if self.poller:
                            self.poller.set_modbus_enabled(enabled)
                        await ws.send_json({"type": "ack", "command": "set_modbus_enabled", "ok": True, "enabled": enabled})
                if msg.type == WSMsgType.ERROR:
                    break
        finally:
            self.websockets.discard(ws)
        return ws

    async def websocket_logs_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        last_seq = 0
        try:
            init = self.state.app_log_tail(50)
            if init:
                last_seq = int(init[-1].get("seq") or 0)
            await ws.send_json({"type": "log_init", "data": {"lines": init}})
            loop = asyncio.get_running_loop()
            while not self.stop_event.is_set():
                try:
                    triggered = await loop.run_in_executor(None, self.state.app_log_event.wait, 1.0)
                finally:
                    self.state.app_log_event.clear()
                if not triggered:
                    continue
                lines = self.state.app_logs_since(last_seq)
                if not lines:
                    continue
                last_seq = int(lines[-1].get("seq") or last_seq)
                await ws.send_json({"type": "log", "data": {"lines": lines}})
        finally:
            try:
                await ws.close()
            except Exception:
                pass
        return ws

    def _tail_external_log(self, source: str) -> Tuple[List[str], Optional[str]]:
        spec = EXTERNAL_LOG_SOURCES.get(source)
        if spec is None:
            return [], f"Unknown log source: {source}"
        try:
            res = subprocess.run(
                spec["tail_cmd"],
                capture_output=True,
                text=True,
                timeout=8,
            )
            output = (res.stdout or "").splitlines()
            if res.returncode != 0:
                err = (res.stderr or res.stdout or "").strip() or f"exit {res.returncode}"
                if output:
                    return output, err
                return [], err
            return [ln for ln in output if ln], None
        except Exception as e:
            return [], str(e)

    async def _stream_external_logs(self, ws: web.WebSocketResponse, source: str) -> None:
        loop = asyncio.get_running_loop()
        init_lines, err = await loop.run_in_executor(None, self._tail_external_log, source)
        init_payload = [{"line": ln} for ln in init_lines]
        if err and not init_lines:
            init_payload = [{"line": f"[error] {err}"}]
        elif err:
            init_payload.append({"line": f"[warning] tail command: {err}"})
        await ws.send_json({"type": "log_init", "data": {"lines": init_payload}})

        spec = EXTERNAL_LOG_SOURCES.get(source)
        if spec is None:
            return

        proc: Optional[asyncio.subprocess.Process] = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *spec["follow_cmd"],
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            assert proc.stdout is not None
            while not self.stop_event.is_set():
                line = await proc.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip("\n")
                if text:
                    await ws.send_json({"type": "log", "data": {"lines": [{"line": text}]}})
        except Exception as e:
            try:
                await ws.send_json({"type": "log", "data": {"lines": [{"line": f"[stream error] {e}"}]}})
            except Exception:
                pass
        finally:
            if proc is not None:
                try:
                    proc.terminate()
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

    async def websocket_external_logs_handler(self, request: web.Request) -> web.WebSocketResponse:
        source = str(request.match_info.get("source") or "").strip().lower()
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        try:
            await self._stream_external_logs(ws, source)
        finally:
            try:
                await ws.close()
            except Exception:
                pass
        return ws

    async def broadcast_snapshot(self):
        if not self.websockets:
            return
        payload = {"type": "state", "data": self.state.snapshot()}
        async def _send_one(ws: web.WebSocketResponse) -> Optional[web.WebSocketResponse]:
            try:
                await asyncio.wait_for(ws.send_json(payload), timeout=1.5)
                return None
            except Exception:
                return ws

        tasks = [_send_one(ws) for ws in list(self.websockets)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, web.WebSocketResponse):
                self.websockets.discard(r)

    async def broadcast_loop(self):
        loop = asyncio.get_running_loop()
        while not self.stop_event.is_set():
            deadline = loop.time() + self.broadcast_interval
            timeout = max(0.0, deadline - loop.time())
            try:
                triggered = await loop.run_in_executor(None, self.state.update_event.wait, timeout)
            finally:
                self.state.update_event.clear()
            if triggered:
                await self.broadcast_snapshot()

    async def _run_async(self):
        app = web.Application()
        app.add_routes(
            [
                web.get("/", self.index),
                web.get("/combined", self.combined_index),
                web.get("/logs", self.app_logs_index),
                web.get("/logs/{source}", self.external_logs_index),
                web.get("/ping", self.ping),
                web.get("/ws", self.websocket_handler),
                web.get("/wslogs", self.websocket_logs_handler),
                web.get("/wsexternallogs/{source}", self.websocket_external_logs_handler),
                web.get("/wifilogs", self.wifi_logs_index),
                web.get("/wifilogs/view/{name}", self.wifi_logs_view),
                web.get("/wifilogs/download/{name}", self.wifi_logs_download),
                web.get("/wifilogs/map/{name}", self.wifi_logs_map),
            ]
        )
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, self.host, self.port).start()
        broadcast_task = asyncio.create_task(self.broadcast_loop())
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(0.25)
        finally:
            self.stop_event.set()
            self.state.update_event.set()
            broadcast_task.cancel()
            try:
                await broadcast_task
            except asyncio.CancelledError:
                pass
            await runner.cleanup()

    def start(self):
        self.thread = threading.Thread(target=self._thread_main, daemon=True)
        self.thread.start()

    def _thread_main(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._run_async())
        finally:
            self.loop.close()

    def stop(self):
        self.stop_event.set()
        self.state.update_event.set()
        if self.loop:
            try:
                self.loop.call_soon_threadsafe(lambda: None)
            except Exception:
                pass
        if self.thread:
            self.thread.join(timeout=2)


def pick_vehicle(appcfg: AppCfg, selector: Optional[str]) -> VehicleCfg:
    if not appcfg.vehicles:
        raise SystemExit("No vehicles found in config")
    if selector is None:
        return appcfg.vehicles[0]
    try:
        idx = int(selector)
        if 1 <= idx <= len(appcfg.vehicles):
            return appcfg.vehicles[idx - 1]
    except Exception:
        pass
    for v in appcfg.vehicles:
        if v.name == selector or (v.short_name and v.short_name == selector) or derive_short_vehicle_name(v.name) == selector:
            return v
    raise SystemExit(f"Vehicle '{selector}' not found")


__all__ = [
    "AppState",
    "DataLogger",
    "Poller",
    "WebServer",
    "load_config",
    "pick_vehicle",
    "setup_app_logging",
    "silence_lib_logs",
    "write_default_config",
]


class _StateTailLogHandler(logging.Handler):
    def __init__(self, state: AppState, level: int = logging.NOTSET):
        super().__init__(level=level)
        self.state = state

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.state.append_app_log(line=msg, level=record.levelname, logger=record.name)
        except Exception:
            # Never let logging break the daemon.
            return


def setup_app_logging(state: AppState, *, log_file: Optional[str], level: str = "INFO") -> None:
    """
    App-wide logging:
    - Rotating log file (for persistence)
    - In-memory ring buffer (for /logs live tail)
    """
    lvl = getattr(logging, str(level or "WARNING").upper(), logging.WARNING)
    root = logging.getLogger()
    root.setLevel(lvl)

    fmt = logging.Formatter("%(asctime)sZ %(levelname)s [%(name)s] %(message)s")

    tail_handler = _StateTailLogHandler(state, level=lvl)
    tail_handler.setFormatter(fmt)
    root.addHandler(tail_handler)

    if log_file:
        try:
            path = Path(str(log_file)).expanduser()
            path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.handlers.RotatingFileHandler(
                filename=str(path),
                maxBytes=5 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
            file_handler.setLevel(lvl)
            file_handler.setFormatter(fmt)
            root.addHandler(file_handler)
        except Exception as e:
            state.append_app_log(line=f"Failed to set up file logging: {e}", level="ERROR", logger="logger")
