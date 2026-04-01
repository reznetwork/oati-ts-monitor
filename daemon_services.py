from __future__ import annotations

import asyncio
import json
import json as jsonlib
import logging
import os
import queue
import re
import socket
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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

DEFAULT_MODBUS_PORT = 502
DEFAULT_TIMEOUT = 2.5
DEFAULT_UNIT_CANDIDATES = [1, 255, 0]
DEFAULT_COILS_FALLBACK = True
HISTORY_WINDOW_SEC = 600
TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"


@dataclass
class PointCfg:
    ref: int
    label: str
    invert: bool = False
    style: Optional[str] = None


@dataclass
class ControllerCfg:
    name: str
    host: str
    base: int
    points: List[PointCfg] = field(default_factory=list)
    gear_points: Dict[int, str] = field(default_factory=dict)
    extra_points: Dict[int, str] = field(default_factory=dict)
    model: Optional[str] = None


@dataclass
class VehicleCfg:
    name: str
    controllers: List[ControllerCfg]
    short_name: Optional[str] = None
    gnss_host: Optional[str] = None
    gnss_port: Optional[int] = None
    gnss_admin_host: Optional[str] = None
    gnss_admin_port: Optional[int] = None


def derive_short_vehicle_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:48] if len(s) > 48 else s


@dataclass
class AppCfg:
    port: int = DEFAULT_MODBUS_PORT
    timeout: float = DEFAULT_TIMEOUT
    unit_candidates: List[int] = field(default_factory=lambda: DEFAULT_UNIT_CANDIDATES[:])
    coils_fallback: bool = DEFAULT_COILS_FALLBACK
    gnss_host: Optional[str] = None
    gnss_port: Optional[int] = None
    wifi_iface: Optional[str] = None
    wifi_refresh: Optional[float] = None
    detailed_wifi_log_file: Optional[str] = None
    display_latency_hosts: List[Tuple[str, str, int]] = field(default_factory=list)
    vehicles: List[VehicleCfg] = field(default_factory=list)


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
            return {
                "controllers": jsonlib.loads(jsonlib.dumps(self.controllers)),
                "gnss": jsonlib.loads(jsonlib.dumps(self.gnss)) if self.gnss is not None else None,
                "wifi": jsonlib.loads(jsonlib.dumps(self.wifi)) if self.wifi is not None else None,
                "cpu_load": self.cpu_load,
                "display_latency": jsonlib.loads(jsonlib.dumps(self.display_latency)),
                "detailed_wifi_logging": jsonlib.loads(jsonlib.dumps(self.detailed_wifi_logging)),
                "history": self._history_snapshot(),
                "last_update": self.last_update,
                "vehicle": self.vehicle.name,
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

    def enqueue_wifi_sample(self, wifi: Optional[Dict[str, Any]], gnss: Optional[Dict[str, Any]], ts_ms: int):
        with self.lock:
            enabled = self.enabled
            session_file = str(self.current_session_file) if self.current_session_file else None
        if not enabled or not session_file or not wifi:
            return
        self._put(
            {
                "type": "wifi_sample",
                "ts_ms": ts_ms,
                "_session_file": session_file,
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
            }
        )

    def enqueue_roaming_event(self, event_type: str, details: Dict[str, Any], gnss: Optional[Dict[str, Any]], ts_ms: int):
        with self.lock:
            enabled = self.enabled
            session_file = str(self.current_session_file) if self.current_session_file else None
        if not enabled or not session_file:
            return
        self._put({"type": "roaming_event", "event": event_type, "ts_ms": ts_ms, "_session_file": session_file, "details": details, "gnss": gnss})

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


def load_config(path: str) -> AppCfg:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    modbus_port = int(raw.get("pymodbus", {}).get("port", DEFAULT_MODBUS_PORT))
    timeout = float(raw.get("pymodbus", {}).get("timeout", DEFAULT_TIMEOUT))
    unit_candidates = list(raw.get("pymodbus", {}).get("unitCandidates", DEFAULT_UNIT_CANDIDATES))
    coils_fallback = bool(raw.get("pymodbus", {}).get("coilsFallback", DEFAULT_COILS_FALLBACK))
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
        v_name = str(v.get("name", "vehicle"))
        ctrls: List[ControllerCfg] = []
        for c in v.get("controllers", []):
            points = [PointCfg(ref=int(p["ref"]), label=str(p["label"]), invert=bool(p.get("invert", False)), style=p.get("style")) for p in c.get("points", [])]
            ctrls.append(
                ControllerCfg(
                    name=str(c.get("name", c.get("host", "ctrl"))),
                    host=str(c["host"]),
                    base=int(c["base"]),
                    points=points,
                    gear_points=_as_int_keys(c.get("gear_points", {})),
                    extra_points=_as_int_keys(c.get("extra_points", {})),
                    model=c.get("model"),
                )
            )
        vehicles.append(
            VehicleCfg(
                name=v_name,
                controllers=ctrls,
                short_name=str(v_short) if v_short else derive_short_vehicle_name(v_name),
                gnss_host=str(v_gnss.get("host")) if v_gnss.get("host") else None,
                gnss_port=int(v_gnss_port) if v_gnss_port is not None else None,
                gnss_admin_host=str(v_gnss.get("adminHost")) if v_gnss.get("adminHost") else None,
                gnss_admin_port=int(v_admin_port) if v_admin_port is not None else None,
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
    return AppCfg(
        port=modbus_port,
        timeout=timeout,
        unit_candidates=unit_candidates,
        coils_fallback=coils_fallback,
        gnss_host=str(gnss_cfg.get("host")) if gnss_cfg.get("host") else None,
        gnss_port=int(gnss_port) if gnss_port is not None else None,
        wifi_iface=str(wifi_cfg.get("interface")).strip() if wifi_cfg.get("interface") else None,
        wifi_refresh=wifi_refresh,
        detailed_wifi_log_file=str(wifi_cfg.get("detailedLogFile")).strip() if wifi_cfg.get("detailedLogFile") else None,
        display_latency_hosts=latency_hosts,
        vehicles=vehicles,
    )


def write_default_config(path: str) -> None:
    default = {
        "pymodbus": {"port": DEFAULT_MODBUS_PORT, "timeout": DEFAULT_TIMEOUT, "unitCandidates": DEFAULT_UNIT_CANDIDATES, "coilsFallback": DEFAULT_COILS_FALLBACK},
        "gnss": {"host": "192.168.1.50", "port": 2947},
        "wifi": {"interface": "wlp2s0", "refreshSeconds": 2, "detailedLogFile": "wifilogs"},
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


def logical_state(invert: bool, raw: Optional[bool]) -> Optional[bool]:
    if raw is None:
        return None
    return (not raw) if invert else raw


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
    try:
        return method(address, count=count, slave=unit), None
    except TypeError as e1:
        try:
            return method(address, count=count, unit=unit), None
        except TypeError as e2:
            try:
                return method(address, count=count), None
            except Exception as e3:
                return None, f"kw variants failed: slave({e1}); unit({e2}); no-unit({e3.__class__.__name__}: {e3})"
    except Exception as e:
        return None, f"call error: {e.__class__.__name__}: {e}"


class MBClient:
    def __init__(self, host: str, port: int, timeout: float, unit_candidates: List[int], use_coils_fallback: bool):
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

    def connect(self) -> bool:
        try:
            self.connected = self.client.connect()
            if not self.connected:
                self.last_error = "connect() failed"
            return self.connected
        except Exception as e:
            self.connected = False
            self.last_error = f"connect error: {e.__class__.__name__}: {e}"
            return False

    def ensure_connected(self) -> bool:
        return self.connected or self.connect()

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
    def __init__(self, args, appcfg: AppCfg, vehicle: VehicleCfg, state: AppState):
        self.args = args
        self.appcfg = appcfg
        self.vehicle = vehicle
        self.state = state
        self.stop_event = threading.Event()
        self.reconnect_event = threading.Event()
        self.clients: Dict[str, MBClient] = {}
        self.latency: Dict[str, Optional[float]] = {}
        self.status: Dict[str, str] = {}
        self.debug_msgs: Dict[str, str] = {}
        self.last_net_check_time = 0.0
        self.last_wifi_check_time = 0.0
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
        self.wifi_refresh_interval = self.appcfg.wifi_refresh if self.appcfg.wifi_refresh is not None else self.args.poll * 2
        self.poll_net_interval = self.args.poll_net
        self._build_clients()
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

    def _build_clients(self):
        self.clients = {}
        self.latency = {}
        self.status = {}
        self.debug_msgs = {}
        for cfg in self.vehicle.controllers:
            self.clients[cfg.name] = MBClient(cfg.host, self.args.port, self.args.timeout, self.args.unit_candidates, self.args.coils)

    def request_reconnect(self):
        self.reconnect_event.set()

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

    def poll_controller(self, cfg: ControllerCfg):
        client = self.clients[cfg.name]
        point_refs = [p.ref for p in cfg.points]
        point_values = client.read_refs(cfg.base, point_refs) if point_refs else {}
        gear_refs = sorted(cfg.gear_points.keys())
        gear_values = client.read_refs(cfg.base, gear_refs) if gear_refs else {}
        extra_refs = sorted(cfg.extra_points.keys())
        extra_values = client.read_refs(cfg.base, extra_refs) if extra_refs else {}
        status = self.status.get(cfg.name, "OK")
        debug = self.debug_msgs.get(cfg.name, "")
        if point_refs and all(point_values.get(r) is None for r in point_refs):
            status = status if status.startswith("TCP") else "READ ERR"
            debug = client.last_error or debug
        elif not status.startswith("TCP"):
            status = "OK"
        self.state.set_controller(
            cfg.name,
            points={str(p.ref): logical_state(p.invert, point_values.get(p.ref)) for p in cfg.points},
            gears={str(ref): gear_values.get(ref) for ref in gear_refs},
            extra={str(ref): extra_values.get(ref) for ref in extra_refs},
            status=status,
            debug=debug,
            latency=self.latency.get(cfg.name),
        )

    def run_once(self):
        now = time.monotonic()
        if self.reconnect_event.is_set():
            self._build_clients()
            self.reconnect_event.clear()
        if now - self.last_net_check_time >= self.poll_net_interval:
            self.refresh_tcp_status()
            self.last_net_check_time = now
        for cfg in self.vehicle.controllers:
            self.poll_controller(cfg)
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
            self.detailed_wifi_logger.enqueue_wifi_sample(wifi, self.state.gnss_snapshot(), ts_ms)
            self.last_wifi_check_time = now
        if self.gnss_client:
            self.state.set_gnss(self.gnss_client.snapshot())
        self.state.set_cpu_load(get_cpu_load())
        self.state.set_cpu_temp(get_soc_temp_c())

    def start(self):
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def _loop(self):
        while not self.stop_event.is_set():
            self.run_once()
            if self.stop_event.wait(self.args.poll):
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

    async def index(self, _request: web.Request) -> web.Response:
        return web.Response(text=self.template.render(initial_state=jsonlib.dumps(self.state.snapshot())), content_type="text/html")

    async def combined_index(self, _request: web.Request) -> web.Response:
        return web.Response(text=self.combined_template.render(initial_state=jsonlib.dumps(self.state.snapshot())), content_type="text/html")

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
                if msg.type == WSMsgType.ERROR:
                    break
        finally:
            self.websockets.discard(ws)
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
                web.get("/ws", self.websocket_handler),
                web.get("/wifilogs", self.wifi_logs_index),
                web.get("/wifilogs/view/{name}", self.wifi_logs_view),
                web.get("/wifilogs/download/{name}", self.wifi_logs_download),
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


__all__ = ["AppState", "DataLogger", "Poller", "WebServer", "load_config", "pick_vehicle", "silence_lib_logs", "write_default_config"]
