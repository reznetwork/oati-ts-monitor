"""Application state container."""
from __future__ import annotations

import copy
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.config import AppCfg, VehicleCfg, HISTORY_WINDOW_SEC
from core.tui_utils import infer_style

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
            items = list(self._app_logs)[-lim:]
        if items:
            return items
        log_file = self.appcfg.app_log_file
        if not log_file:
            return []
        try:
            path = Path(str(log_file)).expanduser()
            if not path.exists():
                return []
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
            tail = [ln for ln in lines[-lim:] if ln]
            return [{"seq": 0, "line": ln} for ln in tail]
        except Exception:
            return []

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
                "controllers": copy.deepcopy(self.controllers),
                "gnss": copy.deepcopy(self.gnss) if self.gnss is not None else None,
                "wifi": copy.deepcopy(self.wifi) if self.wifi is not None else None,
                "cpu_load": self.cpu_load,
                "display_latency": copy.deepcopy(self.display_latency),
                "detailed_wifi_logging": copy.deepcopy(self.detailed_wifi_logging),
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
            return copy.deepcopy(self.display_latency)

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
            return copy.deepcopy(self.gnss) if self.gnss is not None else None
