#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Vehicle Modbus Monitor — Discrete Inputs (ncurses TUI) with external JSON config
Compatible with pymodbus 3.11.x signature style (address positional, others keyword-only).

What changed in v3
- Config moved to external JSON (multiple vehicles supported)
- Per-point styles + colors as requested:
    • MB_IO_1 DI1 «Ручник»:          ON=RED,   OFF=GREEN
    • MB_IO_1 DI2 «Ремень»:           ON=GREEN, OFF=RED   (renamed from «!Ремень», inversion preserved)
    • MB_IO_1 DI3 «Двигатель»:        ON=GREEN, OFF=RED   (renamed from «!Двигатель», inversion preserved)
    • MB_IO_2 DI2/DI3 (turn signals): ON=YELLOW, OFF=WHITE
- Robust Modbus read (prefers read_discrete_inputs; falls back to coils if enabled)
- Diagnostics: --diag prints detailed report

Usage
    python3 monitor.py --config monitor_config.json                 # UI
    python3 monitor.py --config monitor_config.json --vehicle "Tractor A"  # Pick vehicle by name
    python3 monitor.py --diag --config monitor_config.json          # Diagnostics only
    python3 monitor.py --unit 1   --timeout 5                       # Force unit/timeout
    python3 monitor.py --no-coils                                   # Disable coils fallback

JSON config (save as monitor_config.json)
----------------------------------------
# Minimal working example for your current setup (ICPDAS ET-7002/ET-7051 + MB_IO_3):
  {
    "pymodbus": {"port": 502, "timeout": 2.5, "pollIntervalSec": 0.05, "unitCandidates": [1, 255, 0], "coilsFallback": true},
    "gnss": {"host": "192.168.1.50", "port": 2947},
    "wifi": {"interface": "wlp2s0", "refreshSeconds": 2},
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
            {"ref": 10001, "label": "Ручник",    "invert": false, "style": "handbrake"},
            {"ref": 10002, "label": "Ремень",    "invert": true,  "style": "seatbelt"},
            {"ref": 10003, "label": "Двигатель", "invert": true,  "style": "engine"}
          ]
        },
        {
          "name": "MB_IO_2",
          "model": "ICPDAS ET-7051",
          "host": "172.16.102.5",
          "base": 10000,
          "points": [
            {"ref": 10000, "label": "тормоз"},
            {"ref": 10002, "label": "левый поворотник",  "style": "indicator"},
            {"ref": 10003, "label": "правый поворотник", "style": "indicator"},
            {"ref": 10005, "label": "ближний свет"},
            {"ref": 10009, "label": "Гудок"}
          ]
        },
        {
          "name": "MB_IO_3",
          "host": "172.16.102.8",
          "base": 10001,
          "gear_points": {
            "10001": "N", "10002": "F1", "10003": "F2", "10004": "F3", "10005": "F4",
            "10006": "F5", "10007": "F6", "10008": "F7", "10009": "F8", "10010": "F9",
            "10021": "R1", "10022": "R2"
          },
          "extra_points": {"10023": "&&"}
        }
      ]
    }
  ]
}

"""
import asyncio
import json
import time
try:
    import curses
except ModuleNotFoundError:  # pragma: no cover - Windows without curses
    class _DummyCurses:
        error = Exception
        A_BOLD = 0
        COLOR_GREEN = 2
        COLOR_RED = 1
        COLOR_YELLOW = 3
        COLOR_CYAN = 6
        COLOR_MAGENTA = 5
        COLOR_WHITE = 7
        COLS = 120
        LINES = 40

        @staticmethod
        def wrapper(_fn):
            raise RuntimeError("curses is not available on this platform")

        @staticmethod
        def color_pair(_idx):
            return 0

        @staticmethod
        def curs_set(_v):
            return None

        @staticmethod
        def start_color():
            return None

        @staticmethod
        def use_default_colors():
            return None

        @staticmethod
        def init_pair(_a, _b, _c):
            return None

    curses = _DummyCurses()
import socket
import locale
import argparse
import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Any
import threading
import json as jsonlib
from pathlib import Path
from datetime import datetime, timezone
from collections import deque

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
    from pymodbus import __version__ as PYMODBUS_VERSION
except ModuleNotFoundError:  # pragma: no cover
    ModbusTcpClient = None
    PYMODBUS_VERSION = "unavailable"

# UTF-8 for Russian labels
locale.setlocale(locale.LC_ALL, '')

from core.config import (
    DEFAULT_COILS_FALLBACK,
    DEFAULT_MODBUS_PORT,
    DEFAULT_POLL_INTERVAL_SEC,
    DEFAULT_TIMEOUT,
    DEFAULT_UNIT_CANDIDATES,
    AppCfg,
    ControllerCfg,
    PointCfg,
    VehicleCfg,
    load_config,
    pick_vehicle,
    write_default_config,
)
from core.state import AppState
from core.tui_utils import (
    Colors,
    STYLE_COLORS,
    _format_coord,
    _last_numeric,
    _sparkline,
    addstr_clip,
    infer_style,
)
from services.modbus import (
    MAX_MODBUS_REF_SPAN,
    call_bits,
    chunk_refs_by_span,
    logical_state,
    refs_to_block,
)
from services.polling import RECONNECT_MAX_PER_WINDOW, RECONNECT_WINDOW_SEC
from services.system import check_tcp, silence_lib_logs
from services.wifi import get_wifi_status
from services.system import get_cpu_load




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

    @property
    def configured(self) -> bool:
        return self.log_file is not None

    def start(self):
        if not self.log_file:
            return
        try:
            if self.log_file.parent:
                self.log_file.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.last_error = f"mkdir failed: {e}"
            return
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.state.log_event.set()
        if self.thread:
            self.thread.join(timeout=2)

    def status_text(self) -> str:
        if not self.log_file:
            return ""
        interval_txt = f"{self.interval:.1f}s" if self.interval is not None else "on change"
        if self.last_error:
            return f"Log error: {self.last_error}"
        return f"Log: {self.log_file} ({interval_txt})"

    def _state_text(self, val: Optional[bool]) -> Optional[str]:
        if val is True:
            return "ON"
        if val is False:
            return "OFF"
        return None

    def _gear_selection(self, cfg: ControllerCfg, ctrl_snap: Dict[str, Any]) -> Optional[str]:
        gear_vals = ctrl_snap.get("gears", {}) or {}
        for ref, label in cfg.gear_points.items():
            if gear_vals.get(str(ref)):
                return label
        return None

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
                "gear_selection": self._gear_selection(cfg, ctrl_snap),
                "extra": ctrl_snap.get("extra"),
            }
            points_state = ctrl_snap.get("points", {}) or {}
            for p in cfg.points:
                ctrl_entry["points"].append({
                    "controller": cfg.name,
                    "ref": p.ref,
                    "label": p.label,
                    "state": self._state_text(points_state.get(str(p.ref))),
                })
            entry["controllers"].append(ctrl_entry)
        return entry

    def _write_snapshot(self, fh):
        snap = self.state.snapshot()
        entry = self._build_entry(snap)
        fh.write(jsonlib.dumps(entry, ensure_ascii=False) + "\n")
        fh.flush()

    def _wait_for_trigger(self) -> bool:
        try:
            if self.interval is None:
                triggered = self.state.log_event.wait()
            else:
                triggered = self.state.log_event.wait(self.interval)
            return triggered
        finally:
            self.state.log_event.clear()

    def _loop(self):
        if not self.log_file:
            return
        try:
            with self.log_file.open("a", encoding="utf-8") as fh:
                self.active = True
                while not self.stop_event.is_set():
                    try:
                        triggered = self._wait_for_trigger()
                        if self.stop_event.is_set():
                            break
                        if triggered or self.interval is not None:
                            self._write_snapshot(fh)
                    except Exception as e:
                        self.last_error = f"write failed: {e}"
                        self.active = False
                        break
        except Exception as e:
            self.last_error = f"open failed: {e}"
            self.active = False


# ---------------- Modbus client wrapper ----------------
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
        self.func: str = "di"  # 'di' (FC2) or 'coils' (FC1)
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
        if not self.connected:
            return self.connect()
        return True

    def close(self) -> None:
        try:
            self.client.close()
        finally:
            self.connected = False

    def _read_bits(self, start: int, count: int, unit: int, func: str) -> Optional[List[bool]]:
        try:
            if func == "di":
                rr, err = call_bits(self.client.read_discrete_inputs, start, count, unit)
            else:
                rr, err = call_bits(self.client.read_coils, start, count, unit)
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

        # Try DI first
        for uid in self.unit_candidates:
            bits = self._read_bits(start, count, uid, "di")
            if bits is not None:
                self.unit = uid
                self.func = "di"
                return True

        # Optional coils fallback
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

        if self.unit is None:
            ok = self.probe(base, refs[: min(4, len(refs))])
            if not ok:
                return out

        start, count = refs_to_block(refs, base)
        bits = self._read_bits(start, count, self.unit, self.func)  # type: ignore
        if bits is None:
            # reconnect once and retry
            self.client.close()
            self.connected = False
            if not self.connect():
                return out
            bits = self._read_bits(start, count, self.unit, self.func)  # type: ignore
            if bits is None:
                return out

        for ref in refs:
            addr = ref - base
            idx = addr - start
            if 0 <= idx < len(bits):
                out[ref] = bool(bits[idx])
        return out

# ---------------- GNSS client ----------------
def _nmea_to_deg(raw: str, direction: str) -> Optional[float]:
    if not raw or not direction:
        return None
    try:
        val = float(raw)
        deg = int(val // 100)
        minutes = val - deg * 100
        deg = deg + minutes / 60.0
        if direction in ('S', 'W'):
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
            if self.state is None:
                return None
            return dict(self.state)

    def _run(self):
        while not self.stop_event.is_set():
            try:
                with socket.create_connection((self.host, self.port), timeout=5) as sock:
                    sock.settimeout(5)
                    self.last_gsa_fix = None
                    fh = sock.makefile('r', encoding='ascii', errors='ignore', newline='\n')
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
        if '*' in line:
            line = line.split('*', 1)[0]
        parts = line.split(',')
        if not parts or not parts[0].startswith('$'):
            return
        sent = parts[0][1:]
        if sent.endswith('GSA'):
            fix_map = {'1': 'NO FIX', '2': 'FIX 2D', '3': 'FIX 3D'}
            if len(parts) > 2:
                self.last_gsa_fix = fix_map.get(parts[2])
        elif sent.endswith('GGA'):
            if len(parts) < 9:
                return
            try:
                fix_quality = int(parts[6] or 0)
            except Exception:
                return
            try:
                sats = int(parts[7]) if parts[7] else None
            except Exception:
                sats = None
            try:
                hdop = float(parts[8]) if parts[8] else None
            except Exception:
                hdop = None
            if fix_quality <= 0:
                return
            lat = _nmea_to_deg(parts[2], parts[3])
            lon = _nmea_to_deg(parts[4], parts[5])
            fix_map = {0: 'NO FIX', 1: 'FIX', 2: 'DGPS'}
            fix = self.last_gsa_fix or fix_map.get(fix_quality, 'FIX')
            with self.lock:
                self.state = {
                    'fix': fix,
                    'sats': sats,
                    'hdop': hdop,
                    'lat': lat,
                    'lon': lon,
                    'ts': time.time(),
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
        self.last_system_check_time = 0.0
        self.system_refresh_interval = 1.0
        self.gnss_client: Optional[GNSSClient] = None
        self.poll_interval = _as_positive_float(getattr(self.args, "poll", None), DEFAULT_POLL_INTERVAL_SEC)
        self.wifi_refresh_interval = self.appcfg.wifi_refresh if self.appcfg.wifi_refresh is not None else self.poll_interval * 2
        self.poll_net_interval = self.args.poll_net
        self._reconnect_times = deque()  # monotonic timestamps
        self._modbus_enabled = True
        self.state.set_modbus_enabled(True)
        self._build_clients()

        if self.appcfg.gnss_host and self.appcfg.gnss_port:
            self.gnss_client = GNSSClient(self.appcfg.gnss_host, int(self.appcfg.gnss_port))
            self.gnss_client.start()

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
                host=cfg.host,
                port=self.args.port,
                timeout=self.args.timeout,
                unit_candidates=self.args.unit_candidates,
                use_coils_fallback=self.args.coils,
            )

    def request_reconnect(self):
        self.reconnect_event.set()

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
            self._build_clients()

    def _allow_reconnect_now(self) -> bool:
        now = time.monotonic()
        cutoff = now - RECONNECT_WINDOW_SEC
        while self._reconnect_times and self._reconnect_times[0] < cutoff:
            self._reconnect_times.popleft()
        if len(self._reconnect_times) >= RECONNECT_MAX_PER_WINDOW:
            return False
        self._reconnect_times.append(now)
        return True

    def refresh_tcp_status(self):
        for cfg in self.vehicle.controllers:
            ok, latency_ms, why = check_tcp(cfg.host, self.args.port, self.args.timeout)
            self.latency[cfg.name] = latency_ms
            if not ok:
                self.status[cfg.name] = f"TCP {why or 'fail'}"
                self.debug_msgs[cfg.name] = f"TCP check: {why}"
            else:
                self.status[cfg.name] = "OK"
                self.debug_msgs[cfg.name] = f"TCP ok ({latency_ms:.1f} ms)" if latency_ms is not None else "TCP ok"
            self.state.set_controller(cfg.name, latency=latency_ms, status=self.status[cfg.name], debug=self.debug_msgs.get(cfg.name, ""))

        display_latency: Dict[str, Dict[str, Any]] = {}
        for name, host, port in self.appcfg.display_latency_hosts:
            ok, latency_ms, why = check_tcp(host, port, self.args.timeout)
            display_latency[name] = {
                "latency": f"{latency_ms:.1f} ms" if latency_ms is not None else "-- ms",
                "latency_ms": latency_ms,
                "status": "OK" if ok else f"TCP {why or 'fail'}",
                "host": host,
                "port": str(port),
            }
        self.state.set_display_latency(display_latency)

    def poll_controller(self, cfg: ControllerCfg):
        if not self._modbus_enabled:
            self.state.set_controller(cfg.name, status="DISABLED", debug="Modbus disabled via UI")
            return
        client = self.clients[cfg.name]
        points = cfg.points
        point_refs = [p.ref for p in points]
        gear_refs = sorted(cfg.gear_points.keys())
        extra_refs = sorted(cfg.extra_points.keys())

        needed_refs = point_refs + gear_refs + extra_refs
        point_ref_set = set(point_refs)
        ref_values: Dict[int, Optional[bool]] = {}
        last_error_points: Optional[str] = None

        for chunk in chunk_refs_by_span(needed_refs, MAX_MODBUS_REF_SPAN):
            vals = client.read_refs(cfg.base, chunk)
            ref_values.update(vals)
            if point_ref_set.intersection(chunk):
                last_error_points = client.last_error

        status = self.status.get(cfg.name, "OK")
        debug = self.debug_msgs.get(cfg.name, "")

        if points:
            if all(ref_values.get(r) is None for r in point_refs):
                status = status if status.startswith("TCP") else "READ ERR"
                debug = last_error_points or client.last_error or debug
            elif not status.startswith("TCP"):
                status = "OK"
        elif not status.startswith("TCP"):
            status = "OK"

        point_payload = {str(p.ref): logical_state(p.invert, ref_values.get(p.ref)) for p in points}
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

    def refresh_gnss(self):
        if not self.gnss_client:
            return
        self.state.set_gnss(self.gnss_client.snapshot())

    def refresh_wifi(self):
        if not self.appcfg.wifi_iface:
            return
        self.state.set_wifi(get_wifi_status(self.appcfg.wifi_iface))

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
                # Rate-limited: keep existing connections and surface a debug hint.
                for cfg in self.vehicle.controllers:
                    self.state.set_controller(
                        cfg.name,
                        debug=f"Reconnect rate-limited ({RECONNECT_MAX_PER_WINDOW} per {int(RECONNECT_WINDOW_SEC)}s)",
                    )
            self.reconnect_event.clear()
        if now - self.last_net_check_time >= self.poll_net_interval:
            self.refresh_tcp_status()
            self.last_net_check_time = now

        for cfg in self.vehicle.controllers:
            self.poll_controller(cfg)

        if now - self.last_wifi_check_time >= self.wifi_refresh_interval:
            self.refresh_wifi()
            self.last_wifi_check_time = now

        self.refresh_gnss()
        if now - self.last_system_check_time >= self.system_refresh_interval:
            self.state.set_cpu_load(get_cpu_load())
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
        if self.gnss_client:
            self.gnss_client.stop()
        if getattr(self, "thread", None):
            self.thread.join(timeout=2)

# ---------------- UI helpers ----------------

# ---------------- TUI ----------------
class TUI:
    def __init__(self, stdscr, args, appcfg: AppCfg, vehicle: VehicleCfg, state: AppState, poller: "Poller", datalogger: Optional["DataLogger"] = None):
        self.stdscr = stdscr
        self.args = args
        self.appcfg = appcfg
        self.vehicle = vehicle
        self.state = state
        self.poller = poller
        self.datalogger = datalogger
        self.show_debug = False
        self.show_charts = False

        curses.curs_set(0)
        self.stdscr.nodelay(True)
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(Colors.GREEN,  curses.COLOR_GREEN,  -1)
        curses.init_pair(Colors.RED,    curses.COLOR_RED,    -1)
        curses.init_pair(Colors.YELLOW, curses.COLOR_YELLOW, -1)
        curses.init_pair(Colors.CYAN,   curses.COLOR_CYAN,   -1)
        curses.init_pair(Colors.MAGENTA,curses.COLOR_MAGENTA,-1)
        curses.init_pair(Colors.WHITE,  curses.COLOR_WHITE,  -1)

    def draw_header(self, row: int, snap: dict) -> int:
        ts = snap.get("last_update") or 0.0
        age = time.time() - ts if ts else 0.0
        title = f"Vehicle Modbus Monitor — {self.vehicle.name} (q: quit, r: reconnect, d: debug, c: charts)"
        addstr_clip(self.stdscr, row, 0, title, curses.color_pair(Colors.YELLOW) | curses.A_BOLD)
        addstr_clip(self.stdscr, row, max(len(title) + 2, 50), f"Age {age:.1f}s", curses.color_pair(Colors.CYAN))
        return row + 1

    def draw_status_line(self, row: int, snap: dict) -> int:
        x = 0
        ctrls = snap.get("controllers", {})
        for cfg in self.vehicle.controllers:
            st_data = ctrls.get(cfg.name, {})
            st = st_data.get("status", "—")
            col = curses.color_pair(Colors.GREEN) if st == 'OK' else curses.color_pair(Colors.RED)
            latency_ms = st_data.get("latency")
            latency_txt = f"{latency_ms:.1f} ms" if latency_ms is not None else "-- ms"
            chunk = f"{cfg.name}:{st} {cfg.host} {latency_txt}  "
            addstr_clip(self.stdscr, row, x, chunk, col | curses.A_BOLD)
            x += len(chunk)
        if self.datalogger and self.datalogger.configured:
            col = curses.color_pair(Colors.GREEN)
            if self.datalogger.last_error:
                col = curses.color_pair(Colors.RED) | curses.A_BOLD
            addstr_clip(self.stdscr, row, x, self.datalogger.status_text(), col)
        return row + 1

    def draw_debug(self, row: int, snap: dict) -> int:
        if not self.show_debug:
            return row
        addstr_clip(self.stdscr, row, 0, "Debug:", curses.color_pair(Colors.WHITE) | curses.A_BOLD)
        row += 1
        ctrls = snap.get("controllers", {})
        for cfg in self.vehicle.controllers:
            msg = ctrls.get(cfg.name, {}).get("debug", "")
            addstr_clip(self.stdscr, row, 2, f"{cfg.name}: {msg}", curses.color_pair(Colors.WHITE))
            row += 1
        if self.appcfg.display_latency_hosts:
            addstr_clip(self.stdscr, row, 2, f"Display latency: {snap.get('display_latency', {})}", curses.color_pair(Colors.WHITE))
            row += 1
        if self.appcfg.wifi_iface:
            wi_msg = snap.get("wifi") or {"status": "n/a"}
            addstr_clip(self.stdscr, row, 2, f"WiFi: {wi_msg}", curses.color_pair(Colors.WHITE))
            row += 1
        return row

    def render_controller_points(self, row: int, cfg: ControllerCfg, snap: dict) -> int:
        heading = cfg.name if not cfg.model else f"{cfg.name} ({cfg.model})"
        addstr_clip(self.stdscr, row, 0, heading + ":", curses.color_pair(Colors.YELLOW) | curses.A_BOLD); row += 1
        ctrl_state = snap.get("controllers", {}).get(cfg.name, {})
        point_values = ctrl_state.get("points", {})

        if cfg.points:
            for p in cfg.points:
                logical = point_values.get(str(p.ref)) if isinstance(point_values, dict) else point_values.get(p.ref)
                if isinstance(point_values, dict) and str(p.ref) in point_values:
                    logical = point_values[str(p.ref)]
                di_num = p.ref - cfg.base
                style_name = infer_style(p.label, p.style)
                on_col, off_col = STYLE_COLORS.get(style_name, STYLE_COLORS['default'])
                if logical is None:
                    status_txt = "N/A"
                    attr = curses.color_pair(Colors.RED)
                else:
                    status_txt = "ON " if logical else "OFF"
                    attr = curses.color_pair(on_col) | curses.A_BOLD if logical else curses.color_pair(off_col)
                disp = f"  DI{di_num:>2}: {p.label} : {status_txt}"
                addstr_clip(self.stdscr, row, 0, disp, attr)
                row += 1

        if cfg.gear_points:
            gear_values = ctrl_state.get("gears", {})
            tokens = []
            for ref in sorted(cfg.gear_points.keys()):
                label = cfg.gear_points[ref]
                val = gear_values.get(str(ref)) if isinstance(gear_values, dict) else gear_values.get(ref)
                di_num = ref - cfg.base
                token = f"DI{di_num}:{label}"
                if val:
                    tokens.append((" (" + token + ")", curses.color_pair(Colors.GREEN) | curses.A_BOLD))
                else:
                    tokens.append((" " + token, curses.color_pair(Colors.CYAN)))

            addstr_clip(self.stdscr, row, 2, "Gears:", curses.A_BOLD)
            x = len("  Gears:")
            y = row
            for text, style in tokens:
                if x + len(text) >= curses.COLS - 2:
                    y += 1
                    x = 2
                addstr_clip(self.stdscr, y, x, text, style)
                x += len(text)
            row = y + 1

        if cfg.extra_points:
            extra_values = ctrl_state.get("extra", {})
            for ref in sorted(cfg.extra_points.keys()):
                label = cfg.extra_points[ref]
                val = extra_values.get(str(ref)) if isinstance(extra_values, dict) else extra_values.get(ref)
                di_num = ref - cfg.base
                if val is None:
                    status_txt = "N/A"
                    attr = curses.color_pair(Colors.RED)
                else:
                    status_txt = "ON " if val else "OFF"
                    attr = curses.color_pair(Colors.GREEN) | curses.A_BOLD if val else 0
                addstr_clip(self.stdscr, row, 2, f"DI{di_num:>2}:{label} : {status_txt}", attr)
            row += 1

        return row

    def render_display_latency(self, row: int, snap: dict) -> int:
        if not self.appcfg.display_latency_hosts:
            return row
        addstr_clip(self.stdscr, row, 0, "Latency checks:", curses.color_pair(Colors.YELLOW) | curses.A_BOLD)
        row += 1
        dl = snap.get("display_latency", {})
        for name, host, port in self.appcfg.display_latency_hosts:
            info = dl.get(name, {})
            status = info.get("status", "no data")
            latency_txt = info.get("latency", "-- ms")
            target = f"{host}:{port}"
            col = curses.color_pair(Colors.GREEN) if status == "OK" else curses.color_pair(Colors.RED)
            text = f"  {name} ({target}): {status} {latency_txt}"
            addstr_clip(self.stdscr, row, 0, text, col)
            row += 1
        return row

    def render_gnss(self, row: int, snap: dict) -> int:
        gnss = snap.get("gnss")
        if not gnss:
            addstr_clip(self.stdscr, row, 0, "GNSS: no data", curses.color_pair(Colors.RED))
            return row + 1
        age = time.time() - gnss.get('ts', time.time())
        sats_txt = str(gnss.get('sats')) if gnss.get('sats') is not None else "--"
        hdop_val = gnss.get('hdop')
        hdop_txt = f"{hdop_val:.1f}" if isinstance(hdop_val, (int, float)) else "--"
        fix_txt = gnss.get('fix') or "NO FIX"
        addstr_clip(
            self.stdscr,
            row,
            0,
            f"GNSS: {fix_txt}, Sats: {sats_txt}, HDOP {hdop_txt}, Age {age:.1f} s",
            curses.color_pair(Colors.YELLOW),
        )
        lat_txt = _format_coord(gnss.get('lat'), 'N', 'S') or "--"
        lon_txt = _format_coord(gnss.get('lon'), 'E', 'W') or "--"
        addstr_clip(self.stdscr, row + 1, 0, f"Pos: {lat_txt}, {lon_txt}", curses.color_pair(Colors.CYAN))
        return row + 2

    def render_wifi(self, row: int, snap: dict) -> int:
        if not self.appcfg.wifi_iface:
            return row
        status = snap.get("wifi") or {"status": "no data"}
        iface = self.appcfg.wifi_iface
        st = status.get("status")
        if st == "connected":
            sig = status.get("signal") or "-- dBm"
            ssid = status.get("ssid") or "(ssid ?)"
            bssid = status.get("bssid") or "--"
            freq = status.get("freq")
            freq_txt = f", {freq}" if freq else ""
            text = f"WiFi {iface}: connected, {sig}, SSID {ssid}, BSSID {bssid}{freq_txt}"
            addstr_clip(self.stdscr, row, 0, text, curses.color_pair(Colors.GREEN))
        elif st == "no link":
            addstr_clip(self.stdscr, row, 0, f"WiFi {iface}: no link", curses.color_pair(Colors.RED))
        elif st == "error":
            err = status.get("error") or "unknown"
            addstr_clip(self.stdscr, row, 0, f"WiFi {iface}: error {err}", curses.color_pair(Colors.RED))
        else:
            addstr_clip(self.stdscr, row, 0, f"WiFi {iface}: {status}", curses.color_pair(Colors.RED))
        return row + 1

    def draw_section_divider(self, row: int, title: str) -> int:
        label = f"[ {title} ]"
        filler = "─" * max(0, curses.COLS - len(label) - 1)
        addstr_clip(self.stdscr, row, 0, f"{label} {filler}", curses.color_pair(Colors.WHITE) | curses.A_BOLD)
        return row + 1

    def _draw_chart_line(self, row: int, label: str, series: List[Dict[str, Any]], unit: str) -> int:
        last_val = _last_numeric(series)
        last_txt = f"{last_val:.1f}{unit}" if isinstance(last_val, (int, float)) else "--"
        spark_width = max(10, curses.COLS - len(label) - len(last_txt) - 5)
        spark = _sparkline(series, spark_width)
        text = f"{label}: {spark} {last_txt}"
        addstr_clip(self.stdscr, row, 0, text, curses.color_pair(Colors.CYAN))
        return row + 1

    def render_charts(self, row: int, snap: dict) -> int:
        history = snap.get("history") or {}
        has_any = False

        modbus = history.get("modbus_latency", {}) or {}
        if modbus:
            has_any = True
            row = self.draw_section_divider(row, "Modbus latency (ms)")
            for name, series in modbus.items():
                row = self._draw_chart_line(row, name, series, "ms")

        gateway = history.get("gateway_latency", {}) or {}
        if gateway:
            has_any = True
            row = self.draw_section_divider(row, "Gateway latency (ms)")
            for name, series in gateway.items():
                row = self._draw_chart_line(row, name, series, "ms")

        wifi = history.get("wifi", {}) or {}
        wifi_signal = wifi.get("signal_dbm", []) or []
        wifi_tx = wifi.get("tx_rate_mbps", []) or []
        wifi_rx = wifi.get("rx_rate_mbps", []) or []
        if wifi_signal or wifi_tx or wifi_rx:
            has_any = True
            row = self.draw_section_divider(row, "WiFi")
            if wifi_signal:
                row = self._draw_chart_line(row, "Signal", wifi_signal, " dBm")
            if wifi_tx:
                row = self._draw_chart_line(row, "TX rate", wifi_tx, " Mbps")
            if wifi_rx:
                row = self._draw_chart_line(row, "RX rate", wifi_rx, " Mbps")

        cpu = history.get("cpu_load", []) or []
        if cpu:
            has_any = True
            row = self.draw_section_divider(row, "CPU load")
            row = self._draw_chart_line(row, "CPU", cpu, "")

        if not has_any:
            addstr_clip(self.stdscr, row, 0, "No history samples yet.", curses.color_pair(Colors.RED))
            row += 1

        return row

    def loop(self):
        try:
            while True:
                ch = self.stdscr.getch()
                if ch == ord('q'):
                    break
                if ch == ord('d'):
                    self.show_debug = not self.show_debug
                if ch == ord('r'):
                    self.poller.request_reconnect()
                if ch == ord('c'):
                    self.show_charts = not self.show_charts

                snap = self.state.snapshot()
                self.stdscr.erase()
                row = 0
                row = self.draw_header(row, snap)
                row = self.draw_status_line(row, snap)
                row = self.draw_debug(row, snap)
                if self.show_charts:
                    row = self.draw_section_divider(row, "Charts (last 10 min)")
                    row = self.render_charts(row, snap)
                else:
                    row = self.draw_section_divider(row, "Modbus data")
                    addstr_clip(self.stdscr, row, 0, f"Polling {int(self.args.poll*1000)} ms | Unit IDs {self.args.unit_candidates} | Coils fallback: {self.args.coils} | pymodbus {PYMODBUS_VERSION}", curses.color_pair(Colors.MAGENTA))
                    row += 1
                    for cfg in self.vehicle.controllers:
                        row = self.render_controller_points(row, cfg, snap)
                    row = self.render_display_latency(row, snap)
                    row = self.draw_section_divider(row, "GNSS data")
                    row = self.render_gnss(row, snap)
                    row = self.draw_section_divider(row, "WiFi data")
                    row = self.render_wifi(row, snap)
                self.stdscr.refresh()

                time.sleep(self.args.poll)
        finally:
            pass


TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"


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
        loader = jinja2.FileSystemLoader(str(TEMPLATE_DIR))
        self.template_env = jinja2.Environment(loader=loader, autoescape=True)
        self.template = self.template_env.get_template("dashboard.html")

    async def index(self, request: web.Request) -> web.Response:
        initial_json = jsonlib.dumps(self.state.snapshot())
        html = self.template.render(initial_state=initial_json)
        return web.Response(text=html, content_type="text/html")

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

    async def broadcast_snapshot(self):
        if not self.websockets:
            return
        payload = {"type": "state", "data": self.state.snapshot()}
        dead: List[web.WebSocketResponse] = []
        for ws in list(self.websockets):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.websockets.discard(ws)

    async def broadcast_loop(self):
        loop = asyncio.get_running_loop()
        while not self.stop_event.is_set():
            try:
                triggered = await loop.run_in_executor(None, self.state.update_event.wait, self.broadcast_interval)
            finally:
                self.state.update_event.clear()
            if triggered or not self.websockets:
                await self.broadcast_snapshot()

    async def _run_async(self):
        app = web.Application()
        app.add_routes([
            web.get('/', self.index),
            web.get('/ws', self.websocket_handler),
        ])
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
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

    def _thread_main(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._run_async())
        finally:
            self.loop.close()

    def start(self):
        self.thread = threading.Thread(target=self._thread_main, daemon=True)
        self.thread.start()

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
# ---------------- Diagnostics ----------------
def run_diag(args, appcfg: AppCfg, vehicle: VehicleCfg):
    print(f"pymodbus version: {PYMODBUS_VERSION}")
    print("=== Modbus TCP diagnostics ===")

    for cfg in vehicle.controllers:
        print(f"\n{cfg.name} @ {cfg.host}:{args.port}")
        ok, latency_ms, why = check_tcp(cfg.host, args.port, args.timeout)
        if ok:
            lat_txt = f"{latency_ms:.1f} ms" if latency_ms is not None else "-- ms"
            print(f" TCP: reachable ✓ ({lat_txt})")
        else:
            print(f" TCP: unreachable ✗  ({why})")
            continue

        mc = MBClient(cfg.host, args.port, args.timeout, args.unit_candidates, args.coils)
        if not mc.connect():
            print(f" connect(): failed ({mc.last_error})")
            continue
        print(" connect(): OK")

        # refs to probe
        refs = [p.ref for p in cfg.points] or list(cfg.gear_points.keys()) or list(cfg.extra_points.keys())
        refs = sorted(refs)[:4]
        if not refs:
            print(" no refs to read; skipping")
            continue

        if mc.probe(cfg.base, refs):
            print(f" probe: OK (unit {mc.unit}, func {mc.func})")
            vals = mc.read_refs(cfg.base, refs)
            print(" sample read:", {r: vals[r] for r in refs})
        else:
            print(f" probe: failed ({mc.last_error})")

# ---------------- Main ----------------
def parse_args():
    ap = argparse.ArgumentParser(description="Vehicle Modbus Monitor (ncurses) with external JSON config")
    ap.add_argument("--config", type=str, default="monitor_config.json", help="Path to JSON config (default monitor_config.json)")
    ap.add_argument("--vehicle", type=str, default=None, help="Vehicle name (or index starting from 1)")
    ap.add_argument("--port", type=int, default=DEFAULT_MODBUS_PORT, help="Override Modbus TCP port")
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="TCP timeout seconds")
    ap.add_argument("--unit", type=int, help="Force a single unit id (overrides candidates)")
    ap.add_argument("--unit-candidates", type=int, nargs="*", default=DEFAULT_UNIT_CANDIDATES, help="Unit ids to try (default 1 255 0)")
    ap.add_argument("--no-coils", dest="coils", action="store_false", help="Disable fallback to read_coils if DI fails")
    ap.add_argument("--poll", type=float, default=None, help="Polling interval seconds (default from config)")
    ap.add_argument("--poll-net", type=float, default=None, help="Network/latency polling interval seconds (default --poll)")
    ap.add_argument("--diag", action="store_true", help="Run diagnostics and exit")
    ap.add_argument("--verbose", action="store_true", help="Enable verbose pymodbus logging (stdout/stderr)")
    ap.add_argument("--http", action="store_true", help="Enable HTTP/WebSocket server")
    ap.add_argument("--http-port", type=int, default=8080, help="Port for HTTP/WebSocket server (default 8080)")
    ap.add_argument("--no-tui", action="store_true", help="Run headless without curses UI")
    ap.add_argument("--log-file", type=str, default=None, help="Path to JSONL log file (disabled if not set)")
    ap.add_argument("--log-interval", type=float, default=None, help="Seconds between log writes (default: on every update)")
    args = ap.parse_args()

    if args.log_interval is not None and args.log_interval <= 0:
        args.log_interval = None

    return args




def main():
    # Compatibility launcher for split architecture.
    if len(sys.argv) >= 2 and sys.argv[1] in {"daemon", "client"}:
        mode = sys.argv[1]
        forwarded = [sys.argv[0]] + sys.argv[2:]
        if mode == "daemon":
            from daemon_runtime import build_daemon_parser, run_daemon
            parser = build_daemon_parser()
            args = parser.parse_args(forwarded[1:])
            return run_daemon(args)
        from client_runtime import build_client_parser, run_client
        parser = build_client_parser()
        args = parser.parse_args(forwarded[1:])
        return run_client(args)

    args = parse_args()
    # Silence noisy library logs unless verbose is requested
    silence_lib_logs(args.verbose)
    # Load JSON (auto-create happens inside)
    # Auto-create a default config if missing
    if not os.path.exists(args.config):
        write_default_config(args.config)
        print(f"Created default config at {args.config}")
    appcfg = load_config(args.config)

    if args.poll is None:
        args.poll = appcfg.poll_interval_sec
    if args.poll <= 0:
        raise SystemExit("--poll must be > 0")
    if args.poll_net is None:
        args.poll_net = args.poll
    if args.poll_net <= 0:
        raise SystemExit("--poll-net must be > 0")

    # Apply global overrides from CLI if provided
    if args.unit is not None:
        args.unit_candidates = [args.unit]
    # Allow config defaults when CLI left to defaults
    if args.port == DEFAULT_MODBUS_PORT:
        args.port = appcfg.port
    if args.timeout == DEFAULT_TIMEOUT:
        args.timeout = appcfg.timeout
    if args.unit_candidates == DEFAULT_UNIT_CANDIDATES:
        args.unit_candidates = appcfg.unit_candidates
    if args.coils is True and appcfg.coils_fallback is not None:
        args.coils = appcfg.coils_fallback

    vehicle = pick_vehicle(appcfg, args.vehicle)

    if args.diag:
        run_diag(args, appcfg, vehicle)
        return

    state = AppState(vehicle, appcfg)
    poller = Poller(args, appcfg, vehicle, state)
    poller.start()
    datalogger = DataLogger(state, vehicle, args.log_file, args.log_interval)
    datalogger.start()
    web_server: Optional[WebServer] = None
    if args.http:
        web_server = WebServer(state, '0.0.0.0', args.http_port, broadcast_interval=args.poll, poller=poller)
        web_server.start()

    def wrapped(stdscr):
        ui = TUI(stdscr, args, appcfg, vehicle, state, poller, datalogger)
        ui.loop()

    try:
        if args.no_tui:
            while True:
                time.sleep(1)
        else:
            curses.wrapper(wrapped)
    except KeyboardInterrupt:
        pass
    finally:
        poller.stop()
        if web_server:
            web_server.stop()
        if datalogger:
            datalogger.stop()


if __name__ == "__main__":
    main()
