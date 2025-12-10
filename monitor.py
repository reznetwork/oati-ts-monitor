#!/usr/bin/env python3
# -*- coding: utf-8 -*-
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
  "pymodbus": {"port": 502, "timeout": 2.5, "unitCandidates": [1, 255, 0], "coilsFallback": true},
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
import json
import time
import curses
import socket
import locale
import argparse
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Any
import threading

from pymodbus.client import ModbusTcpClient
from pymodbus import __version__ as PYMODBUS_VERSION

# UTF-8 for Russian labels
locale.setlocale(locale.LC_ALL, '')

# ---------------- Defaults ----------------
DEFAULT_POLL_INTERVAL_SEC = 1
DEFAULT_MODBUS_PORT = 502
DEFAULT_TIMEOUT = 2.5
DEFAULT_UNIT_CANDIDATES = [1, 255, 0]
DEFAULT_COILS_FALLBACK = True

# ---------------- Data models ----------------
@dataclass
class PointCfg:
    ref: int
    label: str
    invert: bool = False
    style: Optional[str] = None  # handbrake | seatbelt | engine | indicator | default

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

@dataclass
class AppCfg:
    port: int = DEFAULT_MODBUS_PORT
    timeout: float = DEFAULT_TIMEOUT
    unit_candidates: List[int] = field(default_factory=lambda: DEFAULT_UNIT_CANDIDATES[:])
    coils_fallback: bool = DEFAULT_COILS_FALLBACK
    gnss_host: Optional[str] = None
    gnss_port: Optional[int] = None
    vehicles: List[VehicleCfg] = field(default_factory=list)

# ---------------- Config loader ----------------
def _as_int_keys(d: Dict[str, Any]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for k, v in d.items():
        try:
            out[int(k)] = str(v)
        except Exception:
            continue
    return out

def load_config(path: str) -> AppCfg:
    with open(path, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    port = int(raw.get('pymodbus', {}).get('port', DEFAULT_MODBUS_PORT))
    timeout = float(raw.get('pymodbus', {}).get('timeout', DEFAULT_TIMEOUT))
    unit_candidates = list(raw.get('pymodbus', {}).get('unitCandidates', DEFAULT_UNIT_CANDIDATES))
    coils_fallback = bool(raw.get('pymodbus', {}).get('coilsFallback', DEFAULT_COILS_FALLBACK))
    gnss_cfg = raw.get('gnss', {}) or {}
    gnss_host = gnss_cfg.get('host')
    gnss_port = gnss_cfg.get('port')
    gnss_port_int = int(gnss_port) if gnss_port is not None else None

    vehicles: List[VehicleCfg] = []
    for v in raw.get('vehicles', []):
        ctrls: List[ControllerCfg] = []
        for c in v.get('controllers', []):
            points: List[PointCfg] = []
            for p in c.get('points', []):
                points.append(PointCfg(ref=int(p['ref']), label=str(p['label']), invert=bool(p.get('invert', False)), style=p.get('style')))
            gear = _as_int_keys(c.get('gear_points', {}))
            extra = _as_int_keys(c.get('extra_points', {}))
            ctrls.append(ControllerCfg(
                name=str(c.get('name', c.get('host', 'ctrl'))),
                host=str(c['host']),
                base=int(c['base']),
                points=points,
                gear_points=gear,
                extra_points=extra,
                model=c.get('model')
            ))
        vehicles.append(VehicleCfg(name=str(v.get('name', 'vehicle')), controllers=ctrls))

    return AppCfg(
        port=port,
        timeout=timeout,
        unit_candidates=unit_candidates,
        coils_fallback=coils_fallback,
        gnss_host=str(gnss_host) if gnss_host else None,
        gnss_port=gnss_port_int,
        vehicles=vehicles,
    )

# --- Auto-bootstrap default config if missing ---
def write_default_config(path: str) -> None:
    default = {
        "pymodbus": {"port": DEFAULT_MODBUS_PORT, "timeout": DEFAULT_TIMEOUT, "unitCandidates": DEFAULT_UNIT_CANDIDATES, "coilsFallback": DEFAULT_COILS_FALLBACK},
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
                            {"ref": 10001, "label": "Ручник",    "invert": False, "style": "handbrake"},
                            {"ref": 10002, "label": "Ремень",    "invert": True,  "style": "seatbelt"},
                            {"ref": 10003, "label": "Двигатель", "invert": True,  "style": "engine"}
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
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(default, f, ensure_ascii=False, indent=2)
# ---------------- Helpers ----------------

def silence_lib_logs(verbose: bool) -> None:
    """Suppress noisy pymodbus logs that can corrupt the ncurses screen.
    By default we silence to CRITICAL; with --verbose we relax to ERROR.
    """
    root_level = logging.WARNING if verbose else logging.ERROR
    logging.basicConfig(level=root_level)
    for name in ("pymodbus", "pymodbus.client", "pymodbus.transaction", "pymodbus.framer", "pymodbus.factory"):
        lg = logging.getLogger(name)
        # prevent propagation to root to avoid stderr spam
        lg.propagate = False
        # clear existing handlers attached by the lib/env and add a NullHandler
        try:
            lg.handlers.clear()
        except Exception:
            lg.handlers = []
        lg.addHandler(logging.NullHandler())
        lg.setLevel(logging.CRITICAL if not verbose else logging.ERROR)

def refs_to_block(refs: List[int], base: int) -> Tuple[int, int]:
    addresses = sorted([ref - base for ref in refs])
    start = addresses[0]
    end = addresses[-1]
    count = end - start + 1
    return start, count

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
        etxt = f"{e.__class__.__name__}: {e}"
        if isinstance(e, TimeoutError):
            return False, None, "timeout"
        if isinstance(e, ConnectionRefusedError):
            return False, None, "refused"
        if isinstance(e, OSError) and "No route" in str(e):
            return False, None, "no route"
        return False, None, etxt

# ---------------- Pymodbus call helper ----------------
def call_bits(method, address: int, count: int, unit: int):
    """Robust call into pymodbus 3.11.x (address positional; others keyword-only)."""
    try:
        rr = method(address, count=count, slave=unit)
        return rr, None
    except TypeError as e1:
        try:
            rr = method(address, count=count, unit=unit)
            return rr, None
        except TypeError as e2:
            try:
                rr = method(address, count=count)
                return rr, None
            except Exception as e3:
                return None, f"kw variants failed: slave({e1}); unit({e2}); no-unit({e3.__class__.__name__}: {e3})"
    except Exception as e:
        return None, f"call error: {e.__class__.__name__}: {e}"

# ---------------- Modbus client wrapper ----------------
class MBClient:
    def __init__(self, host: str, port: int, timeout: float, unit_candidates: List[int], use_coils_fallback: bool):
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


def _format_coord(deg: Optional[float], pos: str, neg: str) -> Optional[str]:
    if deg is None:
        return None
    hemi = pos if deg >= 0 else neg
    return f"{abs(deg):.4f} {hemi}"


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
                    'ts': time.monotonic(),
                }

# ---------------- UI helpers ----------------
def addstr_clip(win, y: int, x: int, text: str, attr: int = 0):
    max_x = curses.COLS - 1
    if y >= curses.LINES:
        return
    if x < 0:
        text = text[-x:]
        x = 0
    if x > max_x:
        return
    if x + len(text) > max_x:
        text = text[:max(0, max_x - x)]
    try:
        win.addstr(y, x, text, attr)
    except curses.error:
        pass

# Color policy
class Colors:
    GREEN = 1
    RED = 2
    YELLOW = 3
    CYAN = 4
    MAGENTA = 5
    WHITE = 6

# Style → (ON color, OFF color)
STYLE_COLORS = {
    'handbrake': (Colors.RED, Colors.GREEN),      # Ручник
    'seatbelt':  (Colors.GREEN, Colors.RED),      # Ремень
    'engine':    (Colors.GREEN, Colors.RED),      # Двигатель
    'indicator': (Colors.YELLOW, Colors.WHITE),   # поворотники
    'default':   (Colors.GREEN, 0),               # generic
}

def infer_style(label: str, explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    l = label.lower()
    if 'ручник' in l:
        return 'handbrake'
    if 'ремень' in l:
        return 'seatbelt'
    if 'двигател' in l:
        return 'engine'
    if 'поворотник' in l:
        return 'indicator'
    return 'default'

# ---------------- TUI ----------------
class TUI:
    def __init__(self, stdscr, args, appcfg: AppCfg, vehicle: VehicleCfg):
        self.stdscr = stdscr
        self.args = args
        self.appcfg = appcfg
        self.vehicle = vehicle
        self.clients: Dict[str, MBClient] = {}
        self.status: Dict[str, str] = {}
        self.debug_msgs: Dict[str, str] = {}
        self.latency: Dict[str, Optional[float]] = {}
        self.show_debug = False
        self.last_poll_time = 0.0
        self.last_net_check_time = 0.0
        self.gnss: Optional[GNSSClient] = None

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

        self.create_clients()
        if self.appcfg.gnss_host and self.appcfg.gnss_port:
            self.gnss = GNSSClient(self.appcfg.gnss_host, int(self.appcfg.gnss_port))
            self.gnss.start()

    def create_clients(self):
        self.clients = {}
        self.status = {}
        self.debug_msgs = {}
        self.latency = {}
        for cfg in self.vehicle.controllers:
            mc = MBClient(
                host=cfg.host,
                port=self.args.port,
                timeout=self.args.timeout,
                unit_candidates=self.args.unit_candidates,
                use_coils_fallback=self.args.coils,
            )
            self.clients[cfg.name] = mc
        self.refresh_tcp_status()
        self.last_net_check_time = time.monotonic()
        self.last_poll_time = 0.0

    def refresh_tcp_status(self):
        for cfg in self.vehicle.controllers:
            ok, latency_ms, why = check_tcp(cfg.host, self.args.port, self.args.timeout)
            self.latency[cfg.name] = latency_ms
            if not ok:
                self.status[cfg.name] = f"TCP {why or 'fail'}"
                self.debug_msgs[cfg.name] = f"TCP check: {why}"
            else:
                if self.status.get(cfg.name, "") in ("", None) or self.status.get(cfg.name, "").startswith("TCP "):
                    self.status[cfg.name] = "OK"
                if latency_ms is not None:
                    self.debug_msgs[cfg.name] = f"TCP ok ({latency_ms:.1f} ms)"
                else:
                    self.debug_msgs[cfg.name] = "TCP ok"

    def draw_header(self, row: int) -> int:
        title = f"Vehicle Modbus Monitor — {self.vehicle.name} (q: quit, r: reconnect, d: debug)"
        addstr_clip(self.stdscr, row, 0, title, curses.color_pair(Colors.YELLOW) | curses.A_BOLD)
        return row + 1

    def draw_status_line(self, row: int) -> int:
        x = 0
        for cfg in self.vehicle.controllers:
            name = cfg.name
            st = self.status.get(name, '—')
            col = curses.color_pair(Colors.GREEN) if st == 'OK' else curses.color_pair(Colors.RED)
            latency_ms = self.latency.get(name)
            latency_txt = f"{latency_ms:.1f} ms" if latency_ms is not None else "-- ms"
            chunk = f"{name}:{st} {cfg.host} {latency_txt}  "
            addstr_clip(self.stdscr, row, x, chunk, col | curses.A_BOLD)
            x += len(chunk)
        return row + 1

    def draw_debug(self, row: int) -> int:
        if not self.show_debug:
            return row
        addstr_clip(self.stdscr, row, 0, "Debug:", curses.color_pair(Colors.WHITE) | curses.A_BOLD)
        row += 1
        for cfg in self.vehicle.controllers:
            msg = self.debug_msgs.get(cfg.name, "")
            addstr_clip(self.stdscr, row, 2, f"{cfg.name}: {msg}", curses.color_pair(Colors.WHITE))
            row += 1
        return row

    def render_controller_points(self, row: int, cfg: ControllerCfg) -> int:
        heading = cfg.name if not cfg.model else f"{cfg.name} ({cfg.model})"
        addstr_clip(self.stdscr, row, 0, heading + ":", curses.color_pair(Colors.YELLOW) | curses.A_BOLD); row += 1
        client = self.clients[cfg.name]

        if cfg.points:
            refs = [p.ref for p in cfg.points]
            values = client.read_refs(cfg.base, refs)
            if all(values.get(r) is None for r in refs):
                self.status[cfg.name] = "READ ERR"
                if client.last_error:
                    self.debug_msgs[cfg.name] = client.last_error
            else:
                self.status[cfg.name] = "OK"

            for p in cfg.points:
                raw = values.get(p.ref)
                logical = logical_state(p.invert, raw)
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

        # MB_IO_3 gears if present
        if cfg.gear_points:
            # Read gears
            gear_refs = sorted(cfg.gear_points.keys())
            values = client.read_refs(cfg.base, gear_refs)
            if any(values.get(r) is None for r in gear_refs):
                self.status[cfg.name] = "READ ERR"
                if client.last_error:
                    self.debug_msgs[cfg.name] = client.last_error
            else:
                self.status[cfg.name] = "OK"

            # Tokens
            tokens: List[Tuple[str, int]] = []
            for ref in gear_refs:
                label = cfg.gear_points[ref]
                val = values.get(ref)
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

        # Extra points if any
        if cfg.extra_points:
            refs = sorted(cfg.extra_points.keys())
            values = client.read_refs(cfg.base, refs)
            for ref in refs:
                label = cfg.extra_points[ref]
                val = values.get(ref)
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

    def render_gnss(self, row: int) -> int:
        if not self.gnss:
            return row
        snap = self.gnss.snapshot()
        if snap is None:
            addstr_clip(self.stdscr, row, 0, "GNSS: no data", curses.color_pair(Colors.CYAN))
            return row + 1
        age = time.monotonic() - snap.get('ts', time.monotonic())
        sats_txt = str(snap.get('sats')) if snap.get('sats') is not None else "--"
        hdop_val = snap.get('hdop')
        hdop_txt = f"{hdop_val:.1f}" if isinstance(hdop_val, (int, float)) else "--"
        fix_txt = snap.get('fix') or "NO FIX"
        addstr_clip(
            self.stdscr,
            row,
            0,
            f"GNSS: {fix_txt}, Sats: {sats_txt}, HDOP {hdop_txt}, Age {age:.1f} s",
            curses.color_pair(Colors.YELLOW),
        )
        lat_txt = _format_coord(snap.get('lat'), 'N', 'S') or "--"
        lon_txt = _format_coord(snap.get('lon'), 'E', 'W') or "--"
        addstr_clip(self.stdscr, row + 1, 0, f"Pos: {lat_txt}, {lon_txt}", curses.color_pair(Colors.CYAN))
        return row + 2

    def loop(self):
        try:
            while True:
                try:
                    ch = self.stdscr.getch()
                    if ch == ord('q'):
                        break
                    elif ch == ord('r'):
                        self.create_clients()
                    elif ch == ord('d'):
                        self.show_debug = not self.show_debug
                except Exception:
                    pass

                now = time.monotonic()
                if now - self.last_net_check_time >= self.args.poll_net:
                    self.refresh_tcp_status()
                    self.last_net_check_time = now

                if now - self.last_poll_time < self.args.poll:
                    time.sleep(0.05)
                    continue
                self.last_poll_time = now

                self.stdscr.erase()
                row = 0
                row = self.draw_header(row)
                row = self.draw_status_line(row)
                row = self.draw_debug(row)
                row += 1

                for cfg in self.vehicle.controllers:
                    row = self.render_controller_points(row, cfg)
                    row += 1

                row = self.render_gnss(row)

                addstr_clip(self.stdscr, row, 0, f"Polling {int(self.args.poll*1000)} ms | Unit IDs {self.args.unit_candidates} | Coils fallback: {self.args.coils} | pymodbus {PYMODBUS_VERSION}", curses.color_pair(Colors.MAGENTA))
                self.stdscr.refresh()
        finally:
            if self.gnss:
                self.gnss.stop()

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
    ap.add_argument("--poll", type=float, default=DEFAULT_POLL_INTERVAL_SEC, help="Polling interval seconds (default 0.35)")
    ap.add_argument("--poll-net", type=float, default=None, help="Network/latency polling interval seconds (default --poll)")
    ap.add_argument("--diag", action="store_true", help="Run diagnostics and exit")
    ap.add_argument("--verbose", action="store_true", help="Enable verbose pymodbus logging (stdout/stderr)")
    args = ap.parse_args()

    if args.poll_net is None:
        args.poll_net = args.poll

    return args


def pick_vehicle(appcfg: AppCfg, selector: Optional[str]) -> VehicleCfg:
    if not appcfg.vehicles:
        raise SystemExit("No vehicles found in config")
    if selector is None:
        return appcfg.vehicles[0]
    # try index
    try:
        idx = int(selector)
        if 1 <= idx <= len(appcfg.vehicles):
            return appcfg.vehicles[idx - 1]
    except Exception:
        pass
    # try name
    for v in appcfg.vehicles:
        if v.name == selector:
            return v
    raise SystemExit(f"Vehicle '{selector}' not found")


def main():
    args = parse_args()
    # Silence noisy library logs unless verbose is requested
    silence_lib_logs(args.verbose)
    # Load JSON (auto-create happens inside)
    # Auto-create a default config if missing
    if not os.path.exists(args.config):
        write_default_config(args.config)
        print(f"Created default config at {args.config}")
    appcfg = load_config(args.config)

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

    def wrapped(stdscr):
        ui = TUI(stdscr, args, appcfg, vehicle)
        ui.loop()

    curses.wrapper(wrapped)


if __name__ == "__main__":
    main()
