"""Modbus/GNSS polling loop."""
from __future__ import annotations

import logging
import socket
import threading
import time
from collections import deque
from typing import Any, Dict, List, Optional, Tuple

from core.config import (
    AppCfg,
    ControllerCfg,
    DEFAULT_POLL_INTERVAL_SEC,
    MAX_MODBUS_REF_SPAN,
    VehicleCfg,
    _as_positive_float,
)
from core.state import AppState
from services.io_bridge import IOBridgeEvaluator
from services.modbus import (
    ConnectionGuard,
    MBClient,
    chunk_refs_by_span,
    logical_state,
)
from services.system import check_tcp, get_cpu_load, get_soc_temp_c
from services.wifi import (
    DetailedWifiLogger,
    RoamingEventWatcher,
    apply_bssid_transition_timing,
    get_wifi_status,
    parse_iw_scan_candidates,
    parse_station_dump,
    _run_iw_command,
)

RECONNECT_WINDOW_SEC = 30.0
RECONNECT_MAX_PER_WINDOW = 3

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
        self._logger = logging.getLogger("gnss")

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
            except Exception as e:
                self._logger.debug("GNSS reconnect after error: %s", e)
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
