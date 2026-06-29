"""WiFi status, parsing, and detailed logging."""
from __future__ import annotations

import json
import logging
import queue
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from core.state import AppState

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
                    fh.write(json.dumps(item, ensure_ascii=False) + "\n")
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
        self._logger = logging.getLogger("wifi.roaming")

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
            except Exception as e:
                self._logger.debug("iw event watcher error: %s", e)
            finally:
                if proc is not None:
                    try:
                        proc.terminate()
                    except Exception:
                        pass
            if self.stop_event.wait(1.0):
                break
