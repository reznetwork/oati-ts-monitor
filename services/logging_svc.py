"""Data logging, full-fidelity logs, and HTTP upload."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import queue
import socket
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.config import VehicleCfg, derive_short_vehicle_name
from core.state import AppState

FULL_LOG_SCHEMA_VERSION = 1
FULL_LOG_COMPRESSED_SUFFIX = ".jsonl.gz"

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
                    fh.write(json.dumps(self._build_entry(self.state.snapshot()), ensure_ascii=False) + "\n")
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
        line = (json.dumps(rec, ensure_ascii=False) + "\n").encode("utf-8")
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
