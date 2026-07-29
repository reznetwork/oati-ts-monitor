"""
Gunzip NDJSON full-fidelity segments and insert into ClickHouse.
"""

from __future__ import annotations

import gzip
import json
import logging
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from .clickhouse_client import ClickHouseStore

logger = logging.getLogger(__name__)

BATCH_SIZE = 2000


def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _extract_wifi_geo(kind: str, payload: Any) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[str]]:
    if not isinstance(payload, dict):
        return None, None, None, None
    # Full-log wifi events store the flat wifi_capture record as payload
    gnss = payload.get("gnss") or {}
    wifi = payload.get("wifi") or {}
    lat = _as_float(gnss.get("lat"))
    lon = _as_float(gnss.get("lon"))
    rssi = _as_float(wifi.get("rssi_dbm"))
    if rssi is None:
        rssi = _as_float(wifi.get("signal_avg_dbm"))
    bssid_raw = wifi.get("bssid")
    bssid = str(bssid_raw) if bssid_raw else None
    if kind in ("wifi_roaming_event", "roaming_event"):
        # roaming payloads also carry gnss
        pass
    return lat, lon, rssi, bssid


def gunzip_best_effort(payload: bytes) -> Tuple[bytes, bool]:
    """
    Decompress gzip bytes.

    Returns (raw_bytes, truncated).
    truncated=True when the stream ended early but some data was recovered
    (typical of incomplete uploads / truncated .jsonl.gz files).
    """
    try:
        return gzip.decompress(payload), False
    except EOFError:
        pass
    except OSError:
        pass

    # Best-effort: read whatever bytes are available before EOF.
    chunks: list[bytes] = []
    try:
        with gzip.GzipFile(fileobj=BytesIO(payload), mode="rb") as gz:
            while True:
                try:
                    block = gz.read(1024 * 1024)
                except (EOFError, OSError):
                    break
                if not block:
                    break
                chunks.append(block)
    except Exception as exc:
        logger.warning("gzip open failed (%s)", exc)
        return b"", True

    return b"".join(chunks), True


def _mark_unreadable(
    store: ClickHouseStore,
    *,
    device_id: str,
    vehicle: str,
    vehicle_short: str,
    segment: str,
) -> Dict[str, Any]:
    """Record a permanently-skipped corrupt segment so backfill won't retry it."""
    store.mark_segment_ingested(
        device_id=device_id,
        vehicle=vehicle,
        vehicle_short=vehicle_short,
        segment=segment,
        min_ts_ms=0,
        max_ts_ms=0,
        row_count=0,
    )
    logger.warning(
        "Marked unreadable segment as ingested (skip future backfill): %s/%s/%s",
        device_id,
        vehicle_short,
        segment,
    )
    return {"status": "corrupt", "snapshots": 0, "events": 0}


def iter_ndjson_records(payload: bytes) -> Iterable[dict]:
    """Yield parsed NDJSON objects from a gzip-compressed or raw NDJSON body."""
    if not payload:
        return
    truncated = False
    if payload.startswith(b"\x1f\x8b"):
        raw, truncated = gunzip_best_effort(payload)
        if truncated:
            if raw:
                logger.warning(
                    "Truncated gzip segment; recovering %d decompressed byte(s)",
                    len(raw),
                )
            else:
                logger.warning("Unreadable gzip segment (no recoverable data)")
                return
    else:
        raw = payload

    # Always decode as UTF-8 text before json.loads.
    # Passing bytes to json.loads() uses encoding detection and can raise
    # UnicodeDecodeError (e.g. mis-detecting utf-32-be on binary-ish lines).
    text = raw.decode("utf-8", errors="replace")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(rec, dict):
            yield rec


def parse_segment_records(
    records: Iterable[dict],
    *,
    device_id: str,
    vehicle: str,
    vehicle_short: str,
    segment: str,
) -> Tuple[List[list], List[list], Optional[int], Optional[int]]:
    """
    Convert full-log records into ClickHouse row batches.

    Returns (snapshot_rows, event_rows, min_ts_ms, max_ts_ms).
    """
    snapshots: List[list] = []
    events: List[list] = []
    min_ts: Optional[int] = None
    max_ts: Optional[int] = None

    for rec in records:
        try:
            ts_ms = int(rec.get("ts_ms"))
        except (TypeError, ValueError):
            continue
        rtype = rec.get("type")
        src = rec.get("source") or {}
        row_device = str(src.get("device_id") or device_id)
        row_vehicle = str(src.get("vehicle") or vehicle)
        row_vs = str(src.get("vehicle_short") or vehicle_short)
        min_ts = ts_ms if min_ts is None else min(min_ts, ts_ms)
        max_ts = ts_ms if max_ts is None else max(max_ts, ts_ms)

        if rtype == "snapshot":
            data = rec.get("data")
            snapshots.append(
                [
                    row_device,
                    row_vehicle,
                    row_vs,
                    segment,
                    ts_ms,
                    json.dumps(data, ensure_ascii=False, default=str),
                ]
            )
        elif rtype == "event":
            data = rec.get("data") or {}
            if isinstance(data, dict):
                kind = str(data.get("kind") or "event")
                payload = data.get("payload")
            else:
                kind = "event"
                payload = data
            lat, lon, rssi, bssid = _extract_wifi_geo(kind, payload)
            # Also accept nested envelope where payload is the event itself
            if lat is None and isinstance(payload, dict) and payload.get("type") in (
                "wifi_sample",
                "roaming_event",
            ):
                lat, lon, rssi, bssid = _extract_wifi_geo(str(payload.get("type")), payload)
            events.append(
                [
                    row_device,
                    row_vehicle,
                    row_vs,
                    segment,
                    ts_ms,
                    kind,
                    json.dumps(payload, ensure_ascii=False, default=str),
                    lat,
                    lon,
                    rssi,
                    bssid,
                ]
            )

    return snapshots, events, min_ts, max_ts


def ingest_payload_to_clickhouse(
    store: ClickHouseStore,
    *,
    device_id: str,
    vehicle: str,
    vehicle_short: str,
    segment: str,
    payload: bytes,
    skip_if_ingested: bool = True,
) -> Dict[str, Any]:
    """
    Parse a gzip NDJSON segment and insert into ClickHouse.

    Returns a summary dict. Empty payload is a no-op.
    Truncated gzip is ingested best-effort; totally unreadable files are marked
    so backfill does not retry them forever.
    """
    if not payload:
        return {"status": "empty", "snapshots": 0, "events": 0}

    if skip_if_ingested and store.is_segment_ingested(device_id, vehicle_short, segment):
        logger.info("Skip already-ingested segment %s/%s/%s", device_id, vehicle_short, segment)
        return {"status": "skipped", "snapshots": 0, "events": 0}

    # Detect completely unreadable gzip before parsing lines.
    if payload.startswith(b"\x1f\x8b"):
        raw, truncated = gunzip_best_effort(payload)
        if not raw:
            return _mark_unreadable(
                store,
                device_id=device_id,
                vehicle=vehicle,
                vehicle_short=vehicle_short,
                segment=segment,
            )
        # Re-wrap recovered bytes as uncompressed NDJSON for the shared parser path.
        parse_payload = raw
        if truncated:
            logger.warning(
                "Truncated gzip %s/%s/%s; ingesting recovered data (%d bytes)",
                device_id,
                vehicle_short,
                segment,
                len(raw),
            )
    else:
        parse_payload = payload

    snapshots, events, min_ts, max_ts = parse_segment_records(
        iter_ndjson_records(parse_payload),
        device_id=device_id,
        vehicle=vehicle,
        vehicle_short=vehicle_short,
        segment=segment,
    )

    for i in range(0, len(snapshots), BATCH_SIZE):
        store.insert_snapshots(snapshots[i : i + BATCH_SIZE])
    for i in range(0, len(events), BATCH_SIZE):
        store.insert_events(events[i : i + BATCH_SIZE])

    row_count = len(snapshots) + len(events)
    if min_ts is None or max_ts is None:
        min_ts = 0
        max_ts = 0

    store.mark_segment_ingested(
        device_id=device_id,
        vehicle=vehicle,
        vehicle_short=vehicle_short,
        segment=segment,
        min_ts_ms=min_ts,
        max_ts_ms=max_ts,
        row_count=row_count,
    )
    logger.info(
        "Ingested %s/%s/%s → %d snapshots, %d events",
        device_id,
        vehicle_short,
        segment,
        len(snapshots),
        len(events),
    )
    return {
        "status": "ok" if row_count else "empty",
        "snapshots": len(snapshots),
        "events": len(events),
        "min_ts_ms": min_ts,
        "max_ts_ms": max_ts,
    }


def ingest_file_path(
    store: ClickHouseStore,
    path: Union[str, Path],
    *,
    device_id: str,
    vehicle: str,
    vehicle_short: str,
    segment: Optional[str] = None,
) -> Dict[str, Any]:
    path = Path(path)
    segment = segment or path.name
    try:
        payload = path.read_bytes()
    except OSError as exc:
        logger.warning("Cannot read segment %s: %s", path, exc)
        return _mark_unreadable(
            store,
            device_id=device_id,
            vehicle=vehicle,
            vehicle_short=vehicle_short,
            segment=segment,
        )
    try:
        return ingest_payload_to_clickhouse(
            store,
            device_id=device_id,
            vehicle=vehicle,
            vehicle_short=vehicle_short,
            segment=segment,
            payload=payload,
        )
    except Exception:
        logger.exception("Unexpected ingest failure for %s; marking as corrupt", path)
        return _mark_unreadable(
            store,
            device_id=device_id,
            vehicle=vehicle,
            vehicle_short=vehicle_short,
            segment=segment,
        )


def backfill_data_dir(store: ClickHouseStore, data_dir: Union[str, Path]) -> Dict[str, int]:
    """Walk received_logs layout and ingest any not-yet-ingested segments."""
    root = Path(data_dir).expanduser().resolve()
    stats = {
        "ok": 0,
        "skipped": 0,
        "empty": 0,
        "corrupt": 0,
        "errors": 0,
        "found": 0,
        "outstanding": 0,
    }
    if not root.is_dir():
        logger.info("Backfill: data dir %s does not exist yet", root)
        return stats

    paths = sorted({*root.rglob("segment_*.jsonl.gz"), *root.rglob("segment_*.jsonl")})
    paths = [p for p in paths if p.is_file()]
    stats["found"] = len(paths)
    logger.info("Backfill: scanning %d segment file(s) under %s", len(paths), root)

    for seg_path in paths:
        parts = seg_path.relative_to(root).parts
        if len(parts) < 3:
            continue
        device_id, vehicle_short, segment = parts[0], parts[1], parts[-1]
        try:
            if store.is_segment_ingested(device_id, vehicle_short, segment):
                stats["skipped"] += 1
                continue
            stats["outstanding"] += 1
            result = ingest_file_path(
                store,
                seg_path,
                device_id=device_id,
                vehicle=vehicle_short,
                vehicle_short=vehicle_short,
                segment=segment,
            )
            status = result.get("status", "ok")
            if status == "ok":
                stats["ok"] += 1
            else:
                stats[status] = stats.get(status, 0) + 1
        except Exception:
            logger.exception("Backfill failed for %s", seg_path)
            try:
                _mark_unreadable(
                    store,
                    device_id=device_id,
                    vehicle=vehicle_short,
                    vehicle_short=vehicle_short,
                    segment=segment,
                )
                stats["corrupt"] += 1
            except Exception:
                stats["errors"] += 1

    logger.info(
        "Backfill complete: found=%d outstanding=%d ingested=%d skipped=%d "
        "empty=%d corrupt=%d errors=%d",
        stats["found"],
        stats["outstanding"],
        stats["ok"],
        stats["skipped"],
        stats["empty"],
        stats["corrupt"],
        stats["errors"],
    )
    return stats
