"""
oati-ts-monitor Log Collector Server

Receives compressed NDJSON log-file uploads from oati-ts-monitor daemons and exposes
a minimal web dashboard for browsing received data, plus an optional ClickHouse-backed
historical viewer.

Endpoints:
  POST /ingest                            – receive a compressed log file
  GET  /                                  – HTML ops dashboard
  GET  /viewer                            – historical log viewer (dashboard + map + Gantt)
  GET  /api/status                        – JSON overall stats
  GET  /api/devices                       – JSON device list
  GET  /api/segments[?device=<id>]        – JSON segment list
  GET  /api/segments/<device>/<vs>/<seg>  – JSON last 200 lines of a segment
  GET  /api/vehicles                      – vehicles with time coverage (ClickHouse)
  GET  /api/activity                      – multi-vehicle activity bars (ClickHouse)
  GET  /api/slice                         – nearest snapshot at playhead (ClickHouse)
  GET  /api/track                         – wifi track for map (ClickHouse)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from aiohttp import web

from .auth import AuthManager, auth_middleware
from .clickhouse_client import ClickHouseStore, ClickHouseUnavailable
from .clickhouse_ingest import backfill_data_dir, ingest_payload_to_clickhouse
from .metadata_store import MetadataStore
from .storage import ChunkStorage
from .wifi_quality import WiFiQualityService
from http_header_text import decode_http_header_text

logger = logging.getLogger(__name__)

_PACKAGE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _PACKAGE_DIR.parent
_VIEWER_TEMPLATE_CANDIDATES = (
    _REPO_ROOT / "templates" / "log_viewer.html",
    _PACKAGE_DIR / "templates" / "log_viewer.html",
)


# ---------------------------------------------------------------------------
# Ingest handler
# ---------------------------------------------------------------------------

async def handle_ingest(request: web.Request) -> web.Response:
    storage: ChunkStorage = request.app["storage"]

    # Required headers
    try:
        device_id   = decode_http_header_text(request.headers["X-Device-Id"])
        vehicle     = decode_http_header_text(request.headers.get("X-Vehicle", ""))
        vehicle_short = decode_http_header_text(request.headers.get("X-Vehicle-Short", "")) or vehicle
        segment     = request.headers["X-Log-Segment"]
        file_bytes  = int(request.headers["X-File-Bytes"])
        sha256      = request.headers.get("X-File-Sha256", "")
        schema_ver  = int(request.headers.get("X-Schema-Version", "1"))
    except (KeyError, ValueError) as exc:
        raise web.HTTPBadRequest(reason=f"Missing or invalid header: {exc}") from exc

    content_type = request.content_type or ""
    compression = request.headers.get("X-Log-Compression", "").lower()
    if compression != "gzip" and "gzip" not in content_type and request.headers.get("Content-Encoding", "").lower() != "gzip":
        raise web.HTTPBadRequest(reason="compressed gzip log files are required")

    payload = await request.read()

    try:
        await storage.ingest_file(
            device_id=device_id,
            vehicle=vehicle,
            vehicle_short=vehicle_short,
            segment=segment,
            schema_version=schema_ver,
            file_bytes=file_bytes,
            sha256=sha256,
            payload=payload,
        )
    except ValueError as exc:
        logger.warning("Rejected file from %s/%s: %s", device_id, segment, exc)
        raise web.HTTPConflict(reason=str(exc)) from exc

    ch: Optional[ClickHouseStore] = request.app.get("clickhouse")
    if ch is not None and payload:
        try:
            await asyncio.to_thread(
                ingest_payload_to_clickhouse,
                ch,
                device_id=device_id,
                vehicle=vehicle,
                vehicle_short=vehicle_short,
                segment=segment,
                payload=payload,
            )
            quality: Optional[WiFiQualityService] = request.app.get("wifi_quality")
            if quality is not None:
                await asyncio.to_thread(
                    quality.sync_catalog,
                    device_id=device_id,
                    vehicle_short=vehicle_short,
                    segment=segment,
                )
        except Exception:
            logger.exception(
                "ClickHouse ingest failed for %s/%s/%s (file stored on disk)",
                device_id,
                vehicle_short,
                segment,
            )

    logger.info(
        "✓ %s/%s  file  (%d bytes%s)",
        device_id,
        segment,
        len(payload),
        ", skipped empty" if not payload else "",
    )
    return web.Response(status=200, text="ok")


# ---------------------------------------------------------------------------
# JSON API handlers
# ---------------------------------------------------------------------------

async def handle_status(request: web.Request) -> web.Response:
    storage: ChunkStorage = request.app["storage"]
    stats = await storage.stats()
    stats["uptime_sec"] = round(time.monotonic() - request.app["start_mono"], 1)
    stats["server_time"] = datetime.now(timezone.utc).isoformat()
    stats["clickhouse_enabled"] = request.app.get("clickhouse") is not None
    backfill = request.app.get("backfill_status")
    if isinstance(backfill, dict):
        stats["backfill"] = dict(backfill)
    return _json(stats)


async def handle_devices(request: web.Request) -> web.Response:
    storage: ChunkStorage = request.app["storage"]
    devices = await storage.get_devices()
    return _json([
        {
            "device_id": d.device_id,
            "vehicles": d.vehicles,
            "total_bytes": d.total_bytes,
            "total_segments": d.total_segments,
            "total_files": d.total_files,
            "total_chunks": d.total_files,
            "first_seen": _fmt_ts(d.first_seen),
            "last_seen": _fmt_ts(d.last_seen),
        }
        for d in devices
    ])


async def handle_segments(request: web.Request) -> web.Response:
    storage: ChunkStorage = request.app["storage"]
    device_filter = request.rel_url.query.get("device")
    segments = await storage.get_segments(device_id=device_filter)
    return _json([
        {
            **m.to_dict(),
            "first_seen": _fmt_ts(m.first_seen),
            "last_seen": _fmt_ts(m.last_seen),
        }
        for m in segments
    ])


async def handle_segment_tail(request: web.Request) -> web.Response:
    storage: ChunkStorage = request.app["storage"]
    device_id     = request.match_info["device"]
    vehicle_short = request.match_info["vehicle_short"]
    segment       = request.match_info["segment"]
    try:
        max_lines = int(request.rel_url.query.get("n", "200"))
    except ValueError:
        max_lines = 200
    records = await storage.read_segment_tail(device_id, vehicle_short, segment, max_lines)
    return _json({"segment": segment, "records": records, "count": len(records)})


# ---------------------------------------------------------------------------
# ClickHouse query API
# ---------------------------------------------------------------------------

def _require_clickhouse(request: web.Request) -> ClickHouseStore:
    ch: Optional[ClickHouseStore] = request.app.get("clickhouse")
    if ch is None:
        raise web.HTTPServiceUnavailable(reason="ClickHouse is not enabled on this collector")
    return ch


async def handle_api_vehicles(request: web.Request) -> web.Response:
    ch = _require_clickhouse(request)
    try:
        vehicles = await asyncio.to_thread(ch.list_vehicles)
    except ClickHouseUnavailable as exc:
        raise web.HTTPServiceUnavailable(reason=str(exc)) from exc
    return _json({"vehicles": vehicles})


async def handle_api_activity(request: web.Request) -> web.Response:
    ch = _require_clickhouse(request)
    q = request.rel_url.query
    try:
        from_ms = int(q["from_ms"])
        to_ms = int(q["to_ms"])
    except (KeyError, ValueError) as exc:
        raise web.HTTPBadRequest(reason="from_ms and to_ms are required integers") from exc
    default_gap = int(request.app.get("activity_gap_ms") or 120_000)
    try:
        gap_ms = int(q.get("gap_ms", str(default_gap)))
    except ValueError:
        gap_ms = default_gap
    try:
        bars = await asyncio.to_thread(
            ch.activity_intervals, from_ms=from_ms, to_ms=to_ms, gap_ms=gap_ms
        )
    except ClickHouseUnavailable as exc:
        raise web.HTTPServiceUnavailable(reason=str(exc)) from exc
    return _json({"from_ms": from_ms, "to_ms": to_ms, "gap_ms": gap_ms, "bars": bars})


async def handle_api_slice(request: web.Request) -> web.Response:
    ch = _require_clickhouse(request)
    q = request.rel_url.query
    vehicle_short = q.get("vehicle_short") or ""
    if not vehicle_short:
        raise web.HTTPBadRequest(reason="vehicle_short is required")
    try:
        ts_ms = int(q["ts_ms"])
    except (KeyError, ValueError) as exc:
        raise web.HTTPBadRequest(reason="ts_ms is required integer") from exc
    try:
        snap = await asyncio.to_thread(ch.nearest_snapshot, vehicle_short=vehicle_short, ts_ms=ts_ms)
    except ClickHouseUnavailable as exc:
        raise web.HTTPServiceUnavailable(reason=str(exc)) from exc
    if snap is None:
        return _json({"found": False, "vehicle_short": vehicle_short, "ts_ms": ts_ms})
    return _json({"found": True, **snap})


async def handle_api_track(request: web.Request) -> web.Response:
    ch = _require_clickhouse(request)
    q = request.rel_url.query
    vehicle_short = q.get("vehicle_short") or ""
    if not vehicle_short:
        raise web.HTTPBadRequest(reason="vehicle_short is required")
    try:
        from_ms = int(q["from_ms"])
        to_ms = int(q["to_ms"])
    except (KeyError, ValueError) as exc:
        raise web.HTTPBadRequest(reason="from_ms and to_ms are required integers") from exc
    try:
        limit = int(q.get("limit", "50000"))
    except ValueError:
        limit = 50_000
    try:
        track = await asyncio.to_thread(
            ch.wifi_track,
            vehicle_short=vehicle_short,
            from_ms=from_ms,
            to_ms=to_ms,
            limit=limit,
        )
    except ClickHouseUnavailable as exc:
        raise web.HTTPServiceUnavailable(reason=str(exc)) from exc
    return _json({"vehicle_short": vehicle_short, "from_ms": from_ms, "to_ms": to_ms, **track})


async def handle_app_config(request: web.Request) -> web.Response:
    session = request.get("admin_session") or {}
    auth: Optional[AuthManager] = request.app.get("auth")
    auth_enabled = bool(auth is not None and auth.enabled)
    return _json(
        {
            "tile_url": request.app.get("tile_url"),
            "tile_attribution": request.app.get("tile_attribution"),
            "max_analysis_days": request.app.get("max_analysis_days", 90),
            "csrf_token": session.get("csrf_token"),
            "auth_enabled": auth_enabled,
            "authenticated": bool(session) or not auth_enabled,
        }
    )


def _quality_service(request: web.Request) -> WiFiQualityService:
    service: Optional[WiFiQualityService] = request.app.get("wifi_quality")
    if service is None:
        raise web.HTTPServiceUnavailable(reason="Wi-Fi quality analytics requires ClickHouse")
    return service


def _metadata(request: web.Request) -> MetadataStore:
    store: Optional[MetadataStore] = request.app.get("metadata")
    if store is None:
        raise web.HTTPServiceUnavailable(reason="Metadata store is not configured")
    return store


def _csv_values(request: web.Request, name: str) -> list[str]:
    values: list[str] = []
    for raw in request.rel_url.query.getall(name, []):
        values.extend(v.strip() for v in raw.split(",") if v.strip())
    return values


def _quality_polygon(raw: Optional[str]) -> Optional[list[list[float]]]:
    if not raw:
        return None
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get("type") != "Polygon":
        raise ValueError("area must be a GeoJSON Polygon")
    coordinates = value.get("coordinates")
    if (
        not isinstance(coordinates, list)
        or len(coordinates) != 1
        or not isinstance(coordinates[0], list)
        or len(coordinates[0]) < 3
        or len(coordinates[0]) > 500
    ):
        raise ValueError("area must be one exterior polygon ring with 3 to 500 points")
    polygon: list[list[float]] = []
    for point in coordinates[0]:
        if not isinstance(point, list) or len(point) < 2:
            raise ValueError("area polygon coordinates must be [longitude, latitude]")
        polygon.append([float(point[1]), float(point[0])])
    if polygon[0] == polygon[-1]:
        polygon.pop()
    if len(polygon) < 3:
        raise ValueError("area polygon must contain at least three distinct points")
    return polygon


async def handle_wifi_quality(request: web.Request) -> web.Response:
    q = request.rel_url.query
    try:
        from_ms, to_ms = int(q["from_ms"]), int(q["to_ms"])
        resolution = int(q.get("resolution", "9"))
        profile_id = int(q["profile_id"]) if q.get("profile_id") else None
        station_ids = [int(v) for v in _csv_values(request, "station_ids")]
        bounds = [float(v) for v in q["bounds"].split(",")] if q.get("bounds") else None
        polygon = _quality_polygon(q.get("area"))
        if polygon:
            bounds = [
                min(point[0] for point in polygon),
                min(point[1] for point in polygon),
                max(point[0] for point in polygon),
                max(point[1] for point in polygon),
            ]
        result = await asyncio.to_thread(
            _quality_service(request).heatmap,
            from_ms=from_ms,
            to_ms=to_ms,
            vehicles=_csv_values(request, "vehicles"),
            station_ids=station_ids,
            profile_id=profile_id,
            resolution=resolution,
            bounds=bounds,
            display=q.get("display", "h3"),
            station_mode=q.get("station_mode", "aggregate"),
            metric=q.get("metric", "composite"),
            value_mode=q.get("value_mode", "score"),
            polygon=polygon,
            allow_large_range=q.get("allow_large_range") in ("1", "true", "yes"),
        )
    except OverflowError as exc:
        return _json({"error": str(exc), "requires_override": True}, status=422)
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(reason=str(exc)) from exc
    return _json(result)


async def handle_base_stations(request: web.Request) -> web.Response:
    return _json({"base_stations": await asyncio.to_thread(_metadata(request).list_base_stations)})


async def handle_discover_bssids(request: web.Request) -> web.Response:
    created = await asyncio.to_thread(_quality_service(request).sync_catalog)
    stations = await asyncio.to_thread(_metadata(request).list_base_stations)
    return _json({"created": created, "base_stations": stations})


async def handle_create_station(request: web.Request) -> web.Response:
    body = await request.json()
    try:
        station = await asyncio.to_thread(
            _metadata(request).create_station, body.get("name"), body.get("lat"), body.get("lon")
        )
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(reason=str(exc)) from exc
    return _json(station, status=201)


async def handle_update_station(request: web.Request) -> web.Response:
    body = await request.json()
    try:
        station = await asyncio.to_thread(
            _metadata(request).update_station, int(request.match_info["station_id"]), body
        )
    except KeyError as exc:
        raise web.HTTPNotFound(reason=str(exc)) from exc
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(reason=str(exc)) from exc
    return _json(station)


async def handle_assign_bssid(request: web.Request) -> web.Response:
    body = await request.json()
    try:
        await asyncio.to_thread(
            _metadata(request).assign_bssid, body.get("bssid"), int(body.get("station_id"))
        )
    except KeyError as exc:
        raise web.HTTPNotFound(reason=str(exc)) from exc
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(reason=str(exc)) from exc
    return _json({"ok": True})


async def handle_merge_stations(request: web.Request) -> web.Response:
    body = await request.json()
    try:
        await asyncio.to_thread(
            _metadata(request).merge_stations,
            int(body.get("source_id")), int(body.get("target_id")),
        )
    except KeyError as exc:
        raise web.HTTPNotFound(reason=str(exc)) from exc
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(reason=str(exc)) from exc
    return _json({"ok": True})


async def handle_profiles(request: web.Request) -> web.Response:
    return _json({"profiles": await asyncio.to_thread(_metadata(request).list_profiles)})


async def handle_save_profile(request: web.Request) -> web.Response:
    body = await request.json()
    profile_id = int(request.match_info["profile_id"]) if request.match_info.get("profile_id") else None
    try:
        profile = await asyncio.to_thread(_metadata(request).save_profile, body, profile_id)
    except KeyError as exc:
        raise web.HTTPNotFound(reason=str(exc)) from exc
    except (TypeError, ValueError) as exc:
        raise web.HTTPBadRequest(reason=str(exc)) from exc
    return _json(profile, status=201 if profile_id is None else 200)


# ---------------------------------------------------------------------------
# Dashboard (HTML)
# ---------------------------------------------------------------------------

_LOGIN_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>OATI Telemetry · Sign in</title><style>
:root{color-scheme:dark}body{margin:0;background:#0b1419;color:#e7ecee;font:14px system-ui;
display:grid;place-items:center;min-height:100vh}.card{width:min(360px,calc(100% - 32px));
background:#0f1b21;border:1px solid #2a3d46;border-radius:12px;padding:24px;box-sizing:border-box}
h1{font-size:18px;color:#3fb6d8}label{display:block;color:#9fb6bd;margin:14px 0 5px}
input{width:100%;box-sizing:border-box;padding:9px;background:#0b1419;color:#fff;border:1px solid #2a3d46;
border-radius:6px}button{width:100%;margin-top:18px;padding:9px;border:0;border-radius:6px;
background:#3fb6d8;color:#071116;font-weight:700;cursor:pointer}.error{color:#e2615a;min-height:20px}
</style></head><body><form class="card" method="post"><h1>OATI Telemetry</h1>
<div class="error">__ERROR__</div><label>Username</label><input name="username" autocomplete="username" required>
<label>Password</label><input name="password" type="password" autocomplete="current-password" required>
<button type="submit">Sign in</button></form></body></html>"""


async def handle_login(request: web.Request) -> web.Response:
    auth: Optional[AuthManager] = request.app.get("auth")
    if auth is None or not auth.enabled:
        raise web.HTTPFound("/viewer")
    if auth.session(request):
        raise web.HTTPFound("/viewer")
    error = ""
    if request.method == "POST":
        form = await request.post()
        result = auth.authenticate(
            str(form.get("username") or ""),
            str(form.get("password") or ""),
            request.remote or "unknown",
        )
        if result:
            token, _csrf = result
            response = web.HTTPFound("/viewer")
            auth.set_cookie(response, token)
            raise response
        error = "Invalid credentials or too many attempts"
    return web.Response(content_type="text/html", text=_LOGIN_HTML.replace("__ERROR__", error))


async def handle_logout(request: web.Request) -> web.Response:
    auth: Optional[AuthManager] = request.app.get("auth")
    if auth is not None:
        auth.logout(request)
    response = web.HTTPFound("/login")
    response.del_cookie("oati_session", path="/")
    raise response


_DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>oati-ts-monitor · Log Collector</title>
<style>
  :root {
    --bg: #0f1117;
    --surface: #1a1d27;
    --surface2: #252836;
    --border: #2e3145;
    --accent: #4f9cf9;
    --accent2: #6ee7b7;
    --warn: #f59e0b;
    --text: #e2e8f0;
    --muted: #64748b;
    --danger: #f87171;
    --mono: 'JetBrains Mono', 'Fira Code', monospace;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: system-ui, sans-serif;
         font-size: 14px; min-height: 100vh; }
  header {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 14px 24px;
    display: flex; align-items: center; gap: 12px;
  }
  header h1 { font-size: 17px; font-weight: 600; color: var(--accent); }
  header .pill {
    font-size: 11px; padding: 2px 8px; border-radius: 99px;
    background: var(--surface2); border: 1px solid var(--border); color: var(--muted);
  }
  #uptime { margin-left: auto; font-size: 12px; color: var(--muted); font-family: var(--mono); }
  header a.viewer-link {
    font-size: 12px; color: var(--accent); text-decoration: none;
    border: 1px solid var(--border); border-radius: 6px; padding: 4px 10px;
  }
  header a.viewer-link:hover { background: var(--surface2); }

  main { padding: 24px; max-width: 1300px; margin: 0 auto; }

  .stats-grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 14px; margin-bottom: 28px;
  }
  .stat-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 10px; padding: 16px 18px;
  }
  .stat-card .label { font-size: 11px; color: var(--muted); text-transform: uppercase;
                       letter-spacing: .05em; margin-bottom: 6px; }
  .stat-card .value { font-size: 26px; font-weight: 700; font-family: var(--mono);
                       color: var(--accent); }
  .stat-card .sub   { font-size: 11px; color: var(--muted); margin-top: 2px; }

  section { margin-bottom: 32px; }
  section h2 { font-size: 13px; font-weight: 600; text-transform: uppercase;
                letter-spacing: .08em; color: var(--muted); margin-bottom: 12px; }

  .table-wrap { overflow-x: auto; border-radius: 10px; border: 1px solid var(--border); }
  table { border-collapse: collapse; width: 100%; }
  th {
    background: var(--surface2); color: var(--muted); font-size: 11px;
    font-weight: 600; text-transform: uppercase; letter-spacing: .06em;
    padding: 10px 14px; text-align: left; white-space: nowrap;
  }
  td { padding: 10px 14px; border-top: 1px solid var(--border);
       color: var(--text); font-family: var(--mono); font-size: 13px; }
  tr:hover td { background: var(--surface2); }
  .badge {
    display: inline-block; padding: 1px 7px; border-radius: 4px; font-size: 11px;
    font-family: var(--mono); margin-right: 4px;
    background: var(--surface2); border: 1px solid var(--border); color: var(--accent2);
  }
  .device-link { color: var(--accent); cursor: pointer; text-decoration: underline; }
  .refresh-bar {
    display: flex; align-items: center; gap: 10px; margin-bottom: 18px;
  }
  .refresh-bar button {
    background: var(--accent); color: #fff; border: none; border-radius: 6px;
    padding: 6px 14px; cursor: pointer; font-size: 13px; font-weight: 600;
  }
  .refresh-bar button:hover { opacity: .85; }
  .refresh-bar label { font-size: 12px; color: var(--muted); display: flex;
                        align-items: center; gap: 6px; cursor: pointer; }
  #status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--accent2);
                display: inline-block; }
  .empty { color: var(--muted); text-align: center; padding: 24px; font-style: italic; }

  /* segment viewer */
  #viewer { display: none; }
  #viewer h2 { font-size: 13px; color: var(--muted); text-transform: uppercase;
                letter-spacing: .08em; margin-bottom: 8px; }
  #viewer-title { color: var(--text); }
  #viewer pre {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 10px; padding: 16px; overflow-x: auto;
    font-size: 12px; font-family: var(--mono); color: var(--accent2);
    max-height: 480px; overflow-y: auto;
    white-space: pre-wrap; word-break: break-all;
  }
  #viewer-close {
    float: right; background: none; border: 1px solid var(--border);
    color: var(--muted); border-radius: 5px; padding: 3px 10px;
    cursor: pointer; font-size: 12px; margin-top: -2px;
  }
</style>
</head>
<body>
<header>
  <span id="status-dot"></span>
  <h1>oati-ts-monitor · Log Collector</h1>
  <span class="pill">NDJSON ingest</span>
  <a class="viewer-link" href="/viewer">Historical viewer</a>
  <div id="uptime">–</div>
</header>

<main>
  <div class="refresh-bar">
    <button onclick="refresh()">Refresh</button>
    <label>
      <input type="checkbox" id="auto-refresh" checked>
      Auto-refresh every 5 s
    </label>
  </div>

  <div class="stats-grid" id="stats-grid">
    <div class="stat-card"><div class="label">Devices</div><div class="value" id="s-devices">–</div></div>
    <div class="stat-card"><div class="label">Segments</div><div class="value" id="s-segments">–</div></div>
    <div class="stat-card"><div class="label">Files</div><div class="value" id="s-files">–</div></div>
    <div class="stat-card"><div class="label">Data received</div><div class="value" id="s-bytes">–</div><div class="sub" id="s-bytes-raw"></div></div>
    <div class="stat-card"><div class="label">Uptime</div><div class="value" id="s-uptime">–</div></div>
  </div>

  <section>
    <h2>Devices</h2>
    <div class="table-wrap">
      <table id="devices-table">
        <thead><tr>
          <th>Device ID</th><th>Vehicles</th><th>Segments</th>
          <th>Total data</th><th>Last activity</th>
        </tr></thead>
        <tbody id="devices-tbody"><tr><td colspan="5" class="empty">Loading…</td></tr></tbody>
      </table>
    </div>
  </section>

  <section>
    <h2>Recent segments <span id="segments-filter-label"></span></h2>
    <div class="table-wrap">
      <table id="segments-table">
        <thead><tr>
          <th>Device</th><th>Vehicle</th><th>Segment</th>
          <th>Size</th><th>Files</th><th>Last upload</th><th></th>
        </tr></thead>
        <tbody id="segments-tbody"><tr><td colspan="7" class="empty">Loading…</td></tr></tbody>
      </table>
    </div>
  </section>

  <div id="viewer">
    <h2><span id="viewer-title">Segment preview</span> <button id="viewer-close" onclick="closeViewer()">✕ Close</button></h2>
    <pre id="viewer-pre">Loading…</pre>
  </div>
</main>

<script>
let deviceFilter = null;
let autoRefreshTimer = null;

function fmtBytes(b) {
  if (b < 1024) return b + ' B';
  if (b < 1024*1024) return (b/1024).toFixed(1) + ' KB';
  if (b < 1024*1024*1024) return (b/1024/1024).toFixed(2) + ' MB';
  return (b/1024/1024/1024).toFixed(2) + ' GB';
}
function fmtUptime(sec) {
  const h = Math.floor(sec/3600), m = Math.floor((sec%3600)/60), s = Math.floor(sec%60);
  if (h) return h+'h '+m+'m';
  if (m) return m+'m '+s+'s';
  return s+'s';
}
function reltime(iso) {
  const diff = (Date.now() - new Date(iso).getTime()) / 1000;
  if (diff < 5) return 'just now';
  if (diff < 60) return Math.round(diff)+'s ago';
  if (diff < 3600) return Math.round(diff/60)+'m ago';
  if (diff < 86400) return Math.round(diff/3600)+'h ago';
  return new Date(iso).toLocaleDateString();
}
function escHTML(value) {
  return String(value ?? '').replace(/[&<>"']/g, c => ({
    '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'
  }[c]));
}

async function fetchJSON(url) {
  const r = await fetch(url); return r.json();
}

async function refresh() {
  const [status, devices, segments] = await Promise.all([
    fetchJSON('/api/status'),
    fetchJSON('/api/devices'),
    fetchJSON('/api/segments' + (deviceFilter ? '?device='+encodeURIComponent(deviceFilter) : '')),
  ]);

  document.getElementById('s-devices').textContent = devices.length;
  document.getElementById('s-segments').textContent = status.total_segments;
  document.getElementById('s-files').textContent = status.total_files ?? status.total_chunks;
  document.getElementById('s-bytes').textContent = fmtBytes(status.total_bytes);
  document.getElementById('s-bytes-raw').textContent = status.total_bytes.toLocaleString() + ' bytes';
  document.getElementById('s-uptime').textContent = fmtUptime(status.uptime_sec);
  document.getElementById('uptime').textContent = 'up ' + fmtUptime(status.uptime_sec);

  // Devices table
  const dtb = document.getElementById('devices-tbody');
  if (!devices.length) {
    dtb.innerHTML = '<tr><td colspan="5" class="empty">No devices yet.</td></tr>';
  } else {
    dtb.innerHTML = devices.map(d => `
      <tr>
        <td><span class="device-link" data-device="${escHTML(d.device_id)}">${escHTML(d.device_id)}</span></td>
        <td>${d.vehicles.map(v=>`<span class="badge">${escHTML(v)}</span>`).join('')}</td>
        <td>${d.total_segments}</td>
        <td>${fmtBytes(d.total_bytes)}</td>
        <td>${reltime(d.last_seen)}</td>
      </tr>`).join('');
  }

  // Segments table
  const stb = document.getElementById('segments-tbody');
  const label = document.getElementById('segments-filter-label');
  label.textContent = deviceFilter ? `— ${deviceFilter}` : '';
  if (!segments.length) {
    stb.innerHTML = '<tr><td colspan="7" class="empty">No segments yet.</td></tr>';
  } else {
    stb.innerHTML = segments.slice(0, 50).map(s => `
      <tr>
        <td>${escHTML(s.device_id)}</td>
        <td><span class="badge">${escHTML(s.vehicle_short)}</span></td>
        <td style="font-size:12px">${escHTML(s.segment)}</td>
        <td>${fmtBytes(s.bytes_received)}</td>
        <td>${s.files_received ?? s.chunks_received}</td>
        <td>${reltime(s.last_seen)}</td>
        <td><button class="view-segment" data-device="${escHTML(s.device_id)}"
            data-vehicle="${escHTML(s.vehicle_short)}" data-segment="${escHTML(s.segment)}"
            style="background:var(--surface2);border:1px solid var(--border);color:var(--muted);
                   border-radius:5px;padding:3px 10px;cursor:pointer;font-size:11px;">
            View</button></td>
      </tr>`).join('');
    stb.querySelectorAll('.view-segment').forEach(button => {
      button.addEventListener('click', () => viewSegment(
        button.dataset.device, button.dataset.vehicle, button.dataset.segment
      ));
    });
  }
  dtb.querySelectorAll('.device-link').forEach(link => {
    link.addEventListener('click', () => filterDevice(link.dataset.device));
  });
}

function filterDevice(id) {
  deviceFilter = (deviceFilter === id) ? null : id;
  refresh();
}

async function viewSegment(device, vs, seg) {
  document.getElementById('viewer').style.display = 'block';
  document.getElementById('viewer-title').textContent = `${device} / ${vs} / ${seg}`;
  const pre = document.getElementById('viewer-pre');
  pre.textContent = 'Loading…';
  const data = await fetchJSON(`/api/segments/${encodeURIComponent(device)}/${encodeURIComponent(vs)}/${encodeURIComponent(seg)}?n=50`);
  pre.textContent = data.records.map(r => JSON.stringify(r, null, 2)).join('\\n---\\n');
  document.getElementById('viewer').scrollIntoView({behavior:'smooth'});
}

function closeViewer() { document.getElementById('viewer').style.display = 'none'; }

function setupAutoRefresh() {
  const cb = document.getElementById('auto-refresh');
  function tick() {
    if (cb.checked) { refresh(); autoRefreshTimer = setTimeout(tick, 5000); }
  }
  cb.addEventListener('change', () => {
    if (cb.checked) tick(); else clearTimeout(autoRefreshTimer);
  });
  tick();
}

setupAutoRefresh();
</script>
</body>
</html>
"""


async def handle_dashboard(request: web.Request) -> web.Response:
    return web.Response(content_type="text/html", text=_DASHBOARD_HTML)


async def handle_viewer(request: web.Request) -> web.Response:
    for path in _VIEWER_TEMPLATE_CANDIDATES:
        if path.is_file():
            return web.Response(content_type="text/html", text=path.read_text(encoding="utf-8"))
    raise web.HTTPNotFound(reason="log_viewer.html template not found")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def make_app(
    storage: ChunkStorage,
    *,
    max_upload_bytes: int = 16 * 1024 * 1024,
    clickhouse: Optional[ClickHouseStore] = None,
    activity_gap_ms: int = 120_000,
    backfill_data_dir_path: Optional[str] = None,
    backfill_on_startup: bool = False,
    metadata: Optional[MetadataStore] = None,
    auth: Optional[AuthManager] = None,
    max_analysis_days: int = 90,
    tile_url: str = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
    tile_attribution: str = "© OpenStreetMap contributors",
) -> web.Application:
    # aiohttp defaults to 1 MiB; daemon segments can produce larger gzip files.
    middlewares = [auth_middleware(auth)] if auth is not None else []
    app = web.Application(
        client_max_size=max(1024 * 1024, int(max_upload_bytes)),
        middlewares=middlewares,
    )
    app["storage"] = storage
    app["clickhouse"] = clickhouse
    app["activity_gap_ms"] = int(activity_gap_ms)
    app["metadata"] = metadata
    app["auth"] = auth
    app["max_analysis_days"] = max(1, int(max_analysis_days))
    app["tile_url"] = tile_url
    app["tile_attribution"] = tile_attribution
    app["wifi_quality"] = (
        WiFiQualityService(clickhouse, metadata, max_days=max_analysis_days)
        if clickhouse is not None and metadata is not None else None
    )
    app["start_mono"] = time.monotonic()
    app["backfill_status"] = {
        "state": "idle",
        "started_at": None,
        "finished_at": None,
        "stats": None,
        "error": None,
    }

    app.router.add_post("/ingest", handle_ingest)
    app.router.add_get("/login", handle_login)
    app.router.add_post("/login", handle_login)
    app.router.add_post("/logout", handle_logout)
    app.router.add_get("/", handle_dashboard)
    app.router.add_get("/viewer", handle_viewer)
    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/devices", handle_devices)
    app.router.add_get("/api/segments", handle_segments)
    app.router.add_get(
        "/api/segments/{device}/{vehicle_short}/{segment}",
        handle_segment_tail,
    )
    app.router.add_get("/api/vehicles", handle_api_vehicles)
    app.router.add_get("/api/activity", handle_api_activity)
    app.router.add_get("/api/slice", handle_api_slice)
    app.router.add_get("/api/track", handle_api_track)
    app.router.add_get("/api/app-config", handle_app_config)
    app.router.add_get("/api/wifi-quality", handle_wifi_quality)
    app.router.add_get("/api/base-stations", handle_base_stations)
    app.router.add_post("/api/base-stations", handle_create_station)
    app.router.add_patch("/api/base-stations/{station_id}", handle_update_station)
    app.router.add_post("/api/base-stations/discover", handle_discover_bssids)
    app.router.add_post("/api/base-stations/assign-bssid", handle_assign_bssid)
    app.router.add_post("/api/base-stations/merge", handle_merge_stations)
    app.router.add_get("/api/quality-profiles", handle_profiles)
    app.router.add_post("/api/quality-profiles", handle_save_profile)
    app.router.add_patch("/api/quality-profiles/{profile_id}", handle_save_profile)

    if clickhouse is not None and backfill_on_startup and backfill_data_dir_path:
        async def _start_backfill(app_: web.Application) -> None:
            # IMPORTANT: do not await the full backfill here — aiohttp waits for
            # all on_startup handlers before accepting connections.
            status = app_["backfill_status"]
            status["state"] = "running"
            status["started_at"] = datetime.now(timezone.utc).isoformat()
            status["finished_at"] = None
            status["stats"] = None
            status["error"] = None
            logger.info(
                "Starting ClickHouse backfill in background for %s",
                backfill_data_dir_path,
            )

            def _run() -> dict:
                return backfill_data_dir(clickhouse, backfill_data_dir_path)

            async def _job() -> None:
                try:
                    stats = await asyncio.to_thread(_run)
                    quality: Optional[WiFiQualityService] = app_.get("wifi_quality")
                    if quality is not None:
                        await asyncio.to_thread(quality.sync_catalog)
                    status["state"] = "done"
                    status["stats"] = stats
                    logger.info("Background backfill finished: %s", stats)
                except Exception as exc:
                    status["state"] = "error"
                    status["error"] = str(exc)
                    logger.exception("Background backfill failed")
                finally:
                    status["finished_at"] = datetime.now(timezone.utc).isoformat()

            app_["backfill_task"] = asyncio.create_task(_job())

        app.on_startup.append(_start_backfill)

    return app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _json(data: Any, *, status: int = 200) -> web.Response:
    return web.Response(
        status=status,
        content_type="application/json",
        text=json.dumps(data, ensure_ascii=False, default=str),
    )


def _fmt_ts(ts: float) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
