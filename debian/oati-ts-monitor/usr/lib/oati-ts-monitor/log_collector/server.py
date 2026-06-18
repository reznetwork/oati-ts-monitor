"""
oati-ts-monitor Log Collector Server

Receives compressed NDJSON log-file uploads from oati-ts-monitor daemons and exposes
a minimal web dashboard for browsing received data.

Endpoints:
  POST /ingest                            – receive a compressed log file
  GET  /                                  – HTML dashboard
  GET  /api/status                        – JSON overall stats
  GET  /api/devices                       – JSON device list
  GET  /api/segments[?device=<id>]        – JSON segment list
  GET  /api/segments/<device>/<vs>/<seg>  – JSON last 200 lines of a segment
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from aiohttp import web

from .storage import ChunkStorage

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Ingest handler
# ---------------------------------------------------------------------------

async def handle_ingest(request: web.Request) -> web.Response:
    storage: ChunkStorage = request.app["storage"]

    # Required headers
    try:
        device_id   = request.headers["X-Device-Id"]
        vehicle     = request.headers.get("X-Vehicle", "")
        vehicle_short = request.headers.get("X-Vehicle-Short", "") or vehicle
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

    logger.info(
        "✓ %s/%s  file  (%d bytes)",
        device_id, segment, len(payload),
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
# Dashboard (HTML)
# ---------------------------------------------------------------------------

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
        <td><span class="device-link" onclick="filterDevice('${d.device_id}')">${d.device_id}</span></td>
        <td>${d.vehicles.map(v=>`<span class="badge">${v}</span>`).join('')}</td>
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
        <td>${s.device_id}</td>
        <td><span class="badge">${s.vehicle_short}</span></td>
        <td style="font-size:12px">${s.segment}</td>
        <td>${fmtBytes(s.bytes_received)}</td>
        <td>${s.files_received ?? s.chunks_received}</td>
        <td>${reltime(s.last_seen)}</td>
        <td><button onclick="viewSegment('${s.device_id}','${s.vehicle_short}','${s.segment}')"
            style="background:var(--surface2);border:1px solid var(--border);color:var(--muted);
                   border-radius:5px;padding:3px 10px;cursor:pointer;font-size:11px;">
            View</button></td>
      </tr>`).join('');
  }
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


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def make_app(storage: ChunkStorage) -> web.Application:
    app = web.Application()
    app["storage"] = storage
    app["start_mono"] = time.monotonic()

    app.router.add_post("/ingest", handle_ingest)
    app.router.add_get("/", handle_dashboard)
    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/devices", handle_devices)
    app.router.add_get("/api/segments", handle_segments)
    app.router.add_get(
        "/api/segments/{device}/{vehicle_short}/{segment}",
        handle_segment_tail,
    )

    return app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _json(data: Any) -> web.Response:
    return web.Response(
        content_type="application/json",
        text=json.dumps(data, ensure_ascii=False, default=str),
    )


def _fmt_ts(ts: float) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
