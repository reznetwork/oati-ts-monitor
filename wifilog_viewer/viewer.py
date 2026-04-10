from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, Optional


@dataclass(frozen=True)
class WifiSample:
    ts_ms: int
    lat: float
    lon: float
    rssi_dbm: Optional[float]
    signal_avg_dbm: Optional[float]
    bssid: Optional[str]
    channel: Optional[int]
    tx_rate_mbps: Optional[float]
    rx_rate_mbps: Optional[float]
    gateway_latency_ms: Optional[float]


@dataclass(frozen=True)
class RoamingEvent:
    ts_ms: int
    lat: float
    lon: float
    event: str
    details: dict[str, Any]


def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _as_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _valid_lat_lon(lat: Any, lon: Any) -> Optional[tuple[float, float]]:
    lat_f = _as_float(lat)
    lon_f = _as_float(lon)
    if lat_f is None or lon_f is None:
        return None
    if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lon_f <= 180.0):
        return None
    return lat_f, lon_f


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def load_wifilog(
    source: str | Path | IO[str],
    *,
    downsample: int = 1,
    max_jump_m: float = 0.0,
) -> tuple[list[WifiSample], list[RoamingEvent]]:
    """
    Load samples and roaming events from a wifi_capture_*.jsonl.

    - Keeps only entries where type == 'wifi_sample' and GNSS lat/lon is present.
    - Optional downsample keeps every Nth sample (N>=1).
    - Optional max_jump_m drops points that jump too far from previous kept point.
    """
    if downsample < 1:
        downsample = 1
    max_jump_m = float(max_jump_m or 0.0)

    should_close = False
    if isinstance(source, (str, Path)):
        f = Path(source).expanduser().open("r", encoding="utf-8", errors="replace")
        should_close = True
    else:
        f = source

    out: list[WifiSample] = []
    events: list[RoamingEvent] = []
    prev: Optional[WifiSample] = None
    idx = 0
    try:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            rtype = rec.get("type")
            if rtype not in ("wifi_sample", "roaming_event"):
                continue

            gnss = rec.get("gnss") or {}
            ll = _valid_lat_lon(gnss.get("lat"), gnss.get("lon"))
            if ll is None:
                continue
            ts_ms = _as_int(rec.get("ts_ms"))
            if ts_ms is None:
                continue

            if rtype == "roaming_event":
                ev = str(rec.get("event") or "event")
                details = rec.get("details") or {}
                if not isinstance(details, dict):
                    details = {"details": details}
                events.append(
                    RoamingEvent(
                        ts_ms=ts_ms,
                        lat=ll[0],
                        lon=ll[1],
                        event=ev,
                        details=details,
                    )
                )
                continue

            wifi = rec.get("wifi") or {}
            bssid_raw = wifi.get("bssid")
            bssid = str(bssid_raw) if bssid_raw else None
            chan = _as_int(wifi.get("channel"))
            gw_ms: Optional[float] = None
            gateways = rec.get("gateways")
            if isinstance(gateways, dict) and gateways:
                # Prefer a key literally named "gateway" if present; else if only one entry exists use it.
                pick: Any = None
                if "gateway" in gateways and isinstance(gateways.get("gateway"), dict):
                    pick = gateways.get("gateway")
                elif len(gateways) == 1:
                    only = next(iter(gateways.values()))
                    pick = only if isinstance(only, dict) else None
                if isinstance(pick, dict):
                    gw_ms = _as_float(pick.get("latency_ms"))
            sample = WifiSample(
                ts_ms=ts_ms,
                lat=ll[0],
                lon=ll[1],
                rssi_dbm=_as_float(wifi.get("rssi_dbm")),
                signal_avg_dbm=_as_float(wifi.get("signal_avg_dbm")),
                bssid=bssid,
                channel=chan,
                tx_rate_mbps=_as_float(wifi.get("tx_rate_mbps")),
                rx_rate_mbps=_as_float(wifi.get("rx_rate_mbps")),
                gateway_latency_ms=gw_ms,
            )

            idx += 1
            if downsample > 1 and (idx % downsample) != 0:
                continue

            if prev is not None and max_jump_m > 0:
                if _haversine_m(prev.lat, prev.lon, sample.lat, sample.lon) > max_jump_m:
                    continue

            out.append(sample)
            prev = sample
    finally:
        if should_close:
            f.close()

    return out, events


def load_wifi_samples(
    source: str | Path | IO[str],
    *,
    downsample: int = 1,
    max_jump_m: float = 0.0,
) -> list[WifiSample]:
    samples, _events = load_wifilog(source, downsample=downsample, max_jump_m=max_jump_m)
    return samples


def _clamp(v: float, lo: float, hi: float) -> float:
    return lo if v < lo else hi if v > hi else v


def _palette() -> list[str]:
    return [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
        "#bcbd22",
        "#17becf",
    ]


def _cat_color(key: str, mapping: dict[str, str]) -> str:
    if key in mapping:
        return mapping[key]
    colors = _palette()
    mapping[key] = colors[len(mapping) % len(colors)]
    return mapping[key]


def build_map(
    samples: list[WifiSample],
    *,
    min_db: float = -90.0,
    max_db: float = -40.0,
    tiles: str = "OpenStreetMap",
    roaming_events: Optional[list[RoamingEvent]] = None,
) -> "folium.Map":
    """
    Build a folium map with two layers:
    - RSSI (wifi.rssi_dbm)
    - Signal avg (wifi.signal_avg_dbm; fallback to rssi_dbm)
    """
    import folium
    from branca.colormap import LinearColormap

    if not samples:
        m = folium.Map(location=[0, 0], zoom_start=2, tiles=tiles)
        folium.map.LayerControl(collapsed=False).add_to(m)
        return m

    def _add_html(el_html: str) -> None:
        m.get_root().html.add_child(folium.Element(el_html))

    def _add_script(js: str) -> None:
        # folium already renders content under a <script> tag; do not nest <script>.
        m.get_root().script.add_child(folium.Element(js))

    min_db = float(min_db)
    max_db = float(max_db)
    if min_db == max_db:
        min_db -= 1.0
        max_db += 1.0
    if min_db > max_db:
        min_db, max_db = max_db, min_db

    center = [samples[0].lat, samples[0].lon]
    m = folium.Map(location=center, zoom_start=16, tiles=tiles, control_scale=True)

    cm = LinearColormap(["#d73027", "#fee08b", "#1a9850"], vmin=min_db, vmax=max_db)

    fg_rssi = folium.FeatureGroup(name="RSSI (rssi_dbm)", show=True)
    fg_avg = folium.FeatureGroup(name="Signal avg (signal_avg_dbm)", show=False)
    fg_bssid = folium.FeatureGroup(name="BSSID (categorical)", show=False)
    fg_channel = folium.FeatureGroup(name="Channel (categorical)", show=False)
    fg_tx = folium.FeatureGroup(name="TX rate (Mbps)", show=False)
    fg_rx = folium.FeatureGroup(name="RX rate (Mbps)", show=False)
    fg_gw = folium.FeatureGroup(name="Gateway latency (ms)", show=False)
    fg_events = folium.FeatureGroup(name="Roaming events", show=False)

    bounds: list[list[float]] = []

    def _add_segment(
        fg: folium.FeatureGroup, a: WifiSample, b: WifiSample, value: Optional[float]
    ) -> None:
        if value is None:
            return
        v = _clamp(float(value), min_db, max_db)
        folium.PolyLine(
            locations=[[a.lat, a.lon], [b.lat, b.lon]],
            color=cm(v),
            weight=5,
            opacity=0.9,
        ).add_to(fg)

    bssid_colors: dict[str, str] = {}
    chan_colors: dict[str, str] = {}

    # For rates, use automatic bounds from available values (avoid empty / degenerate).
    tx_vals = [s.tx_rate_mbps for s in samples if isinstance(s.tx_rate_mbps, (int, float))]
    rx_vals = [s.rx_rate_mbps for s in samples if isinstance(s.rx_rate_mbps, (int, float))]
    gw_vals = [s.gateway_latency_ms for s in samples if isinstance(s.gateway_latency_ms, (int, float))]
    tx_min = min(tx_vals) if tx_vals else 0.0
    tx_max = max(tx_vals) if tx_vals else 1.0
    rx_min = min(rx_vals) if rx_vals else 0.0
    rx_max = max(rx_vals) if rx_vals else 1.0
    gw_min = min(gw_vals) if gw_vals else 0.0
    gw_max = max(gw_vals) if gw_vals else 1.0
    if tx_min == tx_max:
        tx_max = tx_min + 1.0
    if rx_min == rx_max:
        rx_max = rx_min + 1.0
    if gw_min == gw_max:
        gw_max = gw_min + 1.0
    cm_tx = LinearColormap(["#440154", "#21918c", "#fde725"], vmin=tx_min, vmax=tx_max)
    cm_rx = LinearColormap(["#440154", "#21918c", "#fde725"], vmin=rx_min, vmax=rx_max)
    # Low latency is good -> green; high latency is bad -> red.
    cm_gw = LinearColormap(["#1a9850", "#fee08b", "#d73027"], vmin=gw_min, vmax=gw_max)

    for a, b in zip(samples, samples[1:]):
        bounds.append([a.lat, a.lon])
        bounds.append([b.lat, b.lon])
        _add_segment(fg_rssi, a, b, a.rssi_dbm)
        avg_val = a.signal_avg_dbm if a.signal_avg_dbm is not None else a.rssi_dbm
        _add_segment(fg_avg, a, b, avg_val)

        if a.bssid:
            col = _cat_color(a.bssid, bssid_colors)
            folium.PolyLine(
                locations=[[a.lat, a.lon], [b.lat, b.lon]],
                color=col,
                weight=5,
                opacity=0.9,
                tooltip=f"bssid={a.bssid}",
            ).add_to(fg_bssid)

        if a.channel is not None:
            key = str(a.channel)
            col = _cat_color(key, chan_colors)
            folium.PolyLine(
                locations=[[a.lat, a.lon], [b.lat, b.lon]],
                color=col,
                weight=5,
                opacity=0.9,
                tooltip=f"ch={a.channel}",
            ).add_to(fg_channel)

        if a.tx_rate_mbps is not None:
            v = _clamp(float(a.tx_rate_mbps), float(tx_min), float(tx_max))
            folium.PolyLine(
                locations=[[a.lat, a.lon], [b.lat, b.lon]],
                color=cm_tx(v),
                weight=5,
                opacity=0.9,
                tooltip=f"tx={a.tx_rate_mbps:.1f} Mbps",
            ).add_to(fg_tx)

        if a.rx_rate_mbps is not None:
            v = _clamp(float(a.rx_rate_mbps), float(rx_min), float(rx_max))
            folium.PolyLine(
                locations=[[a.lat, a.lon], [b.lat, b.lon]],
                color=cm_rx(v),
                weight=5,
                opacity=0.9,
                tooltip=f"rx={a.rx_rate_mbps:.1f} Mbps",
            ).add_to(fg_rx)

        if a.gateway_latency_ms is not None:
            v = _clamp(float(a.gateway_latency_ms), float(gw_min), float(gw_max))
            folium.PolyLine(
                locations=[[a.lat, a.lon], [b.lat, b.lon]],
                color=cm_gw(v),
                weight=5,
                opacity=0.9,
                tooltip=f"gw={a.gateway_latency_ms:.1f} ms",
            ).add_to(fg_gw)

    fg_rssi.add_to(m)
    fg_avg.add_to(m)
    fg_bssid.add_to(m)
    fg_channel.add_to(m)
    fg_tx.add_to(m)
    fg_rx.add_to(m)
    fg_gw.add_to(m)

    # ---- Legends (custom HTML so we can toggle per layer) ----
    # Note: Folium/branca legends all share the same `.legend` container by default,
    # so we implement our own with unique IDs to show/hide reliably.
    def _legend_box(title: str, body_html: str, *, legend_id: str) -> str:
        return f"""
<div id="{legend_id}" style="
  display:none;
  position: fixed;
  bottom: 10px;
  right: 10px;
  z-index: 9999;
  background: rgba(255,255,255,0.95);
  padding: 10px 12px;
  border: 1px solid rgba(0,0,0,0.2);
  border-radius: 6px;
  box-shadow: 0 1px 6px rgba(0,0,0,0.2);
  font-size: 12px;
  line-height: 1.25;
  max-width: 260px;
">
  <div style="font-weight: 600; margin-bottom: 6px;">{title}</div>
  {body_html}
</div>
""".strip()

    def _legend_gradient(*, vmin: float, vmax: float, colors: list[str], unit: str) -> str:
        grad = ", ".join(colors)
        return f"""
<div style="display:flex; align-items:center; gap:8px;">
  <div style="min-width: 52px; text-align:left;">{vmin:.0f}{unit}</div>
  <div style="width: 140px; min-width: 140px; height:12px; border-radius:6px; border:1px solid rgba(0,0,0,0.25);
              background-image: linear-gradient(to right, {grad}); background-color: #fff;"></div>
  <div style="min-width: 52px; text-align:right;">{vmax:.0f}{unit}</div>
</div>
""".strip()

    # WiFi signal legend (used for both RSSI and avg)
    _add_html(
        _legend_box(
            "WiFi signal (dBm)",
            _legend_gradient(vmin=min_db, vmax=max_db, colors=["#d73027", "#fee08b", "#1a9850"], unit=""),
            legend_id="legend_signal",
        )
    )
    # TX/RX legends
    _add_html(
        _legend_box(
            "TX rate (Mbps)",
            _legend_gradient(vmin=float(tx_min), vmax=float(tx_max), colors=["#440154", "#21918c", "#fde725"], unit=""),
            legend_id="legend_tx",
        )
    )
    _add_html(
        _legend_box(
            "RX rate (Mbps)",
            _legend_gradient(vmin=float(rx_min), vmax=float(rx_max), colors=["#440154", "#21918c", "#fde725"], unit=""),
            legend_id="legend_rx",
        )
    )
    _add_html(
        _legend_box(
            "Gateway latency (ms)",
            _legend_gradient(vmin=float(gw_min), vmax=float(gw_max), colors=["#1a9850", "#fee08b", "#d73027"], unit=""),
            legend_id="legend_gw",
        )
    )

    # BSSID / Channel categorical legends (scrollable)
    def _legend_kv(items: list[tuple[str, str]]) -> str:
        rows = "\n".join(
            f"""<div style="display:flex; align-items:center; gap:8px; margin:2px 0;">
  <span style="display:inline-block; width:12px; height:12px; border-radius:3px; background:{col}; border:1px solid rgba(0,0,0,0.25);"></span>
  <span style="white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">{k}</span>
</div>"""
            for k, col in items
        )
        return f'<div style="max-height:180px; overflow:auto; padding-right:4px;">{rows}</div>'

    if bssid_colors:
        bssid_items = sorted(bssid_colors.items(), key=lambda kv: kv[0])
        _add_html(_legend_box("BSSID", _legend_kv(bssid_items), legend_id="legend_bssid"))
    else:
        _add_html(_legend_box("BSSID", "<div>(no bssid in log)</div>", legend_id="legend_bssid"))

    if chan_colors:
        chan_items = sorted(chan_colors.items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else kv[0])
        _add_html(_legend_box("Channel", _legend_kv(chan_items), legend_id="legend_channel"))
    else:
        _add_html(_legend_box("Channel", "<div>(no channel in log)</div>", legend_id="legend_channel"))

    if roaming_events:
        # Highlight attachment events; show all roaming_event records.
        ev_marker_names: list[tuple[str, str]] = []
        for ev in roaming_events:
            kind = ev.event or "event"
            color = "#00bcd4" if kind == "attachment" else "#9c27b0" if kind == "search" else "#607d8b"
            popup_lines = [f"<b>{kind}</b>", f"ts_ms={ev.ts_ms}"]
            for k in ("old_bssid", "new_bssid", "old_channel", "new_channel", "reason_code", "candidate_count", "top_candidate_rssi"):
                if k in ev.details and ev.details.get(k) is not None:
                    popup_lines.append(f"{k}={ev.details.get(k)}")
            marker = folium.CircleMarker(
                location=[ev.lat, ev.lon],
                radius=5,
                color=color,
                fill=True,
                fill_opacity=0.9,
                weight=2,
                tooltip=f"{kind}",
                popup=folium.Popup("<br/>".join(popup_lines), max_width=420),
            )
            marker.add_to(fg_events)
            ev_marker_names.append((marker.get_name(), kind))
        fg_events.add_to(m)

    folium.LayerControl(collapsed=False, position="topleft").add_to(m)

    if bounds:
        m.fit_bounds(bounds)

    # ---- JS: legend toggling + roaming event filter UI ----
    map_var = m.get_name()
    layers = {
        "rssi": fg_rssi.get_name(),
        "avg": fg_avg.get_name(),
        "bssid": fg_bssid.get_name(),
        "channel": fg_channel.get_name(),
        "tx": fg_tx.get_name(),
        "rx": fg_rx.get_name(),
        "gw": fg_gw.get_name(),
    }
    events_layer_var = fg_events.get_name()

    # Start with RSSI visible legend.
    _add_script(
        f"""
(function() {{
  function setVisible(id, on) {{
    var el = document.getElementById(id);
    if (!el) return;
    el.style.display = on ? 'block' : 'none';
  }}
  function anyOn(map, layerVars) {{
    for (var i = 0; i < layerVars.length; i++) {{
      try {{
        var lyr = window[layerVars[i]];
        if (lyr && map.hasLayer(lyr)) return true;
      }} catch (e) {{}}
    }}
    return false;
  }}

  var MAP_NAME = {json.dumps(map_var)};
  var LAYERS = {{
    rssi: {json.dumps(layers["rssi"])},
    avg: {json.dumps(layers["avg"])},
    bssid: {json.dumps(layers["bssid"])},
    channel: {json.dumps(layers["channel"])},
    tx: {json.dumps(layers["tx"])},
    rx: {json.dumps(layers["rx"])},
    gw: {json.dumps(layers["gw"])},
    events: {json.dumps(events_layer_var)}
  }};

  function refreshLegends(map) {{
    var rssi = window[LAYERS.rssi], avg = window[LAYERS.avg];
    var bssid = window[LAYERS.bssid], channel = window[LAYERS.channel];
    var tx = window[LAYERS.tx], rx = window[LAYERS.rx], gw = window[LAYERS.gw];
    setVisible('legend_signal', anyOn(map, [LAYERS.rssi, LAYERS.avg]));
    setVisible('legend_bssid', !!(bssid && map.hasLayer(bssid)));
    setVisible('legend_channel', !!(channel && map.hasLayer(channel)));
    setVisible('legend_tx', !!(tx && map.hasLayer(tx)));
    setVisible('legend_rx', !!(rx && map.hasLayer(rx)));
    setVisible('legend_gw', !!(gw && map.hasLayer(gw)));
  }}

  // Roaming event filter (checkboxes by type). Only affects the Roaming events layer.
  var roam = [];
  var roamTypes = {{}};
"""
        + (
            "\n".join(
                f"  roam.push({{n: {json.dumps(name)}, t: {json.dumps(kind)}}}); roamTypes[{json.dumps(kind)}] = true;"
                for name, kind in (ev_marker_names if roaming_events else [])
            )
        )
        + f"""

  function applyRoamFilter(map, selected) {{
    // When events layer is off, do nothing (we avoid re-adding to map).
    var grp = window[LAYERS.events];
    if (!grp) return;
    roam.forEach(function(x) {{
      try {{
        var mk = window[x.n];
        if (!mk) return;
        if (selected[x.t]) {{
          grp.addLayer(mk);
        }} else {{
          grp.removeLayer(mk);
        }}
      }} catch (e) {{}}
    }});
  }}

  function buildRoamControl(map) {{
    var ctrl = L.control({{position: 'topleft'}});
    ctrl.onAdd = function() {{
      var div = L.DomUtil.create('div', 'roam-filter');
      div.style.background = 'rgba(255,255,255,0.95)';
      div.style.padding = '8px 10px';
      div.style.border = '1px solid rgba(0,0,0,0.2)';
      div.style.borderRadius = '6px';
      div.style.boxShadow = '0 1px 6px rgba(0,0,0,0.2)';
      div.style.fontSize = '12px';
      div.style.lineHeight = '1.25';
      div.style.maxWidth = '260px';
      div.style.maxHeight = '220px';
      div.style.overflow = 'auto';
      // Place this control below the layers toggle in the same corner.
      div.style.marginTop = '56px';
      div.innerHTML = '<div style="font-weight:600; margin-bottom:6px;">Roaming events filter</div>';
      var types = Object.keys(roamTypes).sort();
      if (types.length === 0) {{
        div.innerHTML += '<div>(no roaming events)</div>';
      }} else {{
        types.forEach(function(t) {{
          var id = 'roam_' + t.replace(/[^a-zA-Z0-9_\\-]/g, '_');
          div.innerHTML += (
            '<label style="display:flex; align-items:center; gap:8px; margin:2px 0; cursor:pointer;">' +
              '<input type="checkbox" id=\"' + id + '\" checked />' +
              '<span>' + t + '</span>' +
            '</label>'
          );
        }});
      }}
      L.DomEvent.disableClickPropagation(div);
      L.DomEvent.disableScrollPropagation(div);
      setTimeout(function() {{
        types.forEach(function(t) {{
          var id = 'roam_' + t.replace(/[^a-zA-Z0-9_\\-]/g, '_');
          var cb = document.getElementById(id);
          if (!cb) return;
          cb.addEventListener('change', function() {{
            var selected = {{}};
            types.forEach(function(tt) {{
              var iid = 'roam_' + tt.replace(/[^a-zA-Z0-9_\\-]/g, '_');
              var el = document.getElementById(iid);
              selected[tt] = !!(el && el.checked);
            }});
            applyRoamFilter(map, selected);
          }});
        }});
      }}, 0);
      return div;
    }};
    return ctrl;
  }}

  function initWhenReady(tries) {{
    var map = window[MAP_NAME];
    if (!map) {{
      if (tries > 0) return setTimeout(function() {{ initWhenReady(tries - 1); }}, 25);
      return;
    }}
    map.on('overlayadd', function() {{ refreshLegends(map); }});
    map.on('overlayremove', function() {{ refreshLegends(map); }});
    refreshLegends(map);
    if (roam.length > 0) {{
      buildRoamControl(map).addTo(map);
    }}
  }}
  initWhenReady(200);
}})();
"""
    )

    return m


def write_map_html(
    samples: list[WifiSample],
    output: str | Path,
    *,
    min_db: float = -90.0,
    max_db: float = -40.0,
    tiles: str = "OpenStreetMap",
    roaming_events: Optional[list[RoamingEvent]] = None,
) -> None:
    import folium  # noqa: F401

    out = Path(output).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    m = build_map(samples, min_db=min_db, max_db=max_db, tiles=tiles, roaming_events=roaming_events)
    m.save(str(out))


def summarize_samples(samples: list[WifiSample]) -> dict[str, Any]:
    if not samples:
        return {"count": 0}
    rssi = [s.rssi_dbm for s in samples if isinstance(s.rssi_dbm, (int, float))]
    avg = [s.signal_avg_dbm for s in samples if isinstance(s.signal_avg_dbm, (int, float))]
    tx = [s.tx_rate_mbps for s in samples if isinstance(s.tx_rate_mbps, (int, float))]
    rx = [s.rx_rate_mbps for s in samples if isinstance(s.rx_rate_mbps, (int, float))]
    bssid = sorted({s.bssid for s in samples if s.bssid})
    chan = sorted({s.channel for s in samples if isinstance(s.channel, int)})
    return {
        "count": len(samples),
        "start_ts_ms": samples[0].ts_ms,
        "end_ts_ms": samples[-1].ts_ms,
        "bounds": {
            "min_lat": min(s.lat for s in samples),
            "max_lat": max(s.lat for s in samples),
            "min_lon": min(s.lon for s in samples),
            "max_lon": max(s.lon for s in samples),
        },
        "unique_bssid": len(bssid),
        "unique_channels": len(chan),
        "rssi_dbm": {
            "count": len(rssi),
            "min": min(rssi) if rssi else None,
            "max": max(rssi) if rssi else None,
        },
        "signal_avg_dbm": {
            "count": len(avg),
            "min": min(avg) if avg else None,
            "max": max(avg) if avg else None,
        },
        "tx_rate_mbps": {
            "count": len(tx),
            "min": min(tx) if tx else None,
            "max": max(tx) if tx else None,
        },
        "rx_rate_mbps": {
            "count": len(rx),
            "min": min(rx) if rx else None,
            "max": max(rx) if rx else None,
        },
    }

