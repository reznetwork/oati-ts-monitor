# WiFi log map viewer

This is a standalone viewer that converts `wifilogs/wifi_capture_*.jsonl` into an interactive HTML map showing the vehicle path, where **path color reflects signal level**.

## Install

From the repo root:

```bash
python3 -m pip install -r requirements-viewer.txt
```

## Run

Generate an HTML map:

```bash
python3 -m wifilog_viewer \
  --input wifilogs/wifi_capture_20260402_063419_496932.jsonl \
  --output out/wifi_map.html
```

Open the generated `out/wifi_map.html` in your browser.

## Options

- `--min-db` / `--max-db`: colormap range in dBm (defaults `-90`..`-40`)
- `--downsample N`: keep every Nth sample (for huge logs)
- `--max-jump-m M`: drop GNSS glitch points that jump more than M meters (0 disables)
- `--print-summary`: print basic stats about parsed samples

## Map layers

The HTML contains a layer toggle with:
- **RSSI (rssi_dbm)**: colored by `wifi.rssi_dbm`
- **Signal avg (signal_avg_dbm)**: colored by `wifi.signal_avg_dbm` (falls back to `rssi_dbm` when missing)
- **BSSID (categorical)**: colored by current `wifi.bssid`
- **Channel (categorical)**: colored by current `wifi.channel`
- **TX rate (Mbps)**: colored by `wifi.tx_rate_mbps`
- **RX rate (Mbps)**: colored by `wifi.rx_rate_mbps`
- **Roaming events**: markers from `type="roaming_event"` (including `event="attachment"` when present)

