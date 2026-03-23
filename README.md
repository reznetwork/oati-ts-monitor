# oati-ts-monitor

The project now supports a split runtime:

- `daemon`: Modbus polling, optional GNSS/Wi‑Fi enrichment, optional HTTP dashboard, and optional JSONL data logging
- `client`: CLI commands and an ncurses TUI that reads live data from the daemon over a local TCP socket

## Run daemon

```bash
python daemon.py --config monitor_config.json --http --http-port 8080 --log-file monitor.jsonl
```

The daemon keeps polling until you stop it (Ctrl+C or systemd stop).

### Daemon flags (most commonly used)

Run the Modbus polling loop:
- `--config` path to `monitor_config.json` (default `monitor_config.json`)
- `--vehicle` select vehicle by name or index (1-based)
- `--poll` polling interval seconds (default `1.0`)
- `--poll-net` TCP reachability / latency check interval seconds (default: same as `--poll`)
- `--port` Modbus TCP port override (default from config)
- `--timeout` socket timeout seconds (default from config)
- `--unit` force a single Modbus unit id
- `--unit-candidates` list of unit ids to try (default `1 255 0`)
- `--no-coils` disable fallback to Modbus coils if discrete inputs fail

Optional services:
- `--http` enable the HTTP server (dashboard + `/ws` WebSocket)
- `--http-bind` (default `0.0.0.0`) and `--http-port` (default `8080`)
- `--log-file` enable JSONL data logging to that file
- `--log-interval` seconds between log writes (omit for “on change” logging)

IPC (client communication):
- `--ipc-bind` (default `127.0.0.1`) and `--ipc-port` (default `9102`)

## Run client

```bash
python client.py status
python client.py health
python client.py watch
python client.py tui
```

Notes:
- The client connects to the daemon via `--ipc-host`/`--ipc-port` (defaults: `127.0.0.1:9102`).
- `tui` requires `curses` support on your OS. If `curses` is unavailable, use `status`/`health`/`watch` instead.

### Client commands

- `status`: one-shot snapshot print (daemon snapshot payload)
- `health`: basic daemon health info (`last_update` and computed age)
- `watch`: stream timestamps from `state_update` events (prints `last_update` repeatedly)
- `reconnect`: request a reconnect cycle (daemon will rebuild Modbus clients)
- `shutdown`: request daemon shutdown
- `tui`: ncurses UI rendered from live daemon snapshots

Optional socket overrides:

```bash
python client.py --ipc-host 127.0.0.1 --ipc-port 9102 status
```

## How the client talks to the daemon

The daemon exposes a local TCP socket on `--ipc-bind:--ipc-port` using newline-delimited JSON.

Every message is wrapped in an envelope shaped like:

```json
{"v": 1, "type": "<command_or_event>", "payload": {...}}
```

Commands the client can send:
- `ping` -> daemon responds with `pong`
- `get_snapshot` -> responds with `snapshot` (contains `state`)
- `get_health` -> responds with `health`
- `request_reconnect` -> responds with `ack`
- `shutdown` -> responds with `ack` and then stops

Events sent by the daemon:
- `state_update` -> contains a `state` snapshot payload

## Compatibility mode

`monitor.py` remains as a transitional launcher:

```bash
python monitor.py daemon --config monitor_config.json --http
python monitor.py client tui
```

Legacy single-process mode still works when no subcommand is passed.

## Systemd service

This repo includes a daemon-only unit template: `oati-ts-monitor-daemon.service`.

Before enabling it, replace the placeholders in the unit file:
- `WorkingDirectory=/PATH/TO/oati-ts-monitor`
- `ExecStart=/usr/bin/python3 /PATH/TO/oati-ts-monitor/daemon.py`
- `--config /PATH/TO/oati-ts-monitor/monitor_config.json`
- `--log-file /var/log/oati-ts-monitor/monitor.jsonl` (ensure the directory exists and is writable)

Then install and start it:
```bash
sudo cp oati-ts-monitor-daemon.service /etc/systemd/system/oati-ts-monitor-daemon.service
sudo systemctl daemon-reload
sudo systemctl enable --now oati-ts-monitor-daemon.service
```

Watch logs:
```bash
journalctl -u oati-ts-monitor-daemon.service -f
```

## `monitor_config.json` documentation

The daemon/client use a single JSON config file (default: `monitor_config.json`). Key top-level sections:

- `pymodbus`
  - `port` (int): Modbus TCP port (default `502`)
  - `timeout` (number): TCP timeout seconds (default `2.5`)
  - `unitCandidates` (int array): Modbus unit IDs to probe (default `1,255,0`)
  - `coilsFallback` (boolean): if `true`, fall back to Modbus coils (FC1) when discrete inputs fail (FC2)
- `gnss` (optional)
  - `host` (string): GNSS TCP host
  - `port` (int): GNSS TCP port (default examples use `2947`)
- `wifi` (optional)
  - `interface` (string): interface name for `iw dev <interface> link`
  - `refreshSeconds` (number): Wi-Fi refresh interval
- `display`
  - `latencyHosts` (array): extra TCP latency probes displayed in the TUI and dashboard
    - each entry: `{ "name": "<label>", "host": "<ip>", "port": <int> }`
    - `port` is optional; if omitted it uses the Modbus `pymodbus.port`
- `vehicles`
  - array of vehicles, each with:
    - `name` (string)
    - `controllers` (array): Modbus devices for that vehicle
      - `name` (string): controller label (used as a key in UI/state)
      - `host` (string): controller IP/hostname
      - `base` (int): Modbus “base” used to convert `ref` to a displayed DI number
        - the UI displays `DI{ref-base}`
      - `model` (optional string): shown in UI
      - `points` (array of discrete inputs to monitor)
        - each point: `{ "ref": <int>, "label": "<text>", "invert": <bool>, "style": "<optional>" }`
        - `ref` is the Modbus address reference (absolute ref); the code reads from `ref` using the controller `base`
        - `invert` flips the boolean value (ON/OFF)
        - `style` controls the color mapping (example styles: `handbrake`, `seatbelt`, `engine`, `indicator`)
      - `gear_points` (optional object): `{ "<ref>": "<label>" }`
        - these refs are treated as extra boolean states for “gear selection” logic
      - `extra_points` (optional object): `{ "<ref>": "<label>" }`
        - additional boolean states displayed as-is

## Which “sensors” are monitored

The daemon maintains a live `state` snapshot and rolling history (charts) for:

1. **Modbus boolean inputs (primary “sensors”)**
   - For each controller, every `points[]` entry is polled as a boolean.
   - The daemon primarily reads Modbus **Discrete Inputs (FC2)**.
   - If `pymodbus.coilsFallback` is enabled, it will fall back to **Coils (FC1)** when probing fails.
   - `invert` flips each point’s boolean state.
   - `gear_points` and `extra_points` are also polled as boolean refs and shown in the UI/log.

2. **TCP latency probes**
   - For each entry in `display.latencyHosts`, the daemon measures TCP connect latency and status (`OK` vs TCP failure).
   - These appear in the dashboard/TUI under “Latency checks” and contribute to rolling history.

3. **GNSS (optional)**
   - If `gnss.host` and `gnss.port` are set, the daemon connects via TCP and parses:
     - `$...GGA` for fix quality/sats/HDOP and latitude/longitude
     - `$...GSA` for fix type hint
   - The daemon stores a snapshot including fix label, HDOP, satellites, coordinates, and the age timestamp.

4. **Wi‑Fi link stats (optional)**
   - If `wifi.interface` is set, the daemon runs `iw dev <interface> link` periodically.
   - It parses signal level and bitrate to produce fields like `signal_dbm`, `tx_rate_mbps`, `rx_rate_mbps`.
   - These values are also tracked in the rolling history for charting.

5. **CPU load**
   - The daemon samples `os.getloadavg()` (Linux/Unix-style load average) and tracks the 1-minute load value in history.