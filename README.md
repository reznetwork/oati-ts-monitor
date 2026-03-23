# oati-ts-monitor

The project now supports a split runtime:

- `daemon`: polling, HTTP dashboard, and JSONL data logging
- `client`: CLI commands and ncurses TUI over a local TCP socket

## Run daemon

```bash
python daemon.py --config monitor_config.json --http --http-port 8080 --log-file monitor.jsonl
```

By default the daemon control socket listens on `127.0.0.1:9102`.

## Run client

```bash
python client.py status
python client.py health
python client.py watch
python client.py tui
```

Optional socket overrides:

```bash
python client.py --ipc-host 127.0.0.1 --ipc-port 9102 status
```

## Compatibility mode

`monitor.py` remains as a transitional launcher:

```bash
python monitor.py daemon --config monitor_config.json --http
python monitor.py client tui
```

Legacy single-process mode still works when no subcommand is passed.
