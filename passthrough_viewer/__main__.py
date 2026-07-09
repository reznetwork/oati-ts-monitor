from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from daemon_services import load_config, pick_vehicle, silence_lib_logs

from .reader import PassthroughEntry, PassthroughReader, build_vehicle_sources


def _format_vehicle_list(appcfg) -> str:
    lines: List[str] = []
    for i, v in enumerate(getattr(appcfg, "vehicles", []) or [], start=1):
        short = getattr(v, "short_name", None) or ""
        name = getattr(v, "name", None) or ""
        ip = getattr(v, "external_ip", None) or ""
        label = f"{i:>2}) {short}"
        if name and name != short:
            label += f" — {name}"
        if ip:
            label += f" (externalIp={ip})"
        lines.append(label)
    return "\n".join(lines) if lines else "(no vehicles found in config)"


def _resolve_config_path_from_argv(argv: List[str]) -> str:
    # argparse help can be invoked before parsing all args; scan raw argv.
    for i, tok in enumerate(argv):
        if tok in ("-c", "--config"):
            if i + 1 < len(argv):
                return str(argv[i + 1])
    return "monitor_config.json"


def _argv_has_any(argv: List[str], flags: List[str]) -> bool:
    return any(f in argv for f in flags)


def _interactive_select_vehicle(appcfg) -> str:
    vehicles = getattr(appcfg, "vehicles", []) or []
    if not vehicles:
        raise SystemExit("No vehicles found in config")
    print("Available vehicles:")
    print(_format_vehicle_list(appcfg))
    while True:
        try:
            raw = input(f"Select vehicle (1-{len(vehicles)} or name): ").strip()
        except EOFError:
            raw = ""
        if raw == "":
            return "1"
        return raw


class _HelpWithVehicles(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        raw_argv = sys.argv[1:]
        cfg_path = _resolve_config_path_from_argv(raw_argv)
        extra = ""
        try:
            appcfg = load_config(cfg_path)
            extra = "\nAvailable vehicles (from --config):\n" + _format_vehicle_list(appcfg) + "\n"
        except Exception as e:
            extra = f"\n(Unable to load vehicles from config '{cfg_path}': {e})\n"
        parser.print_help()
        sys.stdout.write(extra)
        raise SystemExit(0)


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="passthrough_viewer",
        description=(
            "Connect to the oati-ts-monitor Modbus/TCP passthrough mirror and display unified "
            "passthrough values (FC02 discrete inputs, with FC01 coils fallback)."
        ),
        add_help=False,
    )
    ap.add_argument("-h", "--help", nargs=0, action=_HelpWithVehicles, help="Show this help message and exit")
    ap.add_argument("--config", "-c", default="monitor_config.json", help="Path to monitor_config.json")
    ap.add_argument("--vehicle", "-v", default=None, help="Vehicle name, shortName, or 1-based index (for source labels)")
    ap.add_argument("vehicle_pos", nargs="?", default=None, help="Vehicle selector (name, shortName, or 1-based index)")
    ap.add_argument("--host", default="127.0.0.1", help="Modbus/TCP host (daemon passthrough bind target)")
    ap.add_argument("--port", type=int, default=None, help="Modbus/TCP port (default: passthrough.port from config)")
    ap.add_argument("--unit", type=int, default=None, help="Modbus unit id (default: passthrough.unitId from config)")
    ap.add_argument("--timeout", type=float, default=None, help="Socket timeout seconds (default: pymodbus.timeout from config)")
    ap.add_argument("--group", "-g", action="append", default=None, help="Filter to specific passthrough group(s); repeatable")
    ap.add_argument("--watch", "-w", action="store_true", help="Poll and refresh display until Ctrl+C")
    ap.add_argument("--interval", type=float, default=1.0, help="Seconds between polls in watch mode")
    ap.add_argument("--no-clear", action="store_true", help="In watch mode, append output instead of clearing the screen")
    ap.add_argument("--json", action="store_true", help="Emit JSON instead of a table")
    ap.add_argument("--verbose", action="store_true", help="Show pymodbus diagnostics")
    ap.add_argument("--no-coils", action="store_true", help="Disable FC01 coils fallback when FC02 fails")
    ap.add_argument(
        "--read-mode",
        choices=("multi", "single"),
        default="multi",
        help="multi: one FC02/FC01 read from min to max DI address; single: one read per address (default: multi)",
    )
    return ap


def _build_entries(
    groups: Dict[str, int],
    group_filter: Optional[List[str]],
    sources: Dict[str, str],
) -> List[PassthroughEntry]:
    selected = groups
    if group_filter:
        missing = [g for g in group_filter if g not in groups]
        if missing:
            raise SystemExit(f"Unknown passthrough group(s): {', '.join(missing)}")
        selected = {g: groups[g] for g in group_filter}

    entries = [
        PassthroughEntry(group=group, address=int(addr), source=sources.get(group))
        for group, addr in sorted(selected.items(), key=lambda item: (item[1], item[0]))
    ]
    if not entries:
        raise SystemExit("No passthrough groups configured")
    return entries


def _format_value(value: Optional[bool]) -> str:
    if value is None:
        return "?"
    return "ON" if value else "OFF"


def _render_table(rows: List[Dict[str, Any]]) -> str:
    headers = ("DI", "Group", "Value", "Source")
    body = [
        (
            str(row["address"]),
            row["group"],
            _format_value(row["value"]),
            row.get("source") or "",
        )
        for row in rows
    ]
    widths = [len(h) for h in headers]
    for row in body:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt_line(cells: tuple[str, ...]) -> str:
        return "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(cells))

    lines = [fmt_line(headers), fmt_line(tuple("-" * w for w in widths))]
    lines.extend(fmt_line(row) for row in body)
    return "\n".join(lines)


def _snapshot_payload(
    *,
    host: str,
    port: int,
    unit: int,
    func: Optional[str],
    read_mode: str,
    vehicle_name: Optional[str],
    rows: List[Dict[str, Any]],
    error: Optional[str],
) -> Dict[str, Any]:
    return {
        "ts": time.time(),
        "host": host,
        "port": port,
        "unit": unit,
        "func": func,
        "read_mode": read_mode,
        "vehicle": vehicle_name,
        "error": error,
        "values": rows,
    }


def _clear_screen() -> None:
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()


def watch_until_quit(
    poll_once: Callable[[], Dict[str, Any]],
    *,
    interval: float = 1.0,
    json_output: bool = False,
    clear_screen: bool = True,
) -> None:
    """Poll passthrough values and refresh the display until interrupted."""
    interval = max(0.1, float(interval))
    try:
        while True:
            payload = poll_once()
            if json_output:
                print(json.dumps(payload, ensure_ascii=False), flush=True)
            else:
                if clear_screen:
                    _clear_screen()
                ts = datetime.fromtimestamp(float(payload.get("ts") or time.time())).strftime("%H:%M:%S")
                header = (
                    f"passthrough viewer  {ts}  "
                    f"{payload.get('host')}:{payload.get('port')}  "
                    f"unit={payload.get('unit')}  func={payload.get('func')}  "
                    f"mode={payload.get('read_mode')}"
                )
                if payload.get("vehicle"):
                    header += f"  vehicle={payload['vehicle']}"
                print(header)
                if payload.get("error"):
                    print(f"read error: {payload['error']}", file=sys.stderr)
                print(_render_table(payload["values"]))
                print("\nPress Ctrl+C to quit", flush=True)
            time.sleep(interval)
    except KeyboardInterrupt:
        if not json_output:
            print("\nStopped.", flush=True)


def main(argv: Optional[List[str]] = None) -> int:
    ap = _build_parser()
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    args = ap.parse_args(raw_argv)
    silence_lib_logs(args.verbose)

    appcfg = load_config(args.config)
    if not appcfg.passthrough.enabled and args.port is None:
        print(
            "Warning: passthrough.enabled is false in config; ensure the daemon exposes --mirror-di or passthrough.",
            file=sys.stderr,
        )

    groups = dict(appcfg.passthrough.groups)
    if not groups:
        ap.error("No passthrough.groups found in config")

    # Unify vehicle selector (flag or positional).
    if args.vehicle is None and getattr(args, "vehicle_pos", None) is not None:
        args.vehicle = args.vehicle_pos

    vehicle = None
    sources: Dict[str, str] = {}
    if args.vehicle is None:
        # If no vehicle was provided, prompt interactively.
        args.vehicle = _interactive_select_vehicle(appcfg)

    # If only a vehicle was specified (no host/port/watch/read-mode flags), make startup simpler:
    # - host: from vehicle.externalIp (monitor_config.json)
    # - port: 502
    # - watch: enabled
    # - interval: pymodbus.pollIntervalSec * 2
    # - read-mode: multi
    only_vehicle_mode = not _argv_has_any(
        raw_argv,
        [
            "--host",
            "--port",
            "--unit",
            "--timeout",
            "--group",
            "-g",
            "--watch",
            "-w",
            "--interval",
            "--no-clear",
            "--json",
            "--verbose",
            "--no-coils",
            "--read-mode",
        ],
    )

    vehicle = pick_vehicle(appcfg, args.vehicle)
    sources = build_vehicle_sources(vehicle)

    if only_vehicle_mode:
        if not _argv_has_any(raw_argv, ["--host"]) and getattr(vehicle, "external_ip", None):
            args.host = str(vehicle.external_ip)
        if not _argv_has_any(raw_argv, ["--port"]):
            args.port = 502
        if not _argv_has_any(raw_argv, ["--watch", "-w"]):
            args.watch = True
        if not _argv_has_any(raw_argv, ["--interval"]):
            args.interval = appcfg.poll_interval_sec * 2
        if not _argv_has_any(raw_argv, ["--read-mode"]):
            args.read_mode = "multi"

    if not args.group:
        # Avoid querying global passthrough slots this vehicle does not map.
        groups = {name: addr for name, addr in groups.items() if name in sources}
    entries = _build_entries(groups, args.group, sources)

    port = int(args.port if args.port is not None else appcfg.passthrough.port)
    unit = int(args.unit if args.unit is not None else appcfg.passthrough.unit_id)
    timeout = float(args.timeout if args.timeout is not None else appcfg.timeout)

    reader = PassthroughReader(
        args.host,
        port,
        unit,
        timeout,
        coils_fallback=not args.no_coils and appcfg.coils_fallback,
        unit_candidates=appcfg.unit_candidates if args.unit is None else [unit],
        verbose=args.verbose,
        read_mode=args.read_mode,
    )
    try:
        if not reader.connect():
            ap.error(f"Modbus connect failed to {args.host}:{port} ({reader.last_error})")
        if not reader.probe([e.address for e in entries]):
            hint = ""
            if args.no_coils:
                hint = (
                    " The remote endpoint may expose passthrough as FC01 coils only "
                    "(not the oati-ts-monitor FC02 DI mirror); omit --no-coils to use coils fallback."
                )
            ap.error(
                f"Modbus probe failed on {args.host}:{port} "
                f"(tried units {reader.unit_candidates}, FC02{' + FC01' if reader.coils_fallback else ''}): "
                f"{reader.last_error}.{hint}"
            )
        elif reader.func == "coils":
            print(
                "Note: FC02 discrete inputs are unavailable on this endpoint; reading passthrough via FC01 coils.",
                file=sys.stderr,
            )

        def poll_once() -> Dict[str, Any]:
            measured = reader.read_entries(entries)
            if all(v is None for _, v in measured) and reader.last_error:
                return _snapshot_payload(
                    host=args.host,
                    port=port,
                    unit=reader.unit,
                    func=reader.func,
                    read_mode=reader.read_mode,
                    vehicle_name=getattr(vehicle, "name", None),
                    rows=[],
                    error=reader.last_error,
                )
            rows = [
                {
                    "group": entry.group,
                    "address": entry.address,
                    "value": value,
                    "source": entry.source,
                }
                for entry, value in measured
            ]
            return _snapshot_payload(
                host=args.host,
                port=port,
                unit=reader.unit,
                func=reader.func,
                read_mode=reader.read_mode,
                vehicle_name=getattr(vehicle, "name", None),
                rows=rows,
                error=reader.last_error if all(r["value"] is None for r in rows) else None,
            )

        if args.watch:
            watch_until_quit(
                poll_once,
                interval=args.interval,
                json_output=args.json,
                clear_screen=not args.no_clear,
            )
        else:
            payload = poll_once()
            if payload.get("error") and not payload["values"]:
                ap.error(str(payload["error"]))
            if args.json:
                print(json.dumps(payload, ensure_ascii=False, indent=2))
            else:
                if args.verbose and payload.get("func"):
                    print(
                        f"using unit={payload['unit']} func={payload['func']} read_mode={payload['read_mode']}",
                        file=sys.stderr,
                    )
                print(_render_table(payload["values"]))
    finally:
        reader.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
