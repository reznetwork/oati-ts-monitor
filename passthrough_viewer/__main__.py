from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any, Dict, List, Optional

from daemon_services import load_config, pick_vehicle, silence_lib_logs

from .reader import PassthroughEntry, PassthroughReader, build_vehicle_sources


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="passthrough_viewer",
        description=(
            "Connect to the oati-ts-monitor Modbus/TCP passthrough mirror and display unified "
            "passthrough values (FC02 discrete inputs, with FC01 coils fallback)."
        ),
    )
    ap.add_argument("--config", "-c", default="monitor_config.json", help="Path to monitor_config.json")
    ap.add_argument("--vehicle", "-v", default=None, help="Vehicle name, shortName, or 1-based index (for source labels)")
    ap.add_argument("--host", default="127.0.0.1", help="Modbus/TCP host (daemon passthrough bind target)")
    ap.add_argument("--port", type=int, default=None, help="Modbus/TCP port (default: passthrough.port from config)")
    ap.add_argument("--unit", type=int, default=None, help="Modbus unit id (default: passthrough.unitId from config)")
    ap.add_argument("--timeout", type=float, default=None, help="Socket timeout seconds (default: pymodbus.timeout from config)")
    ap.add_argument("--group", "-g", action="append", default=None, help="Filter to specific passthrough group(s); repeatable")
    ap.add_argument("--watch", "-w", action="store_true", help="Poll continuously")
    ap.add_argument("--interval", type=float, default=1.0, help="Poll interval seconds in watch mode")
    ap.add_argument("--json", action="store_true", help="Emit JSON instead of a table")
    ap.add_argument("--verbose", action="store_true", help="Show pymodbus diagnostics")
    ap.add_argument("--no-coils", action="store_true", help="Disable FC01 coils fallback when FC02 fails")
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
        "vehicle": vehicle_name,
        "error": error,
        "values": rows,
    }


def main(argv: Optional[List[str]] = None) -> int:
    ap = _build_parser()
    args = ap.parse_args(argv)
    silence_lib_logs(args.verbose)

    appcfg = load_config(args.config)
    if not appcfg.passthrough.enabled and args.port is None:
        print(
            "Warning: passthrough.enabled is false in config; ensure the daemon exposes --mirror-di or passthrough.",
            file=sys.stderr,
        )

    port = int(args.port if args.port is not None else appcfg.passthrough.port)
    unit = int(args.unit if args.unit is not None else appcfg.passthrough.unit_id)
    timeout = float(args.timeout if args.timeout is not None else appcfg.timeout)
    groups = dict(appcfg.passthrough.groups)
    if not groups:
        ap.error("No passthrough.groups found in config")

    vehicle = None
    sources: Dict[str, str] = {}
    if args.vehicle is not None:
        vehicle = pick_vehicle(appcfg, args.vehicle)
        sources = build_vehicle_sources(vehicle)
        if not args.group:
            # Avoid querying global passthrough slots this vehicle does not map.
            groups = {name: addr for name, addr in groups.items() if name in sources}
    entries = _build_entries(groups, args.group, sources)

    reader = PassthroughReader(
        args.host,
        port,
        unit,
        timeout,
        coils_fallback=not args.no_coils and appcfg.coils_fallback,
        unit_candidates=appcfg.unit_candidates if args.unit is None else [unit],
        verbose=args.verbose,
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
                vehicle_name=getattr(vehicle, "name", None),
                rows=rows,
                error=reader.last_error if all(r["value"] is None for r in rows) else None,
            )

        if args.watch:
            while True:
                payload = poll_once()
                if args.json:
                    print(json.dumps(payload, ensure_ascii=False))
                else:
                    if payload.get("error"):
                        print(f"read error: {payload['error']}", file=sys.stderr)
                    print(_render_table(payload["values"]))
                    print()
                time.sleep(max(0.1, float(args.interval)))
        else:
            payload = poll_once()
            if payload.get("error") and not payload["values"]:
                ap.error(str(payload["error"]))
            if args.json:
                print(json.dumps(payload, ensure_ascii=False, indent=2))
            else:
                if args.verbose and payload.get("func"):
                    print(f"using unit={payload['unit']} func={payload['func']}", file=sys.stderr)
                print(_render_table(payload["values"]))
    finally:
        reader.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
