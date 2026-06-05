from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .viewer import load_wifilog, summarize_samples, write_map_html


def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="wifilog_viewer",
        description="Generate an interactive HTML map from wifi_capture_*.jsonl with path colored by signal/BSSID/channel/rates and roaming events.",
    )
    ap.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to wifi_capture_*.jsonl",
    )
    ap.add_argument(
        "--output",
        "-o",
        required=True,
        help="Output HTML path (directories will be created).",
    )
    ap.add_argument("--min-db", type=float, default=-90.0, help="Lower bound for colormap (dBm).")
    ap.add_argument("--max-db", type=float, default=-40.0, help="Upper bound for colormap (dBm).")
    ap.add_argument(
        "--downsample",
        type=int,
        default=1,
        help="Keep every Nth sample (>=1). Useful for large logs.",
    )
    ap.add_argument(
        "--max-jump-m",
        type=float,
        default=0.0,
        help="Drop points that jump more than this many meters from the previous kept point (0 disables).",
    )
    ap.add_argument(
        "--tiles",
        type=str,
        default="OpenStreetMap",
        help="Folium tiles provider name.",
    )
    ap.add_argument(
        "--print-summary",
        action="store_true",
        help="Print a short JSON summary to stdout.",
    )
    return ap


def main(argv: list[str] | None = None) -> int:
    ap = _build_parser()
    args = ap.parse_args(argv)

    inp = Path(args.input).expanduser()
    out = Path(args.output).expanduser()

    if not inp.exists():
        ap.error(f"--input not found: {inp}")

    samples, events = load_wifilog(inp, downsample=args.downsample, max_jump_m=args.max_jump_m)
    if args.print_summary:
        print(json.dumps(summarize_samples(samples), ensure_ascii=False, indent=2))

    write_map_html(samples, out, min_db=args.min_db, max_db=args.max_db, tiles=args.tiles, roaming_events=events)
    print(str(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

