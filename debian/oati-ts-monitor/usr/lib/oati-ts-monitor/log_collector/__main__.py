"""
oati-ts-monitor Log Collector – standalone server entry point.

Usage:
  python -m log_collector [options]

Examples:
  python -m log_collector
  python -m log_collector --port 9000 --data-dir /var/lib/oati-collector
  python -m log_collector --bind 0.0.0.0 --port 9000 --data-dir ./received_logs
"""

from __future__ import annotations

import argparse
import logging
import sys

from aiohttp import web

from .server import make_app
from .storage import ChunkStorage


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m log_collector",
        description="oati-ts-monitor log collector - receives and stores compressed NDJSON log files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--bind", default="0.0.0.0", metavar="HOST",
                   help="IP address to bind")
    p.add_argument("--port", type=int, default=9000, metavar="PORT",
                   help="TCP port to listen on")
    p.add_argument("--data-dir", default="received_logs", metavar="DIR",
                   help="Directory for storing received segment files")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   help="Application log level")
    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Silence aiohttp's verbose access log unless DEBUG is requested.
    if args.log_level != "DEBUG":
        logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

    storage = ChunkStorage(args.data_dir)
    app = make_app(storage)

    print(
        f"\n  oati-ts-monitor Log Collector\n"
        f"  ─────────────────────────────\n"
        f"  Ingest endpoint : http://{args.bind}:{args.port}/ingest\n"
        f"  Dashboard       : http://{args.bind}:{args.port}/\n"
        f"  Data directory  : {storage._data_dir}\n",
        flush=True,
    )

    web.run_app(app, host=args.bind, port=args.port, print=None)
    return 0


if __name__ == "__main__":
    sys.exit(main())
