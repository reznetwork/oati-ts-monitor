"""
oati-ts-monitor Log Collector – standalone server entry point.

Usage:
  python -m log_collector [options]

Examples:
  python -m log_collector
  python -m log_collector --config log_collector_config.json
  python -m log_collector --port 9000 --data-dir /var/lib/oati-collector
  python -m log_collector --clickhouse --backfill
"""

from __future__ import annotations

import argparse
import logging
import sys

from aiohttp import web

from .clickhouse_client import ClickHouseConfig, try_create_store
from .clickhouse_ingest import backfill_data_dir
from .config import (
    DEFAULT_CONFIG_PATH,
    explicit_cli_dests,
    load_collector_config,
    merge_cli_overrides,
)
from .server import make_app
from .storage import ChunkStorage


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m log_collector",
        description="oati-ts-monitor log collector - receives and stores compressed NDJSON log files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--config",
        default=None,
        metavar="PATH",
        help=f"JSON config file (default: {DEFAULT_CONFIG_PATH} if present)",
    )
    p.add_argument("--bind", default="0.0.0.0", metavar="HOST",
                   help="IP address to bind (overrides config)")
    p.add_argument("--port", type=int, default=9000, metavar="PORT",
                   help="TCP port to listen on (overrides config)")
    p.add_argument("--data-dir", default="received_logs", metavar="DIR",
                   help="Directory for storing received segment files (overrides config)")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                   help="Application log level (overrides config)")
    p.add_argument("--max-upload-bytes", type=int, default=16 * 1024 * 1024, metavar="BYTES",
                   help="Maximum accepted POST body size for /ingest (overrides config)")
    p.add_argument("--clickhouse", action="store_true",
                   help="Enable ClickHouse ingest and historical viewer APIs (overrides config)")
    p.add_argument("--clickhouse-host", default="127.0.0.1",
                   help="ClickHouse HTTP host (overrides config)")
    p.add_argument("--clickhouse-port", type=int, default=8123,
                   help="ClickHouse HTTP port (overrides config)")
    p.add_argument("--clickhouse-db", default="oati_logs",
                   help="ClickHouse database name (overrides config)")
    p.add_argument("--clickhouse-user", default="default",
                   help="ClickHouse username (overrides config)")
    p.add_argument("--clickhouse-password", default="",
                   help="ClickHouse password (overrides config)")
    p.add_argument(
        "--backfill-on-startup",
        action="store_true",
        help="Ingest outstanding received_logs into ClickHouse at startup (default from config, on)",
    )
    p.add_argument(
        "--no-backfill-on-startup",
        action="store_true",
        help="Skip scanning received_logs for outstanding ClickHouse ingest at startup",
    )
    p.add_argument("--backfill", action="store_true",
                   help="Backfill existing received_logs into ClickHouse then exit")
    p.add_argument("--backfill-and-serve", action="store_true",
                   help="Backfill existing received_logs then start the server (same as default when ClickHouse is on)")
    return p


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    args = parser.parse_args(argv)
    explicit = explicit_cli_dests(argv)

    try:
        cfg = load_collector_config(args.config)
    except FileNotFoundError as exc:
        logging.basicConfig(level=logging.ERROR)
        logging.error("%s", exc)
        return 1
    except (OSError, ValueError) as exc:
        logging.basicConfig(level=logging.ERROR)
        logging.error("Failed to load config: %s", exc)
        return 1

    cfg = merge_cli_overrides(cfg, args, cli_explicit=explicit)

    if args.backfill or args.backfill_and_serve:
        cfg.clickhouse_enabled = True
        cfg.clickhouse_backfill_on_startup = True

    logging.basicConfig(
        level=getattr(logging, cfg.log_level, logging.INFO),
        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if cfg.log_level != "DEBUG":
        logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

    if cfg.config_path:
        logging.info("Loaded config from %s", cfg.config_path)

    storage = ChunkStorage(cfg.data_dir)
    ch_config = ClickHouseConfig.from_args(
        enabled=cfg.clickhouse_enabled,
        host=cfg.clickhouse_host,
        port=cfg.clickhouse_port,
        database=cfg.clickhouse_db,
        username=cfg.clickhouse_user,
        password=cfg.clickhouse_password,
    )
    ch_store = try_create_store(ch_config)

    do_backfill = bool(
        ch_store is not None
        and (
            args.backfill
            or args.backfill_and_serve
            or cfg.clickhouse_backfill_on_startup
        )
    )
    if do_backfill:
        if ch_store is None:
            logging.error(
                "ClickHouse is required for backfill (enable in config or pass --clickhouse)"
            )
            return 1
        stats = backfill_data_dir(ch_store, cfg.data_dir)
        logging.info("Startup backfill stats: %s", stats)
        if args.backfill and not args.backfill_and_serve:
            return 0
    elif cfg.clickhouse_enabled and ch_store is not None:
        logging.info("Startup backfill skipped (clickhouse.backfillOnStartup=false)")

    app = make_app(
        storage,
        max_upload_bytes=cfg.max_upload_bytes,
        clickhouse=ch_store,
        activity_gap_ms=cfg.activity_gap_ms,
    )

    print(
        f"\n  oati-ts-monitor Log Collector\n"
        f"  ─────────────────────────────\n"
        f"  Config          : {cfg.config_path or '(defaults)'}\n"
        f"  Ingest endpoint : http://{cfg.bind}:{cfg.port}/ingest\n"
        f"  Ops dashboard   : http://{cfg.bind}:{cfg.port}/\n"
        f"  Log viewer      : http://{cfg.bind}:{cfg.port}/viewer\n"
        f"  Data directory  : {storage._data_dir}\n"
        f"  ClickHouse      : {'enabled' if ch_store else 'disabled'}\n",
        flush=True,
    )

    web.run_app(app, host=cfg.bind, port=cfg.port, print=None)
    return 0


if __name__ == "__main__":
    sys.exit(main())
