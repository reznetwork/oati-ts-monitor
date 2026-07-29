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
import getpass
import logging
import sys

from aiohttp import web

from .clickhouse_client import ClickHouseConfig, try_create_store
from .auth import AuthManager, hash_password
from .clickhouse_ingest import backfill_data_dir
from .config import (
    DEFAULT_CONFIG_PATH,
    explicit_cli_dests,
    load_collector_config,
    merge_cli_overrides,
)
from .server import make_app
from .metadata_store import MetadataStore
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
        help="Ingest outstanding received_logs into ClickHouse in background at startup (default on)",
    )
    p.add_argument(
        "--no-backfill-on-startup",
        action="store_true",
        help="Skip scanning received_logs for outstanding ClickHouse ingest at startup",
    )
    p.add_argument(
        "--backfill",
        action="store_true",
        help="Run backfill synchronously then exit (does not start the HTTP server)",
    )
    p.add_argument(
        "--backfill-and-serve",
        action="store_true",
        help="Deprecated alias: enable ClickHouse + background backfill, then serve",
    )
    p.add_argument(
        "--hash-password",
        action="store_true",
        help="Prompt for an admin password, print its scrypt hash, then exit",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.hash_password:
        first = getpass.getpass("Admin password: ")
        second = getpass.getpass("Confirm password: ")
        if first != second:
            parser.error("passwords do not match")
        print(hash_password(first))
        return 0
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
    metadata = MetadataStore(cfg.metadata_db)
    if cfg.auth_enabled and not cfg.admin_password_hash:
        logging.error("auth.enabled=true requires auth.passwordHash")
        return 1
    auth = AuthManager(
        metadata,
        username=cfg.admin_username if cfg.auth_enabled else "",
        password_hash=cfg.admin_password_hash if cfg.auth_enabled else "",
        session_hours=cfg.session_hours,
        secure_cookie=cfg.secure_cookie,
    )

    # Synchronous one-shot backfill (no HTTP server).
    if args.backfill and not args.backfill_and_serve:
        if ch_store is None:
            logging.error(
                "ClickHouse is required for --backfill (enable in config or pass --clickhouse)"
            )
            return 1
        stats = backfill_data_dir(ch_store, cfg.data_dir)
        logging.info("Backfill complete: %s", stats)
        return 0

    background_backfill = bool(
        ch_store is not None and cfg.clickhouse_backfill_on_startup
    )
    if cfg.clickhouse_enabled and ch_store is not None and not background_backfill:
        logging.info("Startup backfill skipped (clickhouse.backfillOnStartup=false)")

    app = make_app(
        storage,
        max_upload_bytes=cfg.max_upload_bytes,
        clickhouse=ch_store,
        activity_gap_ms=cfg.activity_gap_ms,
        backfill_data_dir_path=cfg.data_dir,
        backfill_on_startup=background_backfill,
        metadata=metadata,
        auth=auth,
        max_analysis_days=cfg.max_analysis_days,
        tile_url=cfg.tile_url,
        tile_attribution=cfg.tile_attribution,
    )

    print(
        f"\n  oati-ts-monitor Log Collector\n"
        f"  ─────────────────────────────\n"
        f"  Config          : {cfg.config_path or '(defaults)'}\n"
        f"  Ingest endpoint : http://{cfg.bind}:{cfg.port}/ingest\n"
        f"  Ops dashboard   : http://{cfg.bind}:{cfg.port}/\n"
        f"  Log viewer      : http://{cfg.bind}:{cfg.port}/viewer\n"
        f"  Data directory  : {storage._data_dir}\n"
        f"  ClickHouse      : {'enabled' if ch_store else 'disabled'}\n"
        f"  Authentication : {'enabled' if auth.enabled else 'disabled'}\n"
        f"  Backfill        : {'background' if background_backfill else 'off'}\n",
        flush=True,
    )

    web.run_app(app, host=cfg.bind, port=cfg.port, print=None)
    return 0


if __name__ == "__main__":
    sys.exit(main())
