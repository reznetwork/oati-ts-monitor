"""
Load log_collector JSON configuration.

CLI flags override file values when explicitly provided.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


DEFAULT_CONFIG_PATH = "log_collector_config.json"


@dataclass
class CollectorConfig:
    bind: str = "0.0.0.0"
    port: int = 9000
    data_dir: str = "received_logs"
    log_level: str = "INFO"
    max_upload_bytes: int = 16 * 1024 * 1024
    clickhouse_enabled: bool = False
    clickhouse_host: str = "127.0.0.1"
    clickhouse_port: int = 8123
    clickhouse_db: str = "oati_logs"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    activity_gap_ms: int = 120_000
    config_path: Optional[str] = None

    @classmethod
    def defaults(cls) -> "CollectorConfig":
        return cls()


def load_config_file(path: str | Path) -> dict[str, Any]:
    path = Path(path).expanduser()
    if not path.is_file():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"Config root must be a JSON object: {path}")
    return data


def config_from_dict(data: dict[str, Any], *, config_path: Optional[str] = None) -> CollectorConfig:
    cfg = CollectorConfig.defaults()
    cfg.config_path = config_path

    if "bind" in data:
        cfg.bind = str(data["bind"])
    if "port" in data:
        cfg.port = int(data["port"])
    if "dataDir" in data or "data_dir" in data:
        cfg.data_dir = str(data.get("dataDir") or data.get("data_dir"))
    if "logLevel" in data or "log_level" in data:
        cfg.log_level = str(data.get("logLevel") or data.get("log_level")).upper()
    if "maxUploadBytes" in data or "max_upload_bytes" in data:
        cfg.max_upload_bytes = int(data.get("maxUploadBytes") or data.get("max_upload_bytes"))

    ch = data.get("clickhouse")
    if isinstance(ch, dict):
        if "enabled" in ch:
            cfg.clickhouse_enabled = bool(ch["enabled"])
        if "host" in ch:
            cfg.clickhouse_host = str(ch["host"])
        if "port" in ch:
            cfg.clickhouse_port = int(ch["port"])
        if "database" in ch or "db" in ch:
            cfg.clickhouse_db = str(ch.get("database") or ch.get("db"))
        if "user" in ch or "username" in ch:
            cfg.clickhouse_user = str(ch.get("user") or ch.get("username"))
        if "password" in ch:
            cfg.clickhouse_password = str(ch["password"])

    viewer = data.get("viewer")
    if isinstance(viewer, dict):
        if "activityGapMs" in viewer or "activity_gap_ms" in viewer:
            cfg.activity_gap_ms = int(viewer.get("activityGapMs") or viewer.get("activity_gap_ms"))

    return cfg


def load_collector_config(path: Optional[str | Path] = None) -> CollectorConfig:
    """
    Load config from path. If path is None, try DEFAULT_CONFIG_PATH;
    if missing, return defaults without error.
    """
    if path is None:
        candidate = Path(DEFAULT_CONFIG_PATH)
        if not candidate.is_file():
            return CollectorConfig.defaults()
        path = candidate
    data = load_config_file(path)
    return config_from_dict(data, config_path=str(Path(path).expanduser().resolve()))


def merge_cli_overrides(cfg: CollectorConfig, args: Any, *, cli_explicit: set[str]) -> CollectorConfig:
    """
    Apply argparse namespace onto cfg for flags that were explicitly set on the CLI.

    `cli_explicit` should contain dest names that appeared on argv (not defaults).
    """
    if "bind" in cli_explicit:
        cfg.bind = args.bind
    if "port" in cli_explicit:
        cfg.port = int(args.port)
    if "data_dir" in cli_explicit:
        cfg.data_dir = args.data_dir
    if "log_level" in cli_explicit:
        cfg.log_level = str(args.log_level).upper()
    if "max_upload_bytes" in cli_explicit:
        cfg.max_upload_bytes = int(args.max_upload_bytes)
    if "clickhouse" in cli_explicit and args.clickhouse:
        cfg.clickhouse_enabled = True
    if "clickhouse_host" in cli_explicit:
        cfg.clickhouse_host = args.clickhouse_host
    if "clickhouse_port" in cli_explicit:
        cfg.clickhouse_port = int(args.clickhouse_port)
    if "clickhouse_db" in cli_explicit:
        cfg.clickhouse_db = args.clickhouse_db
    if "clickhouse_user" in cli_explicit:
        cfg.clickhouse_user = args.clickhouse_user
    if "clickhouse_password" in cli_explicit:
        cfg.clickhouse_password = args.clickhouse_password
    return cfg


def explicit_cli_dests(argv: list[str]) -> set[str]:
    """Map argv option strings to argparse dest names that were provided."""
    mapping = {
        "--bind": "bind",
        "--port": "port",
        "--data-dir": "data_dir",
        "--log-level": "log_level",
        "--max-upload-bytes": "max_upload_bytes",
        "--clickhouse": "clickhouse",
        "--clickhouse-host": "clickhouse_host",
        "--clickhouse-port": "clickhouse_port",
        "--clickhouse-db": "clickhouse_db",
        "--clickhouse-user": "clickhouse_user",
        "--clickhouse-password": "clickhouse_password",
        "--config": "config",
    }
    found: set[str] = set()
    for token in argv:
        key = token.split("=", 1)[0]
        if key in mapping:
            found.add(mapping[key])
    return found
