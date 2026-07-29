"""Tests for log_collector JSON config loading."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from log_collector.config import (
    config_from_dict,
    explicit_cli_dests,
    load_collector_config,
    merge_cli_overrides,
)


class TestCollectorConfig(unittest.TestCase):
    def test_config_from_dict(self):
        cfg = config_from_dict(
            {
                "bind": "127.0.0.1",
                "port": 9010,
                "dataDir": "/tmp/logs",
                "logLevel": "debug",
                "maxUploadBytes": 1000,
                "clickhouse": {
                    "enabled": True,
                    "host": "ch.local",
                    "port": 9000,
                    "database": "logs",
                    "user": "u",
                    "password": "p",
                },
                "viewer": {
                    "activityGapMs": 60000,
                    "metadataDb": "/tmp/meta.db",
                    "maxAnalysisDays": 90,
                    "tileUrl": "https://tiles/{z}/{x}/{y}.png",
                },
                "auth": {
                    "enabled": True,
                    "username": "operator",
                    "passwordHash": "scrypt$example",
                    "sessionHours": 8,
                    "secureCookie": True,
                },
            }
        )
        self.assertEqual(cfg.bind, "127.0.0.1")
        self.assertEqual(cfg.port, 9010)
        self.assertEqual(cfg.data_dir, "/tmp/logs")
        self.assertEqual(cfg.log_level, "DEBUG")
        self.assertEqual(cfg.max_upload_bytes, 1000)
        self.assertTrue(cfg.clickhouse_enabled)
        self.assertEqual(cfg.clickhouse_host, "ch.local")
        self.assertEqual(cfg.clickhouse_port, 9000)
        self.assertEqual(cfg.clickhouse_db, "logs")
        self.assertEqual(cfg.clickhouse_user, "u")
        self.assertEqual(cfg.clickhouse_password, "p")
        self.assertEqual(cfg.activity_gap_ms, 60000)
        self.assertEqual(cfg.metadata_db, "/tmp/meta.db")
        self.assertEqual(cfg.max_analysis_days, 90)
        self.assertTrue(cfg.auth_enabled)
        self.assertEqual(cfg.admin_username, "operator")
        self.assertEqual(cfg.session_hours, 8)
        self.assertTrue(cfg.secure_cookie)

    def test_load_from_file(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "cfg.json"
            path.write_text(
                json.dumps({"port": 9555, "clickhouse": {"enabled": True, "host": "h"}}),
                encoding="utf-8",
            )
            cfg = load_collector_config(path)
            self.assertEqual(cfg.port, 9555)
            self.assertTrue(cfg.clickhouse_enabled)
            self.assertEqual(cfg.clickhouse_host, "h")
            self.assertTrue(cfg.config_path.endswith("cfg.json"))

    def test_cli_overrides(self):
        cfg = config_from_dict({"port": 9000, "bind": "0.0.0.0"})

        class Args:
            bind = "10.0.0.1"
            port = 9100
            data_dir = "x"
            log_level = "WARNING"
            max_upload_bytes = 1
            clickhouse = True
            clickhouse_host = "override"
            clickhouse_port = 1
            clickhouse_db = "d"
            clickhouse_user = "u"
            clickhouse_password = "pw"

        explicit = explicit_cli_dests(["--port", "9100", "--clickhouse-host", "override"])
        cfg = merge_cli_overrides(cfg, Args(), cli_explicit=explicit)
        self.assertEqual(cfg.port, 9100)
        self.assertEqual(cfg.bind, "0.0.0.0")  # not overridden
        self.assertEqual(cfg.clickhouse_host, "override")


if __name__ == "__main__":
    unittest.main()
