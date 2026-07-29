"""Unit tests for ClickHouse NDJSON parse/ingest helpers (no live ClickHouse required)."""

from __future__ import annotations

import gzip
import json
import tempfile
import unittest

from aiohttp.test_utils import TestClient, TestServer

from log_collector.clickhouse_ingest import (
    gunzip_best_effort,
    iter_ndjson_records,
    parse_segment_records,
)
from log_collector.server import make_app
from log_collector.storage import ChunkStorage


def _gz_lines(records: list[dict]) -> bytes:
    raw = "\n".join(json.dumps(r) for r in records).encode("utf-8") + b"\n"
    return gzip.compress(raw)


class TestClickHouseIngestParse(unittest.TestCase):
    def test_iter_ndjson_from_gzip(self):
        payload = _gz_lines(
            [
                {"v": 1, "ts_ms": 1000, "type": "snapshot", "data": {"ok": True}},
                {"v": 1, "ts_ms": 1001, "type": "event", "data": {"kind": "lifecycle", "payload": {}}},
            ]
        )
        recs = list(iter_ndjson_records(payload))
        self.assertEqual(len(recs), 2)
        self.assertEqual(recs[0]["type"], "snapshot")

    def test_iter_ndjson_utf8_cyrillic_and_null_bytes_do_not_raise(self):
        # Cyrillic vehicle names + a line that would confuse json.loads(bytes) encoding detect.
        records = [
            {
                "v": 1,
                "ts_ms": 1000,
                "type": "snapshot",
                "source": {"vehicle": "Багги Segway", "vehicle_short": "segway_villain"},
                "data": {"ok": True},
            }
        ]
        raw = "\n".join(json.dumps(r, ensure_ascii=False) for r in records).encode("utf-8")
        # Inject a null byte mid-stream in a discarded garbage line (corrupt segment edge case)
        raw = raw + b"\n\x00\x00\x00\x00not-json\n"
        payload = gzip.compress(raw)
        recs = list(iter_ndjson_records(payload))
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["source"]["vehicle"], "Багги Segway")

    def test_truncated_gzip_recovers_partial_data(self):
        full = _gz_lines(
            [
                {"v": 1, "ts_ms": 1, "type": "snapshot", "data": {"n": 1}},
                {"v": 1, "ts_ms": 2, "type": "snapshot", "data": {"n": 2}},
            ]
        )
        # Chop off the gzip footer / tail so decompress raises EOFError.
        truncated = full[:-12]
        raw, was_truncated = gunzip_best_effort(truncated)
        self.assertTrue(was_truncated)
        # May recover zero or some bytes depending on where we cut; must not raise.
        recs = list(iter_ndjson_records(truncated))
        self.assertIsInstance(recs, list)

    def test_parse_snapshot_and_wifi_event(self):
        records = [
            {
                "v": 1,
                "ts_ms": 1_700_000_000_000,
                "source": {"device_id": "dev1", "vehicle": "Buggy", "vehicle_short": "buggy"},
                "type": "snapshot",
                "data": {"vehicle": "Buggy", "controllers": {}},
            },
            {
                "v": 1,
                "ts_ms": 1_700_000_000_500,
                "source": {"device_id": "dev1", "vehicle": "Buggy", "vehicle_short": "buggy"},
                "type": "event",
                "data": {
                    "kind": "wifi_sample",
                    "payload": {
                        "type": "wifi_sample",
                        "ts_ms": 1_700_000_000_500,
                        "gnss": {"lat": 55.1, "lon": 37.2},
                        "wifi": {"rssi_dbm": -61, "bssid": "aa:bb:cc:dd:ee:ff"},
                    },
                },
            },
        ]
        snapshots, events, min_ts, max_ts = parse_segment_records(
            records,
            device_id="dev1",
            vehicle="Buggy",
            vehicle_short="buggy",
            segment="segment_1.jsonl.gz",
        )
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(len(events), 1)
        self.assertEqual(min_ts, 1_700_000_000_000)
        self.assertEqual(max_ts, 1_700_000_000_500)
        self.assertEqual(events[0][5], "wifi_sample")  # kind
        self.assertAlmostEqual(events[0][7], 55.1)  # lat
        self.assertAlmostEqual(events[0][8], 37.2)  # lon
        self.assertAlmostEqual(events[0][9], -61.0)  # rssi
        self.assertEqual(events[0][10], "aa:bb:cc:dd:ee:ff")


class TestViewerRoutes(unittest.IsolatedAsyncioTestCase):
    async def test_viewer_html_served(self):
        with tempfile.TemporaryDirectory() as td:
            app = make_app(ChunkStorage(td))
            async with TestClient(TestServer(app)) as client:
                resp = await client.get("/viewer")
                self.assertEqual(resp.status, 200)
                text = await resp.text()
                self.assertIn("Historical Log Viewer", text)
                self.assertIn("tl-playhead", text)

    async def test_api_vehicles_unavailable_without_clickhouse(self):
        with tempfile.TemporaryDirectory() as td:
            app = make_app(ChunkStorage(td))
            async with TestClient(TestServer(app)) as client:
                resp = await client.get("/api/vehicles")
                self.assertEqual(resp.status, 503)


if __name__ == "__main__":
    unittest.main()
