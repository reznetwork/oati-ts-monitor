from __future__ import annotations

import tempfile
import unittest

from aiohttp.test_utils import TestClient, TestServer

from log_collector.auth import AuthManager, hash_password, verify_password
from log_collector.clickhouse_ingest import derive_wifi_quality_rows
from log_collector.metadata_store import DEFAULT_PROFILE, MetadataStore
from log_collector.server import make_app
from log_collector.storage import ChunkStorage
from log_collector.wifi_quality import WiFiQualityService, score_cell


class TestWifiQualityDerivation(unittest.TestCase):
    def test_counter_rates_and_reset_handling(self):
        def event(ts, tx, rx):
            import json
            payload = {
                "wifi": {"bssid": "AA:BB:CC:DD:EE:FF", "rssi_dbm": -60,
                         "tx_bytes": tx, "rx_bytes": rx},
                "gnss": {"lat": 55.1, "lon": 37.2},
                "gateways": {"gw": {"latency_ms": 12, "status": "OK"}},
            }
            return ["d", "V", "v", "s", ts, "wifi_sample", json.dumps(payload),
                    55.1, 37.2, -60.0, "AA:BB:CC:DD:EE:FF"]

        rows = derive_wifi_quality_rows([
            event(1000, 1000, 2000), event(3000, 3000, 6000), event(5000, 10, 20)
        ])
        self.assertEqual(rows[0][7], "aa:bb:cc:dd:ee:ff")
        self.assertAlmostEqual(rows[1][13], 0.008)
        self.assertAlmostEqual(rows[1][14], 0.016)
        self.assertIsNone(rows[2][13])
        self.assertEqual(rows[1][16], 1)

    def test_score_renormalizes_missing_metrics(self):
        cell = {
            "sample_count": 100, "rssi_sum": -6000, "rssi_count": 100,
            "latency_sum": 0, "latency_count": 0, "gateway_ok_sum": 0,
            "gateway_ok_count": 0, "link_rate_sum": 0, "link_rate_count": 0,
            "traffic_rate_sum": 0, "traffic_rate_count": 0,
            "beacon_loss_sum": 0, "beacon_loss_count": 0, "roam_count": 0,
        }
        scored = score_cell(cell, DEFAULT_PROFILE)
        self.assertGreater(scored["score"], 80)
        self.assertIsNone(scored["metrics"]["latency"]["score"])
        self.assertGreater(scored["confidence"], 0)


class TestMetadataStore(unittest.TestCase):
    def test_discovery_assignment_merge_and_persistence(self):
        with tempfile.TemporaryDirectory() as td:
            path = f"{td}/metadata.sqlite3"
            store = MetadataStore(path)
            created = store.sync_discovered([
                {"bssid": "aabbccddeeff", "lat": 1, "lon": 2, "rssi_dbm": -70},
                {"bssid": "11:22:33:44:55:66", "lat": 3, "lon": 4, "rssi_dbm": -50},
            ])
            self.assertEqual(created, 2)
            stations = store.list_base_stations()
            first, second = stations[0], stations[1]
            store.assign_bssid(first["bssids"][0]["bssid"], second["id"])
            store.merge_stations(first["id"], second["id"])
            reopened = MetadataStore(path)
            active = reopened.list_base_stations()
            self.assertEqual(len(active), 1)
            self.assertEqual(len(active[0]["bssids"]), 2)

    def test_station_optimistic_update_and_profile_validation(self):
        with tempfile.TemporaryDirectory() as td:
            store = MetadataStore(f"{td}/metadata.sqlite3")
            station = store.create_station("A", 1, 2)
            updated = store.update_station(
                station["id"], {"name": "B", "updated_at": station["updated_at"]}
            )
            self.assertEqual(updated["name"], "B")
            with self.assertRaises(ValueError):
                store.update_station(
                    station["id"], {"name": "stale", "updated_at": station["updated_at"]}
                )
            with self.assertRaises(ValueError):
                store.save_profile({"name": "invalid", "metrics": {}})


class FakeClickHouse:
    def wifi_quality_cells(self, **kwargs):
        return []

    def discovered_bssids(self):
        return []


class TestRangeGuard(unittest.TestCase):
    def test_90_day_warning_is_overridable(self):
        with tempfile.TemporaryDirectory() as td:
            service = WiFiQualityService(FakeClickHouse(), MetadataStore(f"{td}/m.db"), max_days=90)
            with self.assertRaises(OverflowError):
                service.heatmap(from_ms=0, to_ms=91 * 86_400_000)
            result = service.heatmap(
                from_ms=0, to_ms=91 * 86_400_000, allow_large_range=True
            )
            self.assertIn("Large", result["warning"])


class TestAuthentication(unittest.IsolatedAsyncioTestCase):
    async def test_login_session_csrf_and_protected_viewer(self):
        with tempfile.TemporaryDirectory() as td:
            metadata = MetadataStore(f"{td}/m.db")
            encoded = hash_password("correct horse battery staple")
            self.assertTrue(verify_password("correct horse battery staple", encoded))
            auth = AuthManager(metadata, username="admin", password_hash=encoded)
            app = make_app(ChunkStorage(f"{td}/logs"), metadata=metadata, auth=auth)
            async with TestClient(TestServer(app)) as client:
                response = await client.get("/viewer", allow_redirects=False)
                self.assertEqual(response.status, 302)
                response = await client.post(
                    "/login",
                    data={"username": "admin", "password": "correct horse battery staple"},
                    allow_redirects=False,
                )
                self.assertEqual(response.status, 302)
                config = await (await client.get("/api/app-config")).json()
                self.assertTrue(config["authenticated"])
                denied = await client.post("/api/base-stations", json={"name": "x"})
                self.assertEqual(denied.status, 403)
                accepted = await client.post(
                    "/api/base-stations", json={"name": "x"},
                    headers={"X-CSRF-Token": config["csrf_token"]},
                )
                self.assertEqual(accepted.status, 201)

    async def test_metadata_mutations_require_auth_to_be_configured(self):
        with tempfile.TemporaryDirectory() as td:
            metadata = MetadataStore(f"{td}/m.db")
            disabled = AuthManager(metadata, username="", password_hash="")
            app = make_app(ChunkStorage(f"{td}/logs"), metadata=metadata, auth=disabled)
            async with TestClient(TestServer(app)) as client:
                response = await client.post("/api/base-stations", json={"name": "x"})
                self.assertEqual(response.status, 503)


if __name__ == "__main__":
    unittest.main()
