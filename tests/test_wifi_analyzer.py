from __future__ import annotations

import tempfile
import unittest

from aiohttp.test_utils import TestClient, TestServer

from log_collector.auth import AuthManager, hash_password, verify_password
from log_collector.clickhouse_client import ClickHouseConfig, ClickHouseStore
from log_collector.clickhouse_ingest import derive_wifi_quality_rows
from log_collector.metadata_store import DEFAULT_PROFILE, MetadataStore
from log_collector.server import make_app
from log_collector.storage import ChunkStorage
from log_collector.wifi_quality import WiFiQualityService, score_cell


class TestWifiQualityDerivation(unittest.TestCase):
    def test_rollup_query_defines_grouping_aliases(self):
        class Client:
            def __init__(self):
                self.sql = ""

            def command(self, sql, parameters=None):
                self.sql = sql

        client = Client()
        store = ClickHouseStore.__new__(ClickHouseStore)
        store.config = ClickHouseConfig(database="logs")
        store._modern_h3_order = True
        store._require = lambda: client
        store.rebuild_quality_segment_rollup("device", "vehicle", "segment")
        self.assertIn("AS bucket_ms", client.sql)
        self.assertIn("AS h3_13", client.sql)

    def test_quality_query_accepts_dict_geo_points(self):
        """Named ClickHouse tuples deserialize as dicts and used to yield HTTP 400: 0."""
        class Result:
            result_rows = [[
                123,
                [{"lat": 1.0, "lon": 2.0}, {"latitude": 1.1, "longitude": 2.1}],
                {"lat": 1.05, "lon": 2.05},
                ["v1"],
                ["aa:bb:cc:dd:ee:ff"],
                *([1] * 14),
            ]]

        class Client:
            def query(self, sql, parameters=None):
                return Result()

        store = ClickHouseStore.__new__(ClickHouseStore)
        store.config = ClickHouseConfig(database="logs")
        store._modern_h3_geo_order = True
        store._require = lambda: Client()
        cells = store.wifi_quality_cells(
            from_ms=0, to_ms=1, vehicles=[], bssids=[], resolution=9,
        )
        self.assertEqual(cells[0]["center"], [1.05, 2.05])
        self.assertEqual(cells[0]["boundary"], [[1.0, 2.0], [1.1, 2.1]])

    def test_quality_query_can_group_by_bssid_and_returns_centers(self):
        class Result:
            result_rows = [[
                123, [[1.0, 2.0], [1.1, 2.1]], (1.05, 2.05), ["v1"],
                ["aa:bb:cc:dd:ee:ff"], "aa:bb:cc:dd:ee:ff",
                *([1] * 14),
            ]]

        class Client:
            def query(self, sql, parameters=None):
                self.sql = sql
                self.parameters = parameters
                return Result()

        client = Client()
        store = ClickHouseStore.__new__(ClickHouseStore)
        store.config = ClickHouseConfig(database="logs")
        store._modern_h3_geo_order = True
        store._require = lambda: client
        cells = store.wifi_quality_cells(
            from_ms=0, to_ms=1, vehicles=[], bssids=[], resolution=9,
            group_by_bssid=True,
        )
        self.assertIn("GROUP BY cell, grouped_bssid", client.sql)
        self.assertEqual(cells[0]["center"], [1.05, 2.05])
        self.assertEqual(cells[0]["grouped_bssid"], "aa:bb:cc:dd:ee:ff")

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
    def __init__(self, rows=None):
        self.last_query = None
        self.rows = rows or []

    def wifi_quality_cells(self, **kwargs):
        self.last_query = kwargs
        return self.rows

    def discovered_bssids(self):
        return []


def quality_row(bssid, rssi, samples, *, cell="123", center=(1.0, 2.0)):
    return {
        "cell": cell,
        "boundary": [[0.9, 1.9], [1.1, 1.9], [1.0, 2.1]],
        "center": list(center),
        "vehicles": ["v1"],
        "bssids": [bssid],
        "grouped_bssid": bssid,
        "sample_count": samples,
        "rssi_sum": rssi * samples,
        "rssi_count": samples,
        "latency_sum": 0,
        "latency_count": 0,
        "gateway_ok_sum": 0,
        "gateway_ok_count": 0,
        "link_rate_sum": 0,
        "link_rate_count": 0,
        "traffic_rate_sum": 0,
        "traffic_rate_count": 0,
        "beacon_loss_sum": 0,
        "beacon_loss_count": 0,
        "roam_count": 0,
    }


class TestSeamlessSurfaces(unittest.TestCase):
    def make_service(self, directory):
        metadata = MetadataStore(f"{directory}/metadata.sqlite3")
        metadata.sync_discovered([
            {"bssid": "aa:bb:cc:dd:ee:ff", "lat": 1, "lon": 2, "rssi_dbm": -60},
            {"bssid": "11:22:33:44:55:66", "lat": 1, "lon": 2, "rssi_dbm": -80},
        ])
        clickhouse = FakeClickHouse([
            quality_row("aa:bb:cc:dd:ee:ff", -60, 10),
            quality_row("11:22:33:44:55:66", -80, 20),
        ])
        return WiFiQualityService(clickhouse, metadata), clickhouse

    def test_aggregate_best_and_separate_station_surfaces(self):
        with tempfile.TemporaryDirectory() as td:
            service, clickhouse = self.make_service(td)
            aggregate = service.heatmap(
                from_ms=0, to_ms=86_400_000, display="seamless",
                station_mode="aggregate", metric="rssi",
            )
            self.assertTrue(clickhouse.last_query["group_by_bssid"])
            self.assertEqual(len(aggregate["cells"]), 1)
            self.assertAlmostEqual(aggregate["cells"][0]["metrics"]["rssi"]["value"], -73.333, places=3)
            self.assertEqual(aggregate["cells"][0]["sample_count"], 30)

            best = service.heatmap(
                from_ms=0, to_ms=86_400_000, display="seamless",
                station_mode="best", metric="rssi",
            )
            self.assertEqual(best["cells"][0]["bssids"], ["aa:bb:cc:dd:ee:ff"])
            self.assertGreater(best["cells"][0]["metrics"]["rssi"]["score"], 80)

            separate = service.heatmap(
                from_ms=0, to_ms=86_400_000, display="seamless",
                station_mode="separate", metric="rssi",
            )
            self.assertEqual(len(separate["surfaces"]), 2)
            self.assertTrue(all(len(surface["cells"]) == 1 for surface in separate["surfaces"]))
            self.assertEqual(separate["summary"]["samples"], 30)

    def test_polygon_filter_and_option_validation(self):
        with tempfile.TemporaryDirectory() as td:
            service, _ = self.make_service(td)
            result = service.heatmap(
                from_ms=0, to_ms=86_400_000, display="seamless",
                polygon=[[10, 10], [10, 11], [11, 11], [11, 10]],
            )
            self.assertEqual(result["cells"], [])
            with self.assertRaises(ValueError):
                service.heatmap(from_ms=0, to_ms=1, display="raster")
            with self.assertRaises(ValueError):
                service.heatmap(from_ms=0, to_ms=1, value_mode="raw", metric="composite")


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

    def test_profile_presets_h3_resolution(self):
        with tempfile.TemporaryDirectory() as td:
            metadata = MetadataStore(f"{td}/m.db")
            definition = {
                **DEFAULT_PROFILE,
                "name": "Small cells",
                "h3_resolution": 13,
            }
            profile = metadata.save_profile(definition)
            clickhouse = FakeClickHouse()
            service = WiFiQualityService(clickhouse, metadata)
            service.heatmap(
                from_ms=0, to_ms=86_400_000, profile_id=profile["id"], resolution=6
            )
            self.assertEqual(clickhouse.last_query["resolution"], 13)
            with self.assertRaises(ValueError):
                metadata.save_profile({**definition, "name": "Invalid", "h3_resolution": 14})


class TestQualityApiValidation(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_invalid_display_and_geojson(self):
        with tempfile.TemporaryDirectory() as td:
            metadata = MetadataStore(f"{td}/m.db")
            app = make_app(
                ChunkStorage(f"{td}/logs"), clickhouse=FakeClickHouse(), metadata=metadata
            )
            async with TestClient(TestServer(app)) as client:
                response = await client.get(
                    "/api/wifi-quality?from_ms=0&to_ms=1&display=invalid"
                )
                self.assertEqual(response.status, 400)
                response = await client.get(
                    "/api/wifi-quality?from_ms=0&to_ms=1&display=seamless&area=%7B%22type%22%3A%22Point%22%7D"
                )
                self.assertEqual(response.status, 400)
                response = await client.get("/api/wifi-quality", params={
                    "from_ms": "0", "to_ms": "1", "display": "seamless",
                    "area": (
                        '{"type":"Polygon","coordinates":['
                        '[[0,0],[2,0],[2,2],[0,0]],'
                        '[[.5,.5],[1,.5],[.5,1],[.5,.5]]]}'
                    ),
                })
                self.assertEqual(response.status, 400)


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
