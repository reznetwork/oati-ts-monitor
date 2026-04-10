import json
import tempfile
import time
import unittest
import os
from pathlib import Path
from unittest.mock import patch

from daemon_services import (
    AppCfg,
    AppState,
    DetailedWifiLogger,
    VehicleCfg,
    apply_bssid_transition_timing,
    get_wifi_status,
    parse_iw_scan_candidates,
    parse_station_dump,
)


class TestWifiFeatures(unittest.TestCase):
    def test_parse_station_dump(self):
        output = "\n".join(
            [
                "Station aa:bb:cc:dd:ee:ff (on wlan0)",
                "inactive time: 50 ms",
                "connected time: 14 seconds",
                "rx bytes: 1234",
                "tx bytes: 4321",
                "tx retries: 9",
                "tx failed: 2",
                "signal: -60 dBm",
                "signal avg: -62 dBm",
            ]
        )
        stats = parse_station_dump(output)
        self.assertEqual(stats["inactive_time_ms"], 50.0)
        self.assertEqual(stats["connected_time_ms"], 14000.0)
        self.assertEqual(stats["rx_bytes"], 1234.0)
        self.assertEqual(stats["tx_bytes"], 4321.0)
        self.assertEqual(stats["tx_retries"], 9.0)
        self.assertEqual(stats["tx_failed"], 2.0)
        self.assertEqual(stats["signal_dbm"], -60.0)
        self.assertEqual(stats["signal_avg_dbm"], -62.0)

    def test_parse_iw_scan_candidates(self):
        output = "\n".join(
            [
                "BSS aa:bb:cc:00:00:01(on wlan0)",
                "SSID: mynet",
                "signal: -70.00 dBm",
                "BSS aa:bb:cc:00:00:02(on wlan0)",
                "SSID: mynet",
                "signal: -55.00 dBm",
                "BSS aa:bb:cc:00:00:03(on wlan0)",
                "SSID: other",
                "signal: -50.00 dBm",
            ]
        )
        c = parse_iw_scan_candidates(output, "mynet")
        self.assertEqual(c["candidate_count"], 2.0)
        self.assertEqual(c["top_candidate_rssi"], -55.0)

    @patch("daemon_services.subprocess.run")
    def test_get_wifi_status_parses_extended_fields(self, mock_run):
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "\n".join(
            [
                "Connected to aa:bb:cc:dd:ee:ff (on wlan0)",
                "SSID: test",
                "freq: 2412",
                "signal: -55 dBm noise: -92 dBm",
                "tx bitrate: 72.2 MBit/s",
                "rx bitrate: 65.0 MBit/s",
                "beacon loss count: 3",
                "max probe tries: 7",
            ]
        )
        mock_run.return_value.stderr = ""

        snap = get_wifi_status("wlan0")
        self.assertEqual(snap["bssid"], "aa:bb:cc:dd:ee:ff")
        self.assertEqual(snap["channel"], 1)
        self.assertEqual(snap["beacon_loss_count"], 3)
        self.assertEqual(snap["max_probe_tries"], 7)
        self.assertEqual(snap["rssi_dbm"], -55.0)
        self.assertEqual(snap["noise_dbm"], -92.0)

    def test_apply_bssid_transition_timing(self):
        wifi = {"bssid": "aa"}
        last_bssid, last_seen = apply_bssid_transition_timing(wifi, None, None, 1000)
        self.assertEqual(last_bssid, "aa")
        self.assertEqual(last_seen, 1000)
        self.assertNotIn("bssid_change_ms", wifi)

        wifi2 = {"bssid": "bb"}
        last_bssid, last_seen = apply_bssid_transition_timing(wifi2, last_bssid, last_seen, 1350)
        self.assertEqual(last_bssid, "bb")
        self.assertEqual(last_seen, 1350)
        self.assertEqual(wifi2["bssid_change_ms"], 350)

    def test_detailed_logger_enable_disable(self):
        with tempfile.TemporaryDirectory() as td:
            cwd = os.getcwd()
            try:
                os.chdir(td)
                state = AppState(vehicle=VehicleCfg(name="v", controllers=[]), appcfg=AppCfg())
                logger = DetailedWifiLogger(state, "wifilogs")
                logger.start()
                logger.set_enabled(True)
                logger.enqueue_roaming_event("search", {"line": "search event"}, {"lat": 1.0, "lon": 2.0}, int(time.time() * 1000))
                time.sleep(0.2)
                logger.set_enabled(False)
                logger.stop()

                files = list((Path(td) / "wifilogs").glob("wifi_capture_*.jsonl"))
                self.assertEqual(len(files), 1)
                contents = files[0].read_text(encoding="utf-8").strip().splitlines()
                self.assertGreaterEqual(len(contents), 1)
                parsed = json.loads(contents[0])
                self.assertEqual(parsed["type"], "roaming_event")
                self.assertEqual(parsed["event"], "search")
            finally:
                os.chdir(cwd)

    def test_wifi_history_tracks_bytes(self):
        st = AppState(vehicle=VehicleCfg(name="v", controllers=[]), appcfg=AppCfg())
        st.set_wifi({"signal_dbm": -60.0, "tx_rate_mbps": 1.0, "rx_rate_mbps": 2.0, "tx_bytes": 1000.0, "rx_bytes": 2000.0})
        snap = st.snapshot()
        self.assertTrue(len(snap["history"]["wifi"]["tx_bytes"]) >= 1)
        self.assertTrue(len(snap["history"]["wifi"]["rx_bytes"]) >= 1)

    def test_viewer_parses_gateway_latency(self):
        from wifilog_viewer.viewer import load_wifilog

        payload = {
            "type": "wifi_sample",
            "ts_ms": 123,
            "gnss": {"lat": 1.0, "lon": 2.0},
            "wifi": {"rssi_dbm": -60.0, "channel": 1},
            "gateways": {"gateway": {"latency_ms": 12.5, "status": "OK", "host": "192.168.1.1", "port": "502"}},
        }
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "wifi_capture_test.jsonl"
            p.write_text(json.dumps(payload) + "\n", encoding="utf-8")
            samples, events = load_wifilog(p)
            self.assertEqual(len(events), 0)
            self.assertEqual(len(samples), 1)
            self.assertEqual(samples[0].gateway_latency_ms, 12.5)


if __name__ == "__main__":
    unittest.main()
