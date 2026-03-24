import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from daemon_services import AppCfg, AppState, DetailedWifiLogger, VehicleCfg, apply_bssid_transition_timing, get_wifi_status


class TestWifiFeatures(unittest.TestCase):
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
            log_path = str(Path(td) / "wifi_detailed.jsonl")
            state = AppState(vehicle=VehicleCfg(name="v", controllers=[]), appcfg=AppCfg())
            logger = DetailedWifiLogger(state, log_path)
            logger.start()
            logger.set_enabled(True)
            logger.enqueue_roaming_event("search", {"line": "search event"}, {"lat": 1.0, "lon": 2.0}, int(time.time() * 1000))
            time.sleep(0.2)
            logger.set_enabled(False)
            logger.stop()
            contents = Path(log_path).read_text(encoding="utf-8").strip().splitlines()
            self.assertGreaterEqual(len(contents), 1)
            parsed = json.loads(contents[0])
            self.assertEqual(parsed["type"], "roaming_event")
            self.assertEqual(parsed["event"], "search")


if __name__ == "__main__":
    unittest.main()
