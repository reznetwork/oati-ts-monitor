import json
import tempfile
import unittest
from pathlib import Path

from daemon_services import DEFAULT_POLL_INTERVAL_SEC, load_config


class TestConfigPolling(unittest.TestCase):
    def test_loads_modbus_poll_interval_from_config(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "monitor_config.json"
            cfg_path.write_text(
                json.dumps(
                    {
                        "pymodbus": {"pollIntervalSec": 0.05},
                        "vehicles": [{"name": "v", "controllers": []}],
                    }
                ),
                encoding="utf-8",
            )

            appcfg = load_config(str(cfg_path))

        self.assertEqual(appcfg.poll_interval_sec, 0.05)

    def test_invalid_modbus_poll_interval_falls_back_to_default(self):
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "monitor_config.json"
            cfg_path.write_text(
                json.dumps(
                    {
                        "pymodbus": {"pollIntervalSec": 0},
                        "vehicles": [{"name": "v", "controllers": []}],
                    }
                ),
                encoding="utf-8",
            )

            appcfg = load_config(str(cfg_path))

        self.assertEqual(appcfg.poll_interval_sec, DEFAULT_POLL_INTERVAL_SEC)


if __name__ == "__main__":
    unittest.main()
