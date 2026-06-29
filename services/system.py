"""System utilities: TCP checks, CPU/temp, logging setup."""
from __future__ import annotations

import logging
import os
import socket
import time
from pathlib import Path
from typing import Optional, Tuple

def silence_lib_logs(verbose: bool) -> None:
    root_level = logging.WARNING if verbose else logging.ERROR
    logging.basicConfig(level=root_level)
    for name in ("pymodbus", "pymodbus.client", "pymodbus.transaction", "pymodbus.framer", "pymodbus.factory"):
        lg = logging.getLogger(name)
        lg.propagate = False
        try:
            lg.handlers.clear()
        except Exception:
            lg.handlers = []
        lg.addHandler(logging.NullHandler())
        lg.setLevel(logging.CRITICAL if not verbose else logging.ERROR)


def check_tcp(host: str, port: int, timeout: float) -> Tuple[bool, Optional[float], Optional[str]]:
    start = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            end = time.perf_counter()
            return True, (end - start) * 1000.0, None
    except Exception as e:
        if isinstance(e, TimeoutError):
            return False, None, "timeout"
        if isinstance(e, ConnectionRefusedError):
            return False, None, "refused"
        if isinstance(e, OSError) and "No route" in str(e):
            return False, None, "no route"
        return False, None, f"{e.__class__.__name__}: {e}"


def get_cpu_load() -> Optional[float]:
    try:
        load1, _, _ = os.getloadavg()
        cores = os.cpu_count() or 1
        return max(0.0, float(load1) / float(cores) * 100.0)
    except Exception:
        return None


def get_soc_temp_c(sensor_name: str = "soc_thermal-virtual-0") -> Optional[float]:
    base = Path("/sys/class/thermal")
    if not base.exists():
        return None
    try:
        for zone in sorted(base.glob("thermal_zone*")):
            type_file = zone / "type"
            temp_file = zone / "temp"
            if not type_file.exists() or not temp_file.exists():
                continue
            sensor = type_file.read_text(encoding="utf-8", errors="ignore").strip()
            if sensor != sensor_name:
                continue
            raw = temp_file.read_text(encoding="utf-8", errors="ignore").strip()
            val = float(raw)
            if abs(val) > 300:
                val = val / 1000.0
            return val
    except Exception:
        return None
    return None
