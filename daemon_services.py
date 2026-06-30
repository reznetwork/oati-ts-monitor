"""Backward-compatibility re-export facade for daemon services."""
from __future__ import annotations

import subprocess  # noqa: F401 - re-exported for test patches

from core.config import (
    DEFAULT_COILS_FALLBACK,
    DEFAULT_MODBUS_PORT,
    DEFAULT_POLL_INTERVAL_SEC,
    DEFAULT_TIMEOUT,
    DEFAULT_UNIT_CANDIDATES,
    AppCfg,
    BridgeInput,
    BridgeMappingCfg,
    BridgeOutput,
    ControllerCfg,
    PassthroughCfg,
    PointCfg,
    VehicleCfg,
    build_passthrough_ref_map,
    derive_short_vehicle_name,
    load_config,
    pick_vehicle,
    write_default_config,
)
from core.state import AppState
from core.tui_utils import infer_style
from services.io_bridge import IOBridgeEvaluator
from services.logging_svc import DataLogger, FullFidelityLogger, HttpLogUploader, make_full_log_record
from services.modbus import (
    CLIENT_CONNECT_MAX_PER_WINDOW,
    CLIENT_CONNECT_WINDOW_SEC,
    ConnectionGuard,
    MBClient,
    MAX_MODBUS_REF_SPAN,
    call_bits,
    call_write_coil,
    chunk_refs_by_span,
    logical_state,
    refs_to_block,
)
from services.passthrough import ModbusDiscreteInputsMirrorServer, MirrorPassthroughDataBlock
from services.polling import GNSSClient, Poller
from services.system import check_tcp, get_cpu_load, get_soc_temp_c, silence_lib_logs
from services.web_server import WebServer, setup_app_logging
from services.wifi import (
    DetailedWifiLogger,
    RoamingEventWatcher,
    apply_bssid_transition_timing,
    get_wifi_status,
    parse_iw_scan_candidates,
    parse_station_dump,
)

__all__ = [
    "AppState",
    "DataLogger",
    "Poller",
    "WebServer",
    "load_config",
    "pick_vehicle",
    "setup_app_logging",
    "silence_lib_logs",
    "write_default_config",
    "DEFAULT_POLL_INTERVAL_SEC",
    "DEFAULT_MODBUS_PORT",
    "DEFAULT_TIMEOUT",
    "DEFAULT_UNIT_CANDIDATES",
    "DEFAULT_COILS_FALLBACK",
    "FullFidelityLogger",
    "HttpLogUploader",
    "IOBridgeEvaluator",
    "ModbusDiscreteInputsMirrorServer",
    "build_passthrough_ref_map",
    "call_bits",
    "infer_style",
]
