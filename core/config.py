"""Configuration models and loaders."""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from core.tui_utils import infer_style


DEFAULT_MODBUS_PORT = 502
DEFAULT_TIMEOUT = 2.5
DEFAULT_POLL_INTERVAL_SEC = 1.0
DEFAULT_UNIT_CANDIDATES = [1, 255, 0]
DEFAULT_COILS_FALLBACK = True
MAX_MODBUS_REF_SPAN = 100  # Modbus read 'count' upper bound per request
HISTORY_WINDOW_SEC = 600

@dataclass
class PointCfg:
    ref: int
    label: str
    invert: bool = False
    style: Optional[str] = None
    passthrough: Optional[str] = None


@dataclass
class ControllerCfg:
    name: str
    host: str
    base: int
    points: List[PointCfg] = field(default_factory=list)
    gear_points: Dict[int, str] = field(default_factory=dict)
    extra_points: Dict[int, str] = field(default_factory=dict)
    passthrough_gears: Dict[int, str] = field(default_factory=dict)
    passthrough_extra: Dict[int, str] = field(default_factory=dict)
    model: Optional[str] = None


@dataclass
class PassthroughCfg:
    enabled: bool = False
    bind: str = "0.0.0.0"
    port: int = 502
    unit_id: int = 1
    groups: Dict[str, int] = field(default_factory=dict)


@dataclass
class VehicleCfg:
    name: str
    controllers: List[ControllerCfg]
    short_name: Optional[str] = None
    external_ip: Optional[str] = None
    gnss_host: Optional[str] = None
    gnss_port: Optional[int] = None
    gnss_admin_host: Optional[str] = None
    gnss_admin_port: Optional[int] = None
    bridge_mappings: List[BridgeMappingCfg] = field(default_factory=list)


@dataclass
class BridgeInput:
    controller: str
    ref: int
    source_type: str = "di"
    name: Optional[str] = None


@dataclass
class BridgeOutput:
    controller: str
    address: int


@dataclass
class BridgeMappingCfg:
    name: str
    inputs: List[BridgeInput]
    output: BridgeOutput
    logic: Optional[str] = None
    invert: bool = False
    debounce_ms: int = 0
    on_error: str = "hold"  # hold | force_off | force_on


def derive_short_vehicle_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:48] if len(s) > 48 else s


@dataclass
class AppCfg:
    port: int = DEFAULT_MODBUS_PORT
    timeout: float = DEFAULT_TIMEOUT
    poll_interval_sec: float = DEFAULT_POLL_INTERVAL_SEC
    unit_candidates: List[int] = field(default_factory=lambda: DEFAULT_UNIT_CANDIDATES[:])
    coils_fallback: bool = DEFAULT_COILS_FALLBACK
    gnss_host: Optional[str] = None
    gnss_port: Optional[int] = None
    wifi_iface: Optional[str] = None
    wifi_refresh: Optional[float] = None
    detailed_wifi_log_file: Optional[str] = None
    app_log_file: Optional[str] = None
    app_log_level: Optional[str] = None
    docker_log_container: Optional[str] = None
    # Full-fidelity always-on logging (JSONL)
    full_log_enabled: bool = False
    full_log_dir: str = "logs/full"
    full_log_snapshot_interval_sec: float = 1.0
    full_log_rotate_bytes: int = 5 * 1024 * 1024
    # HTTP upload (compressed whole-file transfer)
    upload_enabled: bool = False
    upload_url: Optional[str] = None
    upload_device_id: Optional[str] = None
    upload_chunk_bytes: int = 256 * 1024
    upload_state_file: str = "logs/upload_state.json"
    display_latency_hosts: List[Tuple[str, str, int]] = field(default_factory=list)
    vehicles: List[VehicleCfg] = field(default_factory=list)
    passthrough: PassthroughCfg = field(default_factory=PassthroughCfg)


def build_passthrough_ref_map(vehicle: VehicleCfg, groups: Dict[str, int]) -> Dict[Tuple[str, int], int]:
    """Map (controller name, source ref) -> unified passthrough DI address."""
    out: Dict[Tuple[str, int], int] = {}
    if not groups:
        return out
    for ctrl in vehicle.controllers:
        cname = ctrl.name
        for p in ctrl.points:
            if not p.passthrough:
                continue
            addr = groups.get(p.passthrough)
            if addr is not None:
                out[(cname, p.ref)] = int(addr)
        for ref, group in ctrl.passthrough_gears.items():
            addr = groups.get(group)
            if addr is not None:
                out[(cname, int(ref))] = int(addr)
        for ref, group in ctrl.passthrough_extra.items():
            addr = groups.get(group)
            if addr is not None:
                out[(cname, int(ref))] = int(addr)
    return out



def _as_int_keys(d: Dict[str, Any]) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for k, v in d.items():
        try:
            out[int(k)] = str(v)
        except Exception:
            continue
    return out


def _as_positive_float(value: Any, default: float) -> float:
    try:
        parsed = float(value)
        if parsed > 0:
            return parsed
    except Exception:
        pass
    return float(default)


def load_config(path: str) -> AppCfg:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    modbus_cfg = raw.get("pymodbus", {}) or {}
    modbus_port = int(modbus_cfg.get("port", DEFAULT_MODBUS_PORT))
    timeout = float(modbus_cfg.get("timeout", DEFAULT_TIMEOUT))
    poll_interval_sec = _as_positive_float(
        modbus_cfg.get("pollIntervalSec", modbus_cfg.get("pollInterval", modbus_cfg.get("pollSeconds"))),
        DEFAULT_POLL_INTERVAL_SEC,
    )
    unit_candidates = list(modbus_cfg.get("unitCandidates", DEFAULT_UNIT_CANDIDATES))
    coils_fallback = bool(modbus_cfg.get("coilsFallback", DEFAULT_COILS_FALLBACK))
    gnss_cfg = raw.get("gnss", {}) or {}
    wifi_cfg = raw.get("wifi", {}) or {}
    display_cfg = raw.get("display", {}) or {}
    latency_hosts_cfg = display_cfg.get("latencyHosts", []) or []
    latency_hosts: List[Tuple[str, str, int]] = []
    for entry in latency_hosts_cfg:
        try:
            name = str(entry.get("name") or entry.get("label") or entry.get("host") or "host")
            host = str(entry["host"])
            host_port = int(entry.get("port", modbus_port))
            latency_hosts.append((name, host, host_port))
        except Exception:
            continue
    vehicles: List[VehicleCfg] = []
    for v in raw.get("vehicles", []):
        v_gnss = v.get("gnss", {}) or {}
        v_gnss_port = v_gnss.get("port")
        v_admin_port = v_gnss.get("adminPort")
        v_short = v.get("shortName") or v.get("short_name") or None
        v_external_ip = v.get("externalIp") or v.get("external_ip") or None
        v_name = str(v.get("name", "vehicle"))
        ctrls: List[ControllerCfg] = []
        for c in v.get("controllers", []):
            points = [
                PointCfg(
                    ref=int(p["ref"]),
                    label=str(p["label"]),
                    invert=bool(p.get("invert", False)),
                    style=p.get("style"),
                    passthrough=str(p["passthrough"]).strip() if p.get("passthrough") else None,
                )
                for p in c.get("points", [])
            ]
            ctrls.append(
                ControllerCfg(
                    name=str(c.get("name", c.get("host", "ctrl"))),
                    host=str(c["host"]),
                    base=int(c["base"]),
                    points=points,
                    gear_points=_as_int_keys(c.get("gear_points", {})),
                    extra_points=_as_int_keys(c.get("extra_points", {})),
                    passthrough_gears=_as_int_keys(c.get("passthrough_gears", {})),
                    passthrough_extra=_as_int_keys(c.get("passthrough_extra", {})),
                    model=c.get("model"),
                )
            )
        bridge_mappings: List[BridgeMappingCfg] = []
        bridge_raw = v.get("bridge", {}) or {}
        for m in (bridge_raw.get("mappings", []) or []):
            try:
                name = str(m.get("name") or "mapping")
                on_error = str(m.get("on_error") or m.get("onError") or "hold").lower()
                invert = bool(m.get("invert", False))
                debounce_ms = int(m.get("debounce_ms") or m.get("debounceMs") or 0)
                logic = m.get("logic")
                if logic is not None:
                    logic = str(logic)

                raw_inputs = None
                if "inputs" in m and m.get("inputs") is not None:
                    raw_inputs = m.get("inputs") or []
                elif "input" in m and m.get("input") is not None:
                    raw_inputs = [m.get("input")]
                else:
                    raw_inputs = []

                inputs: List[BridgeInput] = []
                for idx, inp in enumerate(raw_inputs):
                    if not isinstance(inp, dict):
                        continue
                    inputs.append(
                        BridgeInput(
                            controller=str(inp.get("controller") or inp.get("device") or ""),
                            ref=int(inp.get("ref")),
                            source_type=str(inp.get("source_type") or inp.get("sourceType") or "di"),
                            name=str(inp.get("name")) if inp.get("name") is not None else None,
                        )
                    )
                out_raw = m.get("output") or {}
                output = BridgeOutput(
                    controller=str(out_raw.get("controller") or out_raw.get("device") or ""),
                    address=int(out_raw.get("address")),
                )
                if not inputs or not output.controller:
                    continue
                bridge_mappings.append(
                    BridgeMappingCfg(
                        name=name,
                        inputs=inputs,
                        output=output,
                        logic=logic,
                        invert=invert,
                        debounce_ms=debounce_ms,
                        on_error=on_error,
                    )
                )
            except Exception:
                continue
        vehicles.append(
            VehicleCfg(
                name=v_name,
                controllers=ctrls,
                short_name=str(v_short) if v_short else derive_short_vehicle_name(v_name),
                external_ip=str(v_external_ip) if v_external_ip else None,
                gnss_host=str(v_gnss.get("host")) if v_gnss.get("host") else None,
                gnss_port=int(v_gnss_port) if v_gnss_port is not None else None,
                gnss_admin_host=str(v_gnss.get("adminHost")) if v_gnss.get("adminHost") else None,
                gnss_admin_port=int(v_admin_port) if v_admin_port is not None else None,
                bridge_mappings=bridge_mappings,
            )
        )
    wifi_refresh_raw = wifi_cfg.get("refreshSeconds")
    try:
        wifi_refresh = float(wifi_refresh_raw) if wifi_refresh_raw is not None else None
    except Exception:
        wifi_refresh = None
    if wifi_refresh is not None and wifi_refresh <= 0:
        wifi_refresh = None
    gnss_port = gnss_cfg.get("port")
    logging_cfg = raw.get("logging", {}) or {}
    app_log_file = logging_cfg.get("appLogFile") or logging_cfg.get("app_log_file") or None
    app_log_level = logging_cfg.get("appLogLevel") or logging_cfg.get("app_log_level") or logging_cfg.get("level") or None
    docker_log_container = logging_cfg.get("dockerLogContainer") or logging_cfg.get("docker_log_container") or None
    full_cfg = logging_cfg.get("full") or logging_cfg.get("fullLog") or {}
    if not isinstance(full_cfg, dict):
        full_cfg = {}
    upload_cfg = raw.get("upload", {}) or {}
    if not isinstance(upload_cfg, dict):
        upload_cfg = {}

    def _as_bool(x: Any, default: bool = False) -> bool:
        if x is None:
            return bool(default)
        if isinstance(x, bool):
            return x
        s = str(x).strip().lower()
        if s in ("1", "true", "yes", "y", "on"):
            return True
        if s in ("0", "false", "no", "n", "off"):
            return False
        return bool(default)

    def _as_int(x: Any, default: int) -> int:
        try:
            return int(x)
        except Exception:
            return int(default)

    def _as_float(x: Any, default: float) -> float:
        try:
            return float(x)
        except Exception:
            return float(default)

    pt_raw = raw.get("passthrough", {}) or {}
    pt_groups: Dict[str, int] = {}
    for k, v in (pt_raw.get("groups", {}) or {}).items():
        try:
            pt_groups[str(k)] = int(v)
        except Exception:
            continue

    return AppCfg(
        port=modbus_port,
        timeout=timeout,
        poll_interval_sec=poll_interval_sec,
        unit_candidates=unit_candidates,
        coils_fallback=coils_fallback,
        gnss_host=str(gnss_cfg.get("host")) if gnss_cfg.get("host") else None,
        gnss_port=int(gnss_port) if gnss_port is not None else None,
        wifi_iface=str(wifi_cfg.get("interface")).strip() if wifi_cfg.get("interface") else None,
        wifi_refresh=wifi_refresh,
        detailed_wifi_log_file=str(wifi_cfg.get("detailedLogFile")).strip() if wifi_cfg.get("detailedLogFile") else None,
        app_log_file=str(app_log_file).strip() if app_log_file else None,
        app_log_level=str(app_log_level).strip() if app_log_level else None,
        docker_log_container=str(docker_log_container).strip() if docker_log_container else None,
        full_log_enabled=_as_bool(full_cfg.get("enabled"), False),
        full_log_dir=str(full_cfg.get("dir") or full_cfg.get("baseDir") or "logs/full"),
        full_log_snapshot_interval_sec=_as_float(full_cfg.get("snapshotIntervalSec"), 1.0),
        full_log_rotate_bytes=_as_int(full_cfg.get("rotateBytes"), 5 * 1024 * 1024),
        upload_enabled=_as_bool(upload_cfg.get("enabled"), False),
        upload_url=str(upload_cfg.get("url")).strip() if upload_cfg.get("url") else None,
        upload_device_id=str(upload_cfg.get("deviceId") or upload_cfg.get("device_id")).strip() if (upload_cfg.get("deviceId") or upload_cfg.get("device_id")) else None,
        upload_chunk_bytes=_as_int(upload_cfg.get("chunkBytes") or upload_cfg.get("chunk_bytes"), 256 * 1024),
        upload_state_file=str(upload_cfg.get("stateFile") or upload_cfg.get("state_file") or "logs/upload_state.json"),
        display_latency_hosts=latency_hosts,
        vehicles=vehicles,
        passthrough=PassthroughCfg(
            enabled=_as_bool(pt_raw.get("enabled"), False),
            bind=str(pt_raw.get("bind") or pt_raw.get("host") or "0.0.0.0"),
            port=_as_int(pt_raw.get("port"), 502),
            unit_id=_as_int(pt_raw.get("unitId") or pt_raw.get("unit_id"), 1),
            groups=pt_groups,
        ),
    )

def write_default_config(path: str) -> None:
    default = {
        "pymodbus": {
            "port": DEFAULT_MODBUS_PORT,
            "timeout": DEFAULT_TIMEOUT,
            "pollIntervalSec": DEFAULT_POLL_INTERVAL_SEC,
            "unitCandidates": DEFAULT_UNIT_CANDIDATES,
            "coilsFallback": DEFAULT_COILS_FALLBACK,
        },
        "gnss": {"host": "192.168.1.50", "port": 2947},
        "wifi": {"interface": "wlp2s0", "refreshSeconds": 2, "detailedLogFile": "wifilogs"},
        "logging": {
            "appLogFile": "logs/app.log",
            "appLogLevel": "WARNING",
            "full": {"enabled": True, "dir": "logs/full", "snapshotIntervalSec": 1.0, "rotateBytes": 5242880},
        },
        "upload": {"enabled": False, "url": "http://collector:9000/ingest", "deviceId": "device-001", "chunkBytes": 262144, "stateFile": "logs/upload_state.json"},
        "display": {"latencyHosts": [{"name": "gateway", "host": "192.168.1.1"}]},
        "vehicles": [
            {
                "name": "Tractor A",
                "controllers": [
                    {
                        "name": "MB_IO_1",
                        "model": "ICPDAS ET-7002",
                        "host": "172.16.102.4",
                        "base": 10000,
                        "points": [
                            {"ref": 10001, "label": "Ручник", "invert": False, "style": "handbrake"},
                            {"ref": 10002, "label": "Ремень", "invert": True, "style": "seatbelt"},
                            {"ref": 10003, "label": "Двигатель", "invert": True, "style": "engine"},
                        ],
                    },
                    {
                        "name": "MB_IO_2",
                        "model": "ICPDAS ET-7051",
                        "host": "172.16.102.5",
                        "base": 10000,
                        "points": [
                            {"ref": 10000, "label": "тормоз"},
                            {"ref": 10002, "label": "левый поворотник", "style": "indicator"},
                            {"ref": 10003, "label": "правый поворотник", "style": "indicator"},
                            {"ref": 10005, "label": "ближний свет"},
                            {"ref": 10009, "label": "Гудок"},
                        ],
                    },
                ],
            }
        ],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(default, f, ensure_ascii=False, indent=2)

def pick_vehicle(appcfg: AppCfg, selector: Optional[str]) -> VehicleCfg:
    if not appcfg.vehicles:
        raise SystemExit("No vehicles found in config")
    if selector is None:
        return appcfg.vehicles[0]
    try:
        idx = int(selector)
        if 1 <= idx <= len(appcfg.vehicles):
            return appcfg.vehicles[idx - 1]
    except Exception:
        pass
    for v in appcfg.vehicles:
        if v.name == selector or (v.short_name and v.short_name == selector) or derive_short_vehicle_name(v.name) == selector:
            return v
    raise SystemExit(f"Vehicle '{selector}' not found")
