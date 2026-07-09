import argparse
import json
import socket
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

from ipc_protocol import decode_message, encode_message


def _startup_indicator_blink(poller: Any) -> None:
    """
    Startup indicator for service mode:
    MB_IO_1 coils (0,1) toggle sequence 10 -> 11 -> 01 -> 00, repeated 3 times.
    """

    def _get_client():
        try:
            clients = getattr(poller, "clients", None) or {}
            return clients.get("MB_IO_1")
        except Exception:
            return None

    client = _get_client()
    if client is None:
        return

    # Visible blink cadence; keep short to avoid slowing startup.
    step_sleep_sec = 0.25
    max_total_sec = 15.0

    seq = [(True, False), (True, True), (False, True), (False, False)]

    # Wait for the network switch to bring MB_IO_1 online after service start.
    time.sleep(15.0)
    start = time.monotonic()

    for _ in range(3):
        for c0, c1 in seq:
            if time.monotonic() - start > max_total_sec:
                return
            ok0 = client.write_coil(0, c0)
            ok1 = client.write_coil(1, c1)
            # If Modbus isn't reachable yet, don't spin; wait a bit and keep going.
            if not (ok0 and ok1):
                time.sleep(1.0)
            else:
                time.sleep(step_sleep_sec)

    # Ensure off at the end.
    try:
        client.write_coil(0, False)
        client.write_coil(1, False)
    except Exception:
        pass


class SocketIpcServer:
    def __init__(self, state: Any, poller: Any, host: str, port: int):
        self.state = state
        self.poller = poller
        self.host = host
        self.port = port
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.sock: Optional[socket.socket] = None
        self.clients_lock = threading.Lock()
        self.clients: Set[socket.socket] = set()
        self.broadcast_thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self.thread = threading.Thread(target=self._accept_loop, daemon=True)
        self.thread.start()
        self.broadcast_thread = threading.Thread(target=self._broadcast_loop, daemon=True)
        self.broadcast_thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.state.update_event.set()
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
        with self.clients_lock:
            for c in list(self.clients):
                try:
                    c.close()
                except Exception:
                    pass
            self.clients.clear()
        if self.thread:
            self.thread.join(timeout=2)
        if self.broadcast_thread:
            self.broadcast_thread.join(timeout=2)

    def _accept_loop(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(16)
        sock.settimeout(0.5)
        self.sock = sock
        while not self.stop_event.is_set():
            try:
                conn, _addr = sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn.settimeout(0.5)
            with self.clients_lock:
                self.clients.add(conn)
            t = threading.Thread(target=self._client_loop, args=(conn,), daemon=True)
            t.start()

    def _send(self, conn: socket.socket, msg_type: str, payload: Dict[str, Any]) -> bool:
        try:
            conn.sendall(encode_message(msg_type, payload))
            return True
        except Exception:
            return False

    def _client_loop(self, conn: socket.socket) -> None:
        fh = conn.makefile("r", encoding="utf-8", newline="\n")
        try:
            self._send(conn, "hello", {"status": "ok"})
            while not self.stop_event.is_set():
                try:
                    line = fh.readline()
                except socket.timeout:
                    continue
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = decode_message(line)
                    resp_type, payload = self._handle_command(msg.get("type"), msg.get("payload", {}))
                    if not self._send(conn, resp_type, payload):
                        break
                except Exception as e:
                    if not self._send(conn, "error", {"error": str(e)}):
                        break
        finally:
            with self.clients_lock:
                self.clients.discard(conn)
            try:
                conn.close()
            except Exception:
                pass

    def _handle_command(self, command: str, payload: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        if command == "ping":
            return "pong", {"ts": time.time()}
        if command == "get_snapshot":
            return "snapshot", {"state": self.state.snapshot()}
        if command == "get_health":
            snap = self.state.snapshot()
            return "health", {
                "status": "ok",
                "last_update": snap.get("last_update"),
                "age_sec": max(0.0, time.time() - float(snap.get("last_update") or 0.0)),
            }
        if command == "request_reconnect":
            self.poller.request_reconnect()
            return "ack", {"ok": True}
        if command == "shutdown":
            self.stop_event.set()
            return "ack", {"ok": True}
        return "error", {"error": f"unknown command: {command}"}

    def _broadcast_loop(self) -> None:
        while not self.stop_event.is_set():
            triggered = self.state.update_event.wait(1.0)
            self.state.update_event.clear()
            if not triggered:
                continue
            payload = {"state": self.state.snapshot()}
            dead = []
            with self.clients_lock:
                for conn in self.clients:
                    ok = self._send(conn, "state_update", payload)
                    if not ok:
                        dead.append(conn)
                for conn in dead:
                    self.clients.discard(conn)
                    try:
                        conn.close()
                    except Exception:
                        pass


def build_daemon_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="oati monitor daemon")
    ap.add_argument("--config", type=str, default="monitor_config.json")
    ap.add_argument("--vehicle", type=str, default=None)
    ap.add_argument("--poll", type=float, default=None, help="Polling interval seconds (default from config)")
    ap.add_argument("--poll-net", type=float, default=None)
    ap.add_argument("--port", type=int, default=502)
    ap.add_argument("--timeout", type=float, default=2.5)
    ap.add_argument("--unit", type=int)
    ap.add_argument("--unit-candidates", type=int, nargs="*", default=[1, 255, 0])
    ap.add_argument("--no-coils", dest="coils", action="store_false")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--http", action="store_true")
    ap.add_argument("--http-bind", type=str, default="0.0.0.0")
    ap.add_argument("--http-port", type=int, default=8080)
    ap.add_argument("--app-log-file", type=str, default=None, help="Application log file (rotating)")
    ap.add_argument("--app-log-level", type=str, default=None, help="Application log level (DEBUG/INFO/WARNING/ERROR)")
    ap.add_argument("--log-file", type=str, default=None)
    ap.add_argument("--log-interval", type=float, default=None)
    ap.add_argument("--full-log", action="store_true", help="Enable full-fidelity always-on JSONL logging (segmented)")
    ap.add_argument("--full-log-dir", type=str, default=None, help="Base directory for full logs (default from config or logs/full)")
    ap.add_argument("--full-log-interval", type=float, default=None, help="Full snapshot interval seconds (default from config)")
    ap.add_argument("--full-log-rotate-bytes", type=int, default=None, help="Rotate full log segments after N bytes")
    ap.add_argument("--upload", action="store_true", help="Enable HTTP upload of compressed full log files")
    ap.add_argument("--upload-url", type=str, default=None, help="HTTP endpoint URL for uploading log files")
    ap.add_argument("--upload-device-id", type=str, default=None, help="Device identifier to include in upload headers")
    ap.add_argument("--upload-chunk-bytes", type=int, default=None, help="Deprecated; full log upload now sends one compressed file at a time")
    ap.add_argument("--upload-state-file", type=str, default=None, help="Path to upload file state JSON")
    ap.add_argument("--ipc-bind", type=str, default="127.0.0.1")
    ap.add_argument("--ipc-port", type=int, default=9102)
    ap.add_argument("--mirror-di", action="store_true", help="Expose collected boolean refs as Modbus/TCP discrete inputs (FC2)")
    ap.add_argument("--mirror-di-bind", type=str, default="0.0.0.0", help="Bind address for DI mirror Modbus/TCP server")
    ap.add_argument("--mirror-di-port", type=int, default=502, help="TCP port for DI mirror Modbus/TCP server (default 502)")
    ap.add_argument("--mirror-di-unit", type=int, default=1, help="Unit id for DI mirror Modbus/TCP server (default 1)")
    return ap


def run_daemon(args: argparse.Namespace) -> int:
    from daemon_services import (
        AppState,
        DataLogger,
        FullFidelityLogger,
        HttpLogUploader,
        IOBridgeEvaluator,
        ModbusDiscreteInputsMirrorServer,
        Poller,
        WebServer,
        build_passthrough_ref_map,
        load_config,
        pick_vehicle,
        setup_app_logging,
        silence_lib_logs,
        write_default_config,
    )

    if args.log_interval is not None and args.log_interval <= 0:
        args.log_interval = None
    silence_lib_logs(args.verbose)
    if not Path(args.config).exists():
        write_default_config(args.config)
        print(f"Created default config at {args.config}")
    appcfg = load_config(args.config)
    if args.poll is None:
        args.poll = appcfg.poll_interval_sec
    if args.poll <= 0:
        raise SystemExit("--poll must be > 0")
    if args.poll_net is None:
        args.poll_net = args.poll
    if args.poll_net <= 0:
        raise SystemExit("--poll-net must be > 0")
    if args.unit is not None:
        args.unit_candidates = [args.unit]
    if args.port == 502:
        args.port = appcfg.port
    if args.timeout == 2.5:
        args.timeout = appcfg.timeout
    if args.unit_candidates == [1, 255, 0]:
        args.unit_candidates = appcfg.unit_candidates
    if args.coils is True and appcfg.coils_fallback is not None:
        args.coils = appcfg.coils_fallback

    vehicle = pick_vehicle(appcfg, args.vehicle)
    state = AppState(vehicle, appcfg)
    app_log_level = args.app_log_level or appcfg.app_log_level or "WARNING"
    app_log_file = args.app_log_file if args.app_log_file is not None else (appcfg.app_log_file or "logs/app.log")
    setup_app_logging(state, log_file=app_log_file, level=app_log_level)
    bridge = IOBridgeEvaluator(vehicle.bridge_mappings) if getattr(vehicle, "bridge_mappings", None) else None
    poller = Poller(args, appcfg, vehicle, state, bridge=bridge)
    poller.start()
    threading.Thread(target=_startup_indicator_blink, args=(poller,), daemon=True).start()
    datalogger = DataLogger(state, vehicle, args.log_file, args.log_interval)
    datalogger.start()

    # Full-fidelity always-on log (segmented)
    full_enabled = bool(appcfg.full_log_enabled) or bool(getattr(args, "full_log", False))
    full_dir = args.full_log_dir if getattr(args, "full_log_dir", None) else appcfg.full_log_dir
    full_interval = args.full_log_interval if getattr(args, "full_log_interval", None) is not None else appcfg.full_log_snapshot_interval_sec
    full_rotate = args.full_log_rotate_bytes if getattr(args, "full_log_rotate_bytes", None) is not None else appcfg.full_log_rotate_bytes
    full_logger = FullFidelityLogger(
        state,
        enabled=full_enabled,
        base_dir=str(full_dir or "logs/full"),
        snapshot_interval_sec=float(full_interval or 1.0),
        rotate_bytes=int(full_rotate or (5 * 1024 * 1024)),
    )
    full_logger.start()
    # Combine extensive Wi-Fi/roaming logging with the full-fidelity stream so it
    # rotates, compresses, and uploads alongside the rest of the vehicle state.
    poller.attach_full_logger(full_logger)

    # HTTP upload. On startup it compresses and flushes any inactive backlog automatically.
    upload_enabled = bool(appcfg.upload_enabled) or bool(getattr(args, "upload", False))
    upload_url = args.upload_url if getattr(args, "upload_url", None) else appcfg.upload_url
    upload_device_id = args.upload_device_id if getattr(args, "upload_device_id", None) else appcfg.upload_device_id
    upload_chunk_bytes = args.upload_chunk_bytes if getattr(args, "upload_chunk_bytes", None) is not None else appcfg.upload_chunk_bytes
    upload_state_file = args.upload_state_file if getattr(args, "upload_state_file", None) else appcfg.upload_state_file
    uploader = HttpLogUploader(
        enabled=upload_enabled,
        base_dir=str(full_dir or "logs/full"),
        url=upload_url,
        device_id=upload_device_id,
        chunk_bytes=int(upload_chunk_bytes or (256 * 1024)),
        state_file=str(upload_state_file or "logs/upload_state.json"),
        vehicle=vehicle.name,
        vehicle_short=getattr(vehicle, "short_name", None) or "",
        active_segment_getter=full_logger.current_segment_path,
    )
    uploader.start()

    web_server = WebServer(state, args.http_bind, args.http_port, broadcast_interval=args.poll, poller=poller) if args.http else None
    if web_server:
        web_server.start()
    ipc_server = SocketIpcServer(state, poller, args.ipc_bind, args.ipc_port)
    ipc_server.start()
    di_mirror = None
    mirror_di = bool(getattr(args, "mirror_di", False)) or bool(appcfg.passthrough.enabled)
    if mirror_di:
        ref_map = build_passthrough_ref_map(vehicle, appcfg.passthrough.groups)
        if getattr(args, "mirror_di", False):
            di_bind = str(getattr(args, "mirror_di_bind", "0.0.0.0"))
            di_port = int(getattr(args, "mirror_di_port", 502))
            di_unit = int(getattr(args, "mirror_di_unit", 1))
        else:
            di_bind = str(appcfg.passthrough.bind)
            di_port = int(appcfg.passthrough.port)
            di_unit = int(appcfg.passthrough.unit_id)
        di_mirror = ModbusDiscreteInputsMirrorServer(
            state=state,
            bind_host=di_bind,
            port=di_port,
            unit_id=di_unit,
            refresh_sec=float(args.poll),
            ref_map=ref_map if ref_map else None,
            seed_addresses=appcfg.passthrough.groups.values(),
        )
        di_mirror.start()
    print(json.dumps({"daemon": "started", "ipc": f"{args.ipc_bind}:{args.ipc_port}", "vehicle": vehicle.name, "vehicle_short": getattr(vehicle, "short_name", None)}))

    try:
        while not ipc_server.stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        ipc_server.stop()
        if di_mirror:
            di_mirror.stop()
        if web_server:
            web_server.stop()
        uploader.stop()
        full_logger.stop()
        datalogger.stop()
        poller.stop()
    return 0
