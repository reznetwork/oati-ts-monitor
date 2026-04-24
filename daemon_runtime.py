import argparse
import json
import socket
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

from ipc_protocol import decode_message, encode_message


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
    ap.add_argument("--poll", type=float, default=1.0)
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
    ap.add_argument("--ipc-bind", type=str, default="127.0.0.1")
    ap.add_argument("--ipc-port", type=int, default=9102)
    return ap


def run_daemon(args: argparse.Namespace) -> int:
    from daemon_services import (
        AppState,
        DataLogger,
        Poller,
        WebServer,
        load_config,
        pick_vehicle,
        setup_app_logging,
        silence_lib_logs,
        write_default_config,
    )

    if args.poll_net is None:
        args.poll_net = args.poll
    if args.log_interval is not None and args.log_interval <= 0:
        args.log_interval = None
    silence_lib_logs(args.verbose)
    if not Path(args.config).exists():
        write_default_config(args.config)
        print(f"Created default config at {args.config}")
    appcfg = load_config(args.config)
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
    poller = Poller(args, appcfg, vehicle, state)
    poller.start()
    datalogger = DataLogger(state, vehicle, args.log_file, args.log_interval)
    datalogger.start()
    web_server = WebServer(state, args.http_bind, args.http_port, broadcast_interval=args.poll, poller=poller) if args.http else None
    if web_server:
        web_server.start()
    ipc_server = SocketIpcServer(state, poller, args.ipc_bind, args.ipc_port)
    ipc_server.start()
    print(json.dumps({"daemon": "started", "ipc": f"{args.ipc_bind}:{args.ipc_port}", "vehicle": vehicle.name, "vehicle_short": getattr(vehicle, "short_name", None)}))

    try:
        while not ipc_server.stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        ipc_server.stop()
        if web_server:
            web_server.stop()
        datalogger.stop()
        poller.stop()
    return 0
