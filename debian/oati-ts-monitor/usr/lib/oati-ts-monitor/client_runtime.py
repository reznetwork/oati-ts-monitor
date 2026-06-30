import argparse
try:
    import curses
except ModuleNotFoundError:  # pragma: no cover - Windows without curses
    curses = None
import socket
import time
from typing import Any, Dict, Optional

from ipc_protocol import decode_message, encode_message


def addstr_clip(win, y: int, x: int, text: str, attr: int = 0):
    max_x = curses.COLS - 1
    if y >= curses.LINES:
        return
    if x < 0:
        text = text[-x:]
        x = 0
    if x > max_x:
        return
    if x + len(text) > max_x:
        text = text[: max(0, max_x - x)]
    try:
        win.addstr(y, x, text, attr)
    except Exception:
        pass


SPARK_CHARS = "▁▂▃▄▅▆▇█"


def _sparkline(series, width: int) -> str:
    if width <= 0:
        return ""
    values = [item.get("value") for item in series if isinstance(item, dict) and isinstance(item.get("value"), (int, float))]
    if not values:
        return "no data"
    values = values[-width:]
    min_val = min(values)
    max_val = max(values)
    span = max_val - min_val
    if span == 0:
        return SPARK_CHARS[-1] * len(values)
    chars = []
    for val in values:
        idx = int(round((val - min_val) / span * (len(SPARK_CHARS) - 1)))
        chars.append(SPARK_CHARS[idx])
    return "".join(chars)


def _last_numeric(series) -> Optional[float]:
    for item in reversed(series):
        val = item.get("value") if isinstance(item, dict) else None
        if isinstance(val, (int, float)):
            return val
    return None


def _format_coord(deg: Optional[float], pos: str, neg: str) -> Optional[str]:
    if deg is None:
        return None
    hemi = pos if deg >= 0 else neg
    return f"{abs(deg):.4f} {hemi}"


class Colors:
    GREEN = 1
    RED = 2
    YELLOW = 3
    CYAN = 4
    MAGENTA = 5
    WHITE = 6


STYLE_COLORS = {
    "handbrake": (Colors.RED, Colors.GREEN),
    "seatbelt": (Colors.GREEN, Colors.RED),
    "engine": (Colors.GREEN, Colors.RED),
    "indicator": (Colors.YELLOW, Colors.WHITE),
    "default": (Colors.GREEN, 0),
}


def infer_style(label: str, explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    l = label.lower()
    if "ручник" in l:
        return "handbrake"
    if "ремень" in l:
        return "seatbelt"
    if "двигател" in l:
        return "engine"
    if "поворотник" in l:
        return "indicator"
    return "default"


class SocketIpcClient:
    def __init__(self, host: str, port: int, timeout: float = 2.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self.fh = None

    def connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self.sock.settimeout(self.timeout)
        self.fh = self.sock.makefile("r", encoding="utf-8", newline="\n")
        _ = self.read_message()

    def close(self) -> None:
        if self.fh:
            try:
                self.fh.close()
            except Exception:
                pass
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass

    def send_command(self, cmd: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if self.sock is None:
            raise RuntimeError("not connected")
        self.sock.sendall(encode_message(cmd, payload or {}))
        return self.read_message()

    def read_message(self) -> Dict[str, Any]:
        if not self.fh:
            raise RuntimeError("not connected")
        line = self.fh.readline()
        if not line:
            raise RuntimeError("disconnected")
        return decode_message(line.strip())


class RemoteState:
    def __init__(self, initial: Optional[Dict[str, Any]] = None):
        self._snap = initial or {}

    def update(self, snap: Dict[str, Any]) -> None:
        self._snap = snap

    def snapshot(self) -> Dict[str, Any]:
        return self._snap


class ClientTUI:
    def __init__(self, stdscr, args, state: RemoteState, client: SocketIpcClient):
        if curses is None:
            raise RuntimeError("curses is not available on this platform")
        self.stdscr = stdscr
        self.args = args
        self.state = state
        self.client = client
        self.show_charts = False
        self.show_debug = False
        curses.curs_set(0)
        self.stdscr.nodelay(True)
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(Colors.GREEN, curses.COLOR_GREEN, -1)
        curses.init_pair(Colors.RED, curses.COLOR_RED, -1)
        curses.init_pair(Colors.YELLOW, curses.COLOR_YELLOW, -1)
        curses.init_pair(Colors.CYAN, curses.COLOR_CYAN, -1)
        curses.init_pair(Colors.MAGENTA, curses.COLOR_MAGENTA, -1)
        curses.init_pair(Colors.WHITE, curses.COLOR_WHITE, -1)

    def _draw(self, snap: Dict[str, Any]) -> None:
        self.stdscr.erase()
        row = 0
        ts = snap.get("last_update") or 0.0
        age = time.time() - ts if ts else 0.0
        title = f"Monitor Client — {snap.get('vehicle', 'unknown')} (q quit, r reconnect, c charts)"
        addstr_clip(self.stdscr, row, 0, title, curses.color_pair(Colors.YELLOW) | curses.A_BOLD)
        addstr_clip(self.stdscr, row, max(len(title) + 2, 50), f"Age {age:.1f}s", curses.color_pair(Colors.CYAN))
        row += 1

        ctrls = snap.get("controllers", {}) or {}
        for name, ctrl in ctrls.items():
            st = ctrl.get("status", "—")
            latency_ms = ctrl.get("latency")
            lat_txt = f"{latency_ms:.1f} ms" if isinstance(latency_ms, (int, float)) else "-- ms"
            col = curses.color_pair(Colors.GREEN) if st == "OK" else curses.color_pair(Colors.RED)
            addstr_clip(self.stdscr, row, 0, f"{name}: {st} {ctrl.get('host', '')} {lat_txt}", col)
            row += 1
            points = ctrl.get("points", {}) or {}
            meta = ctrl.get("points_meta", {}) or {}
            for ref, val in points.items():
                info = meta.get(ref, {})
                label = info.get("label", ref)
                style = infer_style(label, info.get("style"))
                on_col, off_col = STYLE_COLORS.get(style, STYLE_COLORS["default"])
                if val is None:
                    txt = "N/A"
                    attr = curses.color_pair(Colors.RED)
                else:
                    txt = "ON " if val else "OFF"
                    attr = curses.color_pair(on_col) | curses.A_BOLD if val else curses.color_pair(off_col)
                addstr_clip(self.stdscr, row, 2, f"{label}: {txt}", attr)
                row += 1

        if self.show_charts:
            history = snap.get("history", {}) or {}
            row += 1
            addstr_clip(self.stdscr, row, 0, "Charts", curses.color_pair(Colors.WHITE) | curses.A_BOLD)
            row += 1
            for name, series in (history.get("modbus_latency", {}) or {}).items():
                last_val = _last_numeric(series)
                last_txt = f"{last_val:.1f}ms" if isinstance(last_val, (int, float)) else "--"
                spark = _sparkline(series, max(10, curses.COLS - 20))
                addstr_clip(self.stdscr, row, 0, f"{name}: {spark} {last_txt}", curses.color_pair(Colors.CYAN))
                row += 1

        gnss = snap.get("gnss")
        if gnss:
            lat_txt = _format_coord(gnss.get("lat"), "N", "S") or "--"
            lon_txt = _format_coord(gnss.get("lon"), "E", "W") or "--"
            sol = gnss.get("solution_level", "--")
            addstr_clip(self.stdscr, row, 0, f"GNSS: {gnss.get('fix', '--')} [{sol}] {lat_txt}, {lon_txt}", curses.color_pair(Colors.CYAN))
            row += 1

        wifi = snap.get("wifi")
        if wifi and isinstance(wifi.get("bssid_change_ms"), (int, float)):
            addstr_clip(self.stdscr, row, 0, f"WiFi BSSID switch: {int(wifi.get('bssid_change_ms'))} ms", curses.color_pair(Colors.MAGENTA))
            row += 1
        self.stdscr.refresh()

    def loop(self) -> None:
        while True:
            ch = self.stdscr.getch()
            if ch == ord("q"):
                break
            if ch == ord("c"):
                self.show_charts = not self.show_charts
            if ch == ord("r"):
                try:
                    self.client.send_command("request_reconnect")
                except Exception:
                    pass
            try:
                msg = self.client.read_message()
                if msg.get("type") == "state_update":
                    self.state.update(msg.get("payload", {}).get("state", {}))
            except Exception:
                pass
            self._draw(self.state.snapshot())
            time.sleep(0.2)


def build_client_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="oati monitor CLI client")
    ap.add_argument("--ipc-host", type=str, default="127.0.0.1")
    ap.add_argument("--ipc-port", type=int, default=9102)
    ap.add_argument("--timeout", type=float, default=2.0)
    sub = ap.add_subparsers(dest="command", required=True)
    sub.add_parser("status", help="Print one snapshot summary")
    sub.add_parser("watch", help="Print live update stream")
    sub.add_parser("tui", help="Run ncurses client UI")
    sub.add_parser("health", help="Query daemon health")
    sub.add_parser("reconnect", help="Request poller reconnect")
    sub.add_parser("shutdown", help="Request daemon shutdown")
    return ap


def run_client(args: argparse.Namespace) -> int:
    client = SocketIpcClient(args.ipc_host, args.ipc_port, args.timeout)
    client.connect()
    try:
        if args.command == "status":
            resp = client.send_command("get_snapshot")
            print(resp.get("payload", {}).get("state", {}))
            return 0
        if args.command == "health":
            resp = client.send_command("get_health")
            print(resp.get("payload", {}))
            return 0
        if args.command == "reconnect":
            resp = client.send_command("request_reconnect")
            print(resp.get("payload", {}))
            return 0
        if args.command == "shutdown":
            resp = client.send_command("shutdown")
            print(resp.get("payload", {}))
            return 0
        if args.command == "watch":
            while True:
                msg = client.read_message()
                if msg.get("type") == "state_update":
                    print(msg.get("payload", {}).get("state", {}).get("last_update"))
            return 0
        if args.command == "tui":
            if curses is None:
                raise RuntimeError("tui command requires curses support")
            snap_msg = client.send_command("get_snapshot")
            state = RemoteState(snap_msg.get("payload", {}).get("state", {}))
            curses.wrapper(lambda stdscr: ClientTUI(stdscr, args, state, client).loop())
            return 0
    finally:
        client.close()
    return 0
