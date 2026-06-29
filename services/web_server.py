"""HTTP dashboard and WebSocket server."""
from __future__ import annotations

import asyncio
import json
import logging
import logging.handlers
import os
import shutil
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import jinja2
except ModuleNotFoundError:  # pragma: no cover
    jinja2 = None
try:
    from aiohttp import WSMsgType, web
except ModuleNotFoundError:  # pragma: no cover
    WSMsgType = None
    web = None

from core.state import AppState
from services.polling import Poller

TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "templates"

EXTERNAL_LOG_SOURCES: Dict[str, Dict[str, Any]] = {
    "networkmanager": {
        "title": "NetworkManager",
        "subtitle": "Live journal tail (NetworkManager.service)",
        "tail_cmd": ["journalctl", "-u", "NetworkManager", "-n", "50", "--no-pager", "--output=short-iso"],
        "follow_cmd": ["journalctl", "-u", "NetworkManager", "-f", "-n", "0", "--no-pager", "--output=short-iso"],
    },
    "exam-vehicle": {
        "title": "exam-vehicle",
        "subtitle": "Live docker logs (exam-vehicle container)",
        "kind": "docker",
        "container": "exam-vehicle",
    },
    "wlan-watchdog": {
        "title": "wlan0_watchdog",
        "subtitle": "/var/log/wlan_watchdog/wlan_watchdog.log",
        "tail_cmd": ["tail", "-n", "50", "/var/log/wlan_watchdog/wlan_watchdog.log"],
        "follow_cmd": ["tail", "-f", "-n", "0", "/var/log/wlan_watchdog/wlan_watchdog.log"],
    },
}

def _docker_bin() -> str:
    return shutil.which("docker") or "/usr/bin/docker"


def _run_log_command(cmd: List[str], *, timeout: float = 8.0) -> Tuple[List[str], Optional[str]]:
    try:
        res = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            stderr=subprocess.STDOUT,
        )
        output = [ln for ln in (res.stdout or "").splitlines() if ln]
        if res.returncode != 0:
            err = "\n".join(output).strip() or (res.stderr or "").strip() or f"exit {res.returncode}"
            return [], err
        return output, None
    except Exception as e:
        return [], str(e)


def _resolve_docker_container(docker: str, name: str) -> Tuple[Optional[str], Optional[str]]:
    if not shutil.which("docker") and not os.path.isfile(docker):
        return None, "docker command not found (is Docker installed and in PATH?)"
    matches: List[str] = []
    try:
        res = subprocess.run(
            [docker, "ps", "-a", "--filter", f"name={name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=8,
            stderr=subprocess.STDOUT,
        )
        if res.returncode != 0:
            err = (res.stdout or "").strip() or f"docker ps failed (exit {res.returncode})"
            return None, err
        matches = [ln.strip() for ln in (res.stdout or "").splitlines() if ln.strip()]
    except Exception as e:
        return None, str(e)

    if matches:
        for m in matches:
            short = m.split("/")[-1]
            if short == name or m == name:
                return short, None
        if len(matches) == 1:
            return matches[0].split("/")[-1], None
        for m in matches:
            short = m.split("/")[-1]
            if short.endswith(name) or name in short:
                return short, None
        return matches[0].split("/")[-1], None

    _, probe_err = _run_log_command([docker, "logs", "--tail", "1", name], timeout=8)
    if probe_err:
        return None, probe_err or f"No container matching '{name}'"
    return name, None


class WebServer:
    def __init__(self, state: AppState, host: str, port: int, broadcast_interval: float = 1.0, poller: Optional[Poller] = None):
        if jinja2 is None or web is None:
            raise RuntimeError("aiohttp and jinja2 are required for HTTP mode")
        self.state = state
        self.host = host
        self.port = port
        self.broadcast_interval = broadcast_interval
        self.poller = poller
        self.thread: Optional[threading.Thread] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.stop_event = threading.Event()
        self.websockets: set = set()
        self.jinja = jinja2.Environment(loader=jinja2.FileSystemLoader(str(TEMPLATE_DIR)), autoescape=True)
        self.template = self.jinja.get_template("dashboard.html")
        self.combined_template = self.jinja.get_template("combined_dashboard.html")
        self.logs_template = self.jinja.get_template("wifi_logs.html")
        self.app_logs_template = self.jinja.get_template("app_logs.html")
        self.external_logs_template = self.jinja.get_template("external_logs.html")

    async def index(self, _request: web.Request) -> web.Response:
        return web.Response(text=self.template.render(initial_state=json.dumps(self.state.snapshot())), content_type="text/html")

    async def combined_index(self, _request: web.Request) -> web.Response:
        return web.Response(text=self.combined_template.render(initial_state=json.dumps(self.state.snapshot())), content_type="text/html")

    async def app_logs_index(self, _request: web.Request) -> web.Response:
        return web.Response(text=self.app_logs_template.render(), content_type="text/html")

    async def external_logs_index(self, request: web.Request) -> web.Response:
        source = str(request.match_info.get("source") or "").strip().lower()
        spec = EXTERNAL_LOG_SOURCES.get(source)
        if spec is None:
            raise web.HTTPNotFound()
        return web.Response(
            text=self.external_logs_template.render(
                page_title=spec.get("title", source),
                page_subtitle=spec.get("subtitle", ""),
                initial_state=json.dumps({"source": source}),
            ),
            content_type="text/html",
        )

    async def ping(self, _request: web.Request) -> web.Response:
        # Used by other dashboards to check if this vehicle's web UI is reachable.
        return web.Response(
            status=204,
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-store",
            },
        )

    def _wifi_logs_dir(self) -> Path:
        configured = Path(self.state.appcfg.detailed_wifi_log_file).expanduser() if self.state.appcfg.detailed_wifi_log_file else Path("wifilogs")
        return (configured.parent / "wifilogs") if configured.suffix else configured

    def _safe_log_file(self, name: str) -> Optional[Path]:
        if "/" in name or "\\" in name:
            return None
        if not name.endswith(".jsonl"):
            return None
        path = (self._wifi_logs_dir() / name).resolve()
        try:
            path.relative_to(self._wifi_logs_dir().resolve())
        except Exception:
            return None
        return path

    def _safe_generated_map_file(self, name: str) -> Optional[Path]:
        """
        Map HTML is generated from a wifi_capture_*.jsonl and stored under <wifilogs>/out/.
        """
        p = self._safe_log_file(name)
        if p is None:
            return None
        out_dir = (self._wifi_logs_dir() / "out").resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_stem = p.stem.replace(" ", "_")
        out = (out_dir / f"{safe_stem}.map.html").resolve()
        try:
            out.relative_to(out_dir)
        except Exception:
            return None
        return out

    async def wifi_logs_index(self, _request: web.Request) -> web.Response:
        log_dir = self._wifi_logs_dir()
        files = []
        if log_dir.exists():
            for p in sorted(log_dir.glob("*.jsonl"), key=lambda x: x.stat().st_mtime, reverse=True):
                st = p.stat()
                files.append(
                    {
                        "name": p.name,
                        "size": f"{st.st_size} B",
                        "modified": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat(),
                    }
                )
        return web.Response(text=self.logs_template.render(files=files), content_type="text/html")

    async def wifi_logs_view(self, request: web.Request) -> web.Response:
        name = request.match_info.get("name", "")
        p = self._safe_log_file(name)
        if p is None or not p.exists():
            raise web.HTTPNotFound()
        text = p.read_text(encoding="utf-8", errors="replace")
        return web.Response(text=text, content_type="text/plain")

    async def wifi_logs_download(self, request: web.Request) -> web.StreamResponse:
        name = request.match_info.get("name", "")
        p = self._safe_log_file(name)
        if p is None or not p.exists():
            raise web.HTTPNotFound()
        return web.FileResponse(
            path=p,
            headers={"Content-Disposition": f'attachment; filename="{p.name}"'},
        )

    async def wifi_logs_map(self, request: web.Request) -> web.StreamResponse:
        name = request.match_info.get("name", "")
        src = self._safe_log_file(name)
        if src is None or not src.exists():
            raise web.HTTPNotFound()
        out = self._safe_generated_map_file(name)
        if out is None:
            raise web.HTTPNotFound()

        def _parse_int_qs(key: str, default: int) -> int:
            try:
                v = request.query.get(key)
                return int(v) if v is not None else default
            except Exception:
                return default

        def _parse_float_qs(key: str, default: float) -> float:
            try:
                v = request.query.get(key)
                return float(v) if v is not None else default
            except Exception:
                return default

        downsample = max(1, _parse_int_qs("downsample", 1))
        max_jump_m = max(0.0, _parse_float_qs("max_jump_m", 0.0))
        min_db = _parse_float_qs("min_db", -90.0)
        max_db = _parse_float_qs("max_db", -40.0)
        tiles = str(request.query.get("tiles") or "OpenStreetMap")
        force = str(request.query.get("force") or "").lower() in ("1", "true", "yes", "y", "on")

        async def _render_if_needed() -> None:
            if not force and out.exists():
                try:
                    if out.stat().st_mtime >= src.stat().st_mtime:
                        return
                except Exception:
                    pass

            def _do_render():
                try:
                    from wifilog_viewer.viewer import load_wifilog, write_map_html
                except Exception as e:
                    raise RuntimeError(f"wifilog_viewer unavailable: {e}") from e
                samples, events = load_wifilog(src, downsample=downsample, max_jump_m=max_jump_m)
                write_map_html(samples, out, min_db=min_db, max_db=max_db, tiles=tiles, roaming_events=events)

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _do_render)

        try:
            await _render_if_needed()
        except Exception as e:
            return web.Response(
                status=500,
                text=f"Map generation failed: {e}\n\nTip: ensure viewer deps are installed (folium/branca).\n",
                content_type="text/plain",
                headers={"Cache-Control": "no-store"},
            )

        return web.FileResponse(
            path=out,
            headers={"Cache-Control": "no-store"},
        )

    async def websocket_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.websockets.add(ws)
        try:
            await ws.send_json({"type": "state", "data": self.state.snapshot()})
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    try:
                        body = json.loads(msg.data)
                    except Exception:
                        body = {}
                    if body.get("type") == "set_detailed_wifi_logging":
                        enabled = bool(body.get("enabled"))
                        if self.poller:
                            self.poller.set_detailed_wifi_logging(enabled)
                        await ws.send_json({"type": "ack", "command": "set_detailed_wifi_logging", "ok": True, "enabled": enabled})
                    if body.get("type") == "set_modbus_enabled":
                        enabled = bool(body.get("enabled"))
                        if self.poller:
                            self.poller.set_modbus_enabled(enabled)
                        await ws.send_json({"type": "ack", "command": "set_modbus_enabled", "ok": True, "enabled": enabled})
                if msg.type == WSMsgType.ERROR:
                    break
        finally:
            self.websockets.discard(ws)
        return ws

    async def websocket_logs_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        last_seq = 0
        try:
            init = self.state.app_log_tail(50)
            if init:
                last_seq = int(init[-1].get("seq") or 0)
            await ws.send_json({"type": "log_init", "data": {"lines": init}})
            loop = asyncio.get_running_loop()
            while not self.stop_event.is_set():
                triggered = await loop.run_in_executor(None, self.state.app_log_event.wait, 1.0)
                if not triggered:
                    continue
                self.state.app_log_event.clear()
                lines = self.state.app_logs_since(last_seq)
                if not lines:
                    continue
                last_seq = int(lines[-1].get("seq") or last_seq)
                await ws.send_json({"type": "log", "data": {"lines": lines}})
        finally:
            try:
                await ws.close()
            except Exception:
                pass
        return ws

    def _tail_external_log(self, source: str) -> Tuple[List[str], Optional[str]]:
        spec = EXTERNAL_LOG_SOURCES.get(source)
        if spec is None:
            return [], f"Unknown log source: {source}"
        if spec.get("kind") == "docker":
            return [], None
        tail_cmd = spec.get("tail_cmd")
        if not tail_cmd:
            return [], f"No tail command configured for: {source}"
        return _run_log_command(list(tail_cmd))

    def _docker_container_name(self, source: str, spec: Dict[str, Any]) -> str:
        if source == "exam-vehicle" and self.state.appcfg.docker_log_container:
            return self.state.appcfg.docker_log_container
        return str(spec.get("container") or source)

    async def _stream_docker_logs(self, ws: web.WebSocketResponse, source: str, spec: Dict[str, Any]) -> None:
        docker = _docker_bin()
        container = self._docker_container_name(source, spec)
        loop = asyncio.get_running_loop()
        resolved, resolve_err = await loop.run_in_executor(None, _resolve_docker_container, docker, container)
        if resolve_err or not resolved:
            await ws.send_json(
                {
                    "type": "log_init",
                    "data": {"lines": [{"line": f"[error] {resolve_err or f'No container matching {container!r}'}"}]},
                }
            )
            return

        await ws.send_json({"type": "log_init", "data": {"lines": []}})
        cmd = [docker, "logs", "-f", "--tail", "50", resolved]
        while not self.stop_event.is_set():
            proc: Optional[asyncio.subprocess.Process] = None
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                assert proc.stdout is not None
                while not self.stop_event.is_set():
                    line = await proc.stdout.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace").rstrip("\n")
                    if text:
                        await ws.send_json({"type": "log", "data": {"lines": [{"line": text}]}})
                rc = await proc.wait()
                if self.stop_event.is_set():
                    break
                if rc != 0:
                    await ws.send_json(
                        {
                            "type": "log",
                            "data": {"lines": [{"line": f"[docker logs exited {rc}, reconnecting…]"}]},
                        }
                    )
                await asyncio.sleep(2.0)
            except Exception as e:
                try:
                    await ws.send_json({"type": "log", "data": {"lines": [{"line": f"[stream error] {e}"}]}})
                except Exception:
                    pass
                await asyncio.sleep(2.0)
            finally:
                if proc is not None:
                    try:
                        proc.terminate()
                        await asyncio.wait_for(proc.wait(), timeout=2.0)
                    except Exception:
                        try:
                            proc.kill()
                        except Exception:
                            pass

    async def _stream_external_logs(self, ws: web.WebSocketResponse, source: str) -> None:
        spec = EXTERNAL_LOG_SOURCES.get(source)
        if spec is None:
            await ws.send_json(
                {"type": "log_init", "data": {"lines": [{"line": f"[error] Unknown log source: {source}"}]}}
            )
            return

        if spec.get("kind") == "docker":
            await self._stream_docker_logs(ws, source, spec)
            return

        loop = asyncio.get_running_loop()
        init_lines, err = await loop.run_in_executor(None, self._tail_external_log, source)
        init_payload = [{"line": ln} for ln in init_lines]
        if err and not init_lines:
            init_payload = [{"line": f"[error] {err}"}]
        elif err:
            init_payload.append({"line": f"[warning] tail command: {err}"})
        await ws.send_json({"type": "log_init", "data": {"lines": init_payload}})

        follow_cmd = spec.get("follow_cmd")
        if not follow_cmd:
            return

        proc: Optional[asyncio.subprocess.Process] = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *follow_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            assert proc.stdout is not None
            while not self.stop_event.is_set():
                line = await proc.stdout.readline()
                if not line:
                    break
                text = line.decode("utf-8", errors="replace").rstrip("\n")
                if text:
                    await ws.send_json({"type": "log", "data": {"lines": [{"line": text}]}})
        except Exception as e:
            try:
                await ws.send_json({"type": "log", "data": {"lines": [{"line": f"[stream error] {e}"}]}})
            except Exception:
                pass
        finally:
            if proc is not None:
                try:
                    proc.terminate()
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

    async def websocket_external_logs_handler(self, request: web.Request) -> web.WebSocketResponse:
        source = str(request.match_info.get("source") or "").strip().lower()
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        try:
            await self._stream_external_logs(ws, source)
        finally:
            try:
                await ws.close()
            except Exception:
                pass
        return ws

    async def broadcast_snapshot(self):
        if not self.websockets:
            return
        payload = {"type": "state", "data": self.state.snapshot()}
        async def _send_one(ws: web.WebSocketResponse) -> Optional[web.WebSocketResponse]:
            try:
                await asyncio.wait_for(ws.send_json(payload), timeout=1.5)
                return None
            except Exception:
                return ws

        tasks = [_send_one(ws) for ws in list(self.websockets)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, web.WebSocketResponse):
                self.websockets.discard(r)

    async def broadcast_loop(self):
        # Single consumer of update_event alongside IPC broadcast loop.
        loop = asyncio.get_running_loop()
        while not self.stop_event.is_set():
            deadline = loop.time() + self.broadcast_interval
            timeout = max(0.0, deadline - loop.time())
            try:
                triggered = await loop.run_in_executor(None, self.state.update_event.wait, timeout)
            finally:
                self.state.update_event.clear()
            if triggered:
                await self.broadcast_snapshot()

    async def _run_async(self):
        app = web.Application()
        app.add_routes(
            [
                web.get("/", self.index),
                web.get("/combined", self.combined_index),
                web.get("/logs", self.app_logs_index),
                web.get("/logs/{source}", self.external_logs_index),
                web.get("/ping", self.ping),
                web.get("/ws", self.websocket_handler),
                web.get("/wslogs", self.websocket_logs_handler),
                web.get("/wsexternallogs/{source}", self.websocket_external_logs_handler),
                web.get("/wifilogs", self.wifi_logs_index),
                web.get("/wifilogs/view/{name}", self.wifi_logs_view),
                web.get("/wifilogs/download/{name}", self.wifi_logs_download),
                web.get("/wifilogs/map/{name}", self.wifi_logs_map),
            ]
        )
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, self.host, self.port).start()
        broadcast_task = asyncio.create_task(self.broadcast_loop())
        try:
            while not self.stop_event.is_set():
                await asyncio.sleep(0.25)
        finally:
            self.stop_event.set()
            self.state.update_event.set()
            broadcast_task.cancel()
            try:
                await broadcast_task
            except asyncio.CancelledError:
                pass
            await runner.cleanup()

    def start(self):
        self.thread = threading.Thread(target=self._thread_main, daemon=True)
        self.thread.start()

    def _thread_main(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._run_async())
        finally:
            self.loop.close()

    def stop(self):
        self.stop_event.set()
        self.state.update_event.set()
        if self.loop:
            try:
                self.loop.call_soon_threadsafe(lambda: None)
            except Exception:
                pass
        if self.thread:
            self.thread.join(timeout=2)


class _StateTailLogHandler(logging.Handler):
    def __init__(self, state: AppState, level: int = logging.NOTSET):
        super().__init__(level=level)
        self.state = state

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.state.append_app_log(line=msg, level=record.levelname, logger=record.name)
        except Exception:
            # Never let logging break the daemon.
            return


def setup_app_logging(state: AppState, *, log_file: Optional[str], level: str = "INFO") -> None:
    """
    App-wide logging:
    - Rotating log file (for persistence)
    - In-memory ring buffer (for /logs live tail)
    """
    lvl = getattr(logging, str(level or "WARNING").upper(), logging.WARNING)
    root = logging.getLogger()
    root.setLevel(lvl)

    fmt = logging.Formatter("%(asctime)sZ %(levelname)s [%(name)s] %(message)s")

    tail_handler = _StateTailLogHandler(state, level=lvl)
    tail_handler.setFormatter(fmt)
    root.addHandler(tail_handler)

    if log_file:
        try:
            path = Path(str(log_file)).expanduser()
            path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.handlers.RotatingFileHandler(
                filename=str(path),
                maxBytes=5 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
            file_handler.setLevel(lvl)
            file_handler.setFormatter(fmt)
            root.addHandler(file_handler)
        except Exception as e:
            state.append_app_log(line=f"Failed to set up file logging: {e}", level="ERROR", logger="logger")
