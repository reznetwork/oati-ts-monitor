import asyncio
import gzip
import hashlib
import json
import os
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from aiohttp.test_utils import TestClient, TestServer

from daemon_services import AppCfg, AppState, FullFidelityLogger, HttpLogUploader, VehicleCfg
from http_header_text import decode_http_header_text
from log_collector.server import make_app
from log_collector.storage import ChunkStorage


class _IngestHandler(BaseHTTPRequestHandler):
    received = b""
    headers_seen = {}
    lock = threading.Lock()

    def do_POST(self):  # noqa: N802 (stdlib naming)
        length = int(self.headers.get("Content-Length") or "0")
        body = self.rfile.read(length) if length > 0 else b""
        with _IngestHandler.lock:
            _IngestHandler.received += body
            _IngestHandler.headers_seen = dict(self.headers.items())
        self.send_response(204)
        self.end_headers()

    def log_message(self, _fmt, *_args):  # silence test output
        return


class TestFullLogging(unittest.TestCase):
    def test_full_logger_writes_segment_and_rotates(self):
        with tempfile.TemporaryDirectory() as td:
            base_dir = Path(td) / "full"
            state = AppState(vehicle=VehicleCfg(name="v", controllers=[], short_name="v1"), appcfg=AppCfg())
            logger = FullFidelityLogger(state, enabled=True, base_dir=str(base_dir), snapshot_interval_sec=999.0, rotate_bytes=220)
            logger.start()
            try:
                # Force a few writes and small rotation threshold.
                for i in range(6):
                    logger.enqueue_snapshot({"i": i, "vehicle": "v"}, ts_ms=1710000000000 + i)
                time.sleep(0.3)
            finally:
                logger.stop()

            segs = sorted(base_dir.rglob("segment_*.jsonl"))
            self.assertGreaterEqual(len(segs), 1)
            # Rotation should usually create >1 file with such a small rotate limit.
            self.assertGreaterEqual(len(segs), 2)

            first = segs[0].read_text(encoding="utf-8").splitlines()[0]
            rec = json.loads(first)
            self.assertEqual(rec["v"], 1)
            self.assertIn("ts_ms", rec)
            self.assertEqual(rec["type"], "snapshot")
            self.assertIn("source", rec)

    def test_http_uploader_compresses_posts_file_and_persists_confirmation(self):
        with tempfile.TemporaryDirectory() as td:
            base_dir = Path(td) / "full"
            vehicle_short = "v1"
            day = "20260101"
            seg_dir = base_dir / vehicle_short / day
            seg_dir.mkdir(parents=True, exist_ok=True)
            seg = seg_dir / "segment_1710000000000_0.jsonl"
            payload = b'{"a":1}\n{"a":2}\n{"a":3}\n'
            seg.write_bytes(payload)
            old = time.time() - 10.0
            os.utime(seg, (old, old))

            # Start local HTTP ingest
            _IngestHandler.received = b""
            _IngestHandler.headers_seen = {}
            httpd = HTTPServer(("127.0.0.1", 0), _IngestHandler)
            port = httpd.server_address[1]
            t = threading.Thread(target=httpd.serve_forever, daemon=True)
            t.start()
            try:
                state_file = Path(td) / "upload_state.json"
                uploader = HttpLogUploader(
                    enabled=True,
                    base_dir=str(base_dir),
                    url=f"http://127.0.0.1:{port}/ingest",
                    device_id="dev1",
                    chunk_bytes=10,  # deprecated and ignored by whole-file upload
                    state_file=str(state_file),
                    vehicle="v",
                    vehicle_short=vehicle_short,
                )
                uploader.start()
                deadline = time.time() + 3.0
                while time.time() < deadline:
                    with _IngestHandler.lock:
                        got = bytes(_IngestHandler.received)
                    if got:
                        break
                    time.sleep(0.05)
                uploader.stop()

                with _IngestHandler.lock:
                    got = bytes(_IngestHandler.received)
                    headers = dict(_IngestHandler.headers_seen)
                self.assertEqual(gzip.decompress(got), payload)
                self.assertEqual(headers.get("X-Log-Compression"), "gzip")
                self.assertEqual(headers.get("X-Log-Segment"), "segment_1710000000000_0.jsonl.gz")
                self.assertFalse(seg.exists())
                gz_seg = seg.with_name(seg.name + ".gz")
                self.assertTrue(gz_seg.exists())

                st = json.loads(state_file.read_text(encoding="utf-8"))
                self.assertIn("uploaded", st)
                self.assertIn(str(gz_seg), st["uploaded"])
            finally:
                httpd.shutdown()
                httpd.server_close()

    def test_http_uploader_encodes_unicode_vehicle_header(self):
        with tempfile.TemporaryDirectory() as td:
            base_dir = Path(td) / "full"
            vehicle_short = "segway_villain"
            day = "20260101"
            seg_dir = base_dir / vehicle_short / day
            seg_dir.mkdir(parents=True, exist_ok=True)
            seg = seg_dir / "segment_1710000000000_0.jsonl"
            seg.write_bytes(b'{"a":1}\n')
            old = time.time() - 10.0
            os.utime(seg, (old, old))

            _IngestHandler.received = b""
            _IngestHandler.headers_seen = {}
            httpd = HTTPServer(("127.0.0.1", 0), _IngestHandler)
            port = httpd.server_address[1]
            t = threading.Thread(target=httpd.serve_forever, daemon=True)
            t.start()
            try:
                uploader = HttpLogUploader(
                    enabled=True,
                    base_dir=str(base_dir),
                    url=f"http://127.0.0.1:{port}/ingest",
                    device_id="dev1",
                    chunk_bytes=256 * 1024,
                    state_file=str(Path(td) / "upload_state.json"),
                    vehicle="Багги Segway Villain SSV",
                    vehicle_short=vehicle_short,
                )
                uploader.start()
                deadline = time.time() + 3.0
                while time.time() < deadline:
                    with _IngestHandler.lock:
                        if _IngestHandler.received:
                            break
                    time.sleep(0.05)
                uploader.stop()

                with _IngestHandler.lock:
                    headers = dict(_IngestHandler.headers_seen)
                vehicle_header = headers.get("X-Vehicle", "")
                vehicle_header.encode("latin-1")
                self.assertEqual(decode_http_header_text(vehicle_header), "Багги Segway Villain SSV")
            finally:
                httpd.shutdown()
                httpd.server_close()

    def test_http_uploader_discards_empty_gzip_segment(self):
        with tempfile.TemporaryDirectory() as td:
            base_dir = Path(td) / "full"
            vehicle_short = "v1"
            day = "20260101"
            seg_dir = base_dir / vehicle_short / day
            seg_dir.mkdir(parents=True, exist_ok=True)
            empty_gz = seg_dir / "segment_1710000000000_14.jsonl.gz"
            empty_gz.write_bytes(b"")

            _IngestHandler.received = b""
            httpd = HTTPServer(("127.0.0.1", 0), _IngestHandler)
            t = threading.Thread(target=httpd.serve_forever, daemon=True)
            t.start()
            try:
                uploader = HttpLogUploader(
                    enabled=True,
                    base_dir=str(base_dir),
                    url=f"http://127.0.0.1:{httpd.server_address[1]}/ingest",
                    device_id="dev1",
                    chunk_bytes=256 * 1024,
                    state_file=str(Path(td) / "upload_state.json"),
                    vehicle="v",
                    vehicle_short=vehicle_short,
                )
                uploader.start()
                deadline = time.time() + 2.0
                while time.time() < deadline and empty_gz.exists():
                    time.sleep(0.05)
                uploader.stop()

                self.assertFalse(empty_gz.exists())
                with _IngestHandler.lock:
                    self.assertEqual(_IngestHandler.received, b"")
            finally:
                httpd.shutdown()
                httpd.server_close()

    def test_log_collector_stores_and_reads_compressed_file(self):
        async def _run():
            with tempfile.TemporaryDirectory() as td:
                storage = ChunkStorage(td)
                raw = b'{"a":1}\n{"a":2}\n'
                payload = gzip.compress(raw)
                await storage.ingest_file(
                    device_id="dev1",
                    vehicle="vehicle",
                    vehicle_short="v1",
                    segment="segment_1710000000000_0.jsonl.gz",
                    schema_version=1,
                    file_bytes=len(payload),
                    sha256=hashlib.sha256(payload).hexdigest(),
                    payload=payload,
                )
                stored = Path(td) / "dev1" / "v1" / "segment_1710000000000_0.jsonl.gz"
                self.assertTrue(stored.exists())
                self.assertEqual(gzip.decompress(stored.read_bytes()), raw)
                records = await storage.read_segment_tail("dev1", "v1", "segment_1710000000000_0.jsonl.gz", 10)
                self.assertEqual(records, [{"a": 1}, {"a": 2}])

        asyncio.run(_run())

    def test_log_collector_accepts_empty_payload(self):
        async def _run():
            with tempfile.TemporaryDirectory() as td:
                storage = ChunkStorage(td)
                app = make_app(storage)
                headers = {
                    "Content-Type": "application/gzip",
                    "X-Log-Compression": "gzip",
                    "X-Schema-Version": "1",
                    "X-Device-Id": "dev1",
                    "X-Vehicle": "vehicle",
                    "X-Vehicle-Short": "v1",
                    "X-Log-Segment": "segment_empty_0.jsonl.gz",
                    "X-File-Bytes": "0",
                    "X-File-Sha256": hashlib.sha256(b"").hexdigest(),
                }
                async with TestClient(TestServer(app)) as client:
                    resp = await client.post("/ingest", data=b"", headers=headers)
                    self.assertEqual(resp.status, 200)

                stored = Path(td) / "dev1" / "v1" / "segment_empty_0.jsonl.gz"
                self.assertFalse(stored.exists())
                stats = await storage.stats()
                self.assertEqual(stats["total_files"], 0)
                self.assertEqual(stats["total_bytes"], 0)

        asyncio.run(_run())

    def test_log_collector_accepts_payload_larger_than_aiohttp_default_limit(self):
        async def _run():
            with tempfile.TemporaryDirectory() as td:
                storage = ChunkStorage(td)
                app = make_app(storage)
                payload = gzip.compress(b"x" * (1024 * 1024 + 128))
                headers = {
                    "Content-Type": "application/gzip",
                    "X-Log-Compression": "gzip",
                    "X-Schema-Version": "1",
                    "X-Device-Id": "dev1",
                    "X-Vehicle": "vehicle",
                    "X-Vehicle-Short": "v1",
                    "X-Log-Segment": "segment_large_0.jsonl.gz",
                    "X-File-Bytes": str(len(payload)),
                    "X-File-Sha256": hashlib.sha256(payload).hexdigest(),
                }
                async with TestClient(TestServer(app)) as client:
                    resp = await client.post("/ingest", data=payload, headers=headers)
                    self.assertEqual(resp.status, 200)

                stored = Path(td) / "dev1" / "v1" / "segment_large_0.jsonl.gz"
                self.assertTrue(stored.exists())
                self.assertEqual(len(stored.read_bytes()), len(payload))

        asyncio.run(_run())


if __name__ == "__main__":
    unittest.main()

