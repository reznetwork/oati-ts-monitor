"""
Persistent storage for received compressed log files.

Compressed segments are stored under:
  <data_dir>/<device_id>/<vehicle_short>/<segment_name>
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass
class SegmentMeta:
    device_id: str
    vehicle: str
    vehicle_short: str
    segment: str
    path: Path
    bytes_received: int
    files_received: int
    first_seen: float
    last_seen: float

    def to_dict(self) -> dict:
        return {
            "device_id": self.device_id,
            "vehicle": self.vehicle,
            "vehicle_short": self.vehicle_short,
            "segment": self.segment,
            "path": str(self.path),
            "bytes_received": self.bytes_received,
            "files_received": self.files_received,
            "chunks_received": self.files_received,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
        }


@dataclass
class DeviceSummary:
    device_id: str
    vehicles: List[str] = field(default_factory=list)
    total_bytes: int = 0
    total_segments: int = 0
    total_files: int = 0
    first_seen: float = 0.0
    last_seen: float = 0.0


class ChunkStorage:
    """Thread-safe, asyncio-friendly storage for compressed NDJSON segment files."""

    def __init__(self, data_dir: str | Path) -> None:
        self._data_dir = Path(data_dir).expanduser().resolve()
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        # {(device_id, vehicle_short, segment) -> SegmentMeta}
        self._segments: Dict[Tuple[str, str, str], SegmentMeta] = {}
        self._total_bytes = 0
        self._total_files = 0
        self._load_existing()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def ingest_file(
        self,
        *,
        device_id: str,
        vehicle: str,
        vehicle_short: str,
        segment: str,
        schema_version: int,
        file_bytes: int,
        sha256: str,
        payload: bytes,
    ) -> None:
        """Write a complete compressed segment file.

        Raises ValueError for protocol violations (wrong size/checksum, etc.).
        Raises OSError for filesystem errors.
        """
        if not payload:
            raise ValueError("empty payload")
        if file_bytes != len(payload):
            raise ValueError(f"X-File-Bytes mismatch: header says {file_bytes}, body is {len(payload)} bytes")
        if sha256:
            digest = hashlib.sha256(payload).hexdigest()
            if digest.lower() != sha256.lower():
                raise ValueError("X-File-Sha256 mismatch")
        if not payload.startswith(b"\x1f\x8b"):
            raise ValueError("payload is not gzip-compressed")

        device_id = _sanitize(device_id) or "unknown"
        vehicle_short = _sanitize(vehicle_short or vehicle) or "unknown"
        segment = _sanitize_filename(segment) or f"segment_{int(time.time() * 1000)}.jsonl.gz"
        if not segment.endswith(".jsonl.gz"):
            raise ValueError("compressed log filename must end with .jsonl.gz")

        key = (device_id, vehicle_short, segment)
        seg_path = self._data_dir / device_id / vehicle_short / segment

        async with self._lock:
            seg_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = seg_path.with_suffix(seg_path.suffix + f".{os.getpid()}.tmp")
            with tmp_path.open("wb") as fh:
                fh.write(payload)
            tmp_path.replace(seg_path)

            now = time.time()
            if key in self._segments:
                meta = self._segments[key]
                meta.bytes_received = len(payload)
                meta.files_received += 1
                meta.last_seen = now
            else:
                meta = SegmentMeta(
                    device_id=device_id,
                    vehicle=vehicle,
                    vehicle_short=vehicle_short,
                    segment=segment,
                    path=seg_path,
                    bytes_received=len(payload),
                    files_received=1,
                    first_seen=now,
                    last_seen=now,
                )
                self._segments[key] = meta

            self._total_bytes += len(payload)
            self._total_files += 1

    async def get_segments(self, device_id: Optional[str] = None) -> List[SegmentMeta]:
        async with self._lock:
            metas = list(self._segments.values())
        if device_id:
            metas = [m for m in metas if m.device_id == device_id]
        return sorted(metas, key=lambda m: m.last_seen, reverse=True)

    async def get_devices(self) -> List[DeviceSummary]:
        async with self._lock:
            metas = list(self._segments.values())
        devices: Dict[str, DeviceSummary] = {}
        for m in metas:
            if m.device_id not in devices:
                devices[m.device_id] = DeviceSummary(
                    device_id=m.device_id,
                    first_seen=m.first_seen,
                    last_seen=m.last_seen,
                )
            d = devices[m.device_id]
            d.total_bytes += m.bytes_received
            d.total_segments += 1
            d.total_files += m.files_received
            d.first_seen = min(d.first_seen, m.first_seen)
            d.last_seen = max(d.last_seen, m.last_seen)
            vs = m.vehicle_short
            if vs not in d.vehicles:
                d.vehicles.append(vs)
        return sorted(devices.values(), key=lambda d: d.last_seen, reverse=True)

    async def stats(self) -> dict:
        async with self._lock:
            total_bytes = self._total_bytes
            total_files = self._total_files
            total_segments = len(self._segments)
        return {
            "total_bytes": total_bytes,
            "total_files": total_files,
            "total_chunks": total_files,
            "total_segments": total_segments,
            "data_dir": str(self._data_dir),
        }

    async def read_segment_tail(self, device_id: str, vehicle_short: str, segment: str, max_lines: int = 100) -> List[dict]:
        """Read the last *max_lines* NDJSON records from a segment file."""
        device_id = _sanitize(device_id)
        vehicle_short = _sanitize(vehicle_short)
        segment = _sanitize_filename(segment)
        seg_path = self._data_dir / device_id / vehicle_short / segment
        if not seg_path.exists():
            return []
        lines: List[dict] = []
        try:
            opener = gzip.open if seg_path.name.endswith(".gz") else open
            with opener(seg_path, "rb") as fh:
                raw_lines = fh.readlines()
            for raw in raw_lines[-max_lines:]:
                raw = raw.strip()
                if raw:
                    try:
                        lines.append(json.loads(raw))
                    except json.JSONDecodeError:
                        pass
        except OSError:
            pass
        return lines

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_existing(self) -> None:
        """Scan data_dir on startup to populate in-memory index."""
        for seg_path in sorted([*self._data_dir.rglob("segment_*.jsonl.gz"), *self._data_dir.rglob("segment_*.jsonl")]):
            parts = seg_path.relative_to(self._data_dir).parts
            if len(parts) < 3:
                continue
            device_id, vehicle_short, segment = parts[0], parts[1], parts[-1]
            stat = seg_path.stat()
            key = (device_id, vehicle_short, segment)
            mtime = stat.st_mtime
            meta = SegmentMeta(
                device_id=device_id,
                vehicle=vehicle_short,
                vehicle_short=vehicle_short,
                segment=segment,
                path=seg_path,
                bytes_received=stat.st_size,
                files_received=0,
                first_seen=mtime,
                last_seen=mtime,
            )
            self._segments[key] = meta
            self._total_bytes += stat.st_size


def _sanitize(name: str) -> str:
    """Strip path separators and whitespace from component names."""
    return name.replace("/", "_").replace("\\", "_").replace("..", "_").strip()


def _sanitize_filename(name: str) -> str:
    safe = os.path.basename(name)
    # Keep only the filename portion; drop embedded slashes entirely.
    return safe.replace("..", "_")
