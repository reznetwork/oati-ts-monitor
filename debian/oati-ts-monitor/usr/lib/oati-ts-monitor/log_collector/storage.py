"""
Persistent storage for received log chunks.

Segments are stored under:
  <data_dir>/<device_id>/<vehicle_short>/<segment_name>

Chunks are expected to arrive in order (start == current file size).
Out-of-order or duplicate chunks are rejected with an explicit error so
the sender can retry from the correct offset.
"""

from __future__ import annotations

import asyncio
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
    chunks_received: int
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
            "chunks_received": self.chunks_received,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
        }


@dataclass
class DeviceSummary:
    device_id: str
    vehicles: List[str] = field(default_factory=list)
    total_bytes: int = 0
    total_segments: int = 0
    total_chunks: int = 0
    first_seen: float = 0.0
    last_seen: float = 0.0


class ChunkStorage:
    """Thread-safe, asyncio-friendly storage that reassembles NDJSON segment files."""

    def __init__(self, data_dir: str | Path) -> None:
        self._data_dir = Path(data_dir).expanduser().resolve()
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        # {(device_id, vehicle_short, segment) -> SegmentMeta}
        self._segments: Dict[Tuple[str, str, str], SegmentMeta] = {}
        self._total_bytes = 0
        self._total_chunks = 0
        self._load_existing()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def ingest(
        self,
        *,
        device_id: str,
        vehicle: str,
        vehicle_short: str,
        segment: str,
        schema_version: int,
        chunk_start: int,
        chunk_end: int,
        payload: bytes,
    ) -> None:
        """Write a chunk to the correct segment file.

        Raises ValueError for protocol violations (gap, wrong size, etc.).
        Raises OSError for filesystem errors.
        """
        if not payload:
            raise ValueError("empty payload")
        if chunk_end - chunk_start != len(payload):
            raise ValueError(
                f"X-Chunk-Bytes mismatch: header says {chunk_end - chunk_start}, "
                f"body is {len(payload)} bytes"
            )

        device_id = _sanitize(device_id) or "unknown"
        vehicle_short = _sanitize(vehicle_short or vehicle) or "unknown"
        segment = _sanitize_filename(segment) or f"segment_{int(time.time() * 1000)}.jsonl"

        key = (device_id, vehicle_short, segment)
        seg_path = self._data_dir / device_id / vehicle_short / segment

        async with self._lock:
            seg_path.parent.mkdir(parents=True, exist_ok=True)
            current_size = seg_path.stat().st_size if seg_path.exists() else 0

            if chunk_start != current_size:
                raise ValueError(
                    f"chunk gap: file has {current_size} bytes, "
                    f"chunk starts at {chunk_start}"
                )

            mode = "ab" if chunk_start > 0 else "wb"
            with seg_path.open(mode) as fh:
                fh.write(payload)

            now = time.time()
            if key in self._segments:
                meta = self._segments[key]
                meta.bytes_received += len(payload)
                meta.chunks_received += 1
                meta.last_seen = now
            else:
                meta = SegmentMeta(
                    device_id=device_id,
                    vehicle=vehicle,
                    vehicle_short=vehicle_short,
                    segment=segment,
                    path=seg_path,
                    bytes_received=len(payload),
                    chunks_received=1,
                    first_seen=now,
                    last_seen=now,
                )
                self._segments[key] = meta

            self._total_bytes += len(payload)
            self._total_chunks += 1

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
            d.total_chunks += m.chunks_received
            d.first_seen = min(d.first_seen, m.first_seen)
            d.last_seen = max(d.last_seen, m.last_seen)
            vs = m.vehicle_short
            if vs not in d.vehicles:
                d.vehicles.append(vs)
        return sorted(devices.values(), key=lambda d: d.last_seen, reverse=True)

    async def stats(self) -> dict:
        async with self._lock:
            total_bytes = self._total_bytes
            total_chunks = self._total_chunks
            total_segments = len(self._segments)
        return {
            "total_bytes": total_bytes,
            "total_chunks": total_chunks,
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
            with seg_path.open("rb") as fh:
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
        now = time.time()
        for seg_path in sorted(self._data_dir.rglob("segment_*.jsonl")):
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
                chunks_received=0,
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
