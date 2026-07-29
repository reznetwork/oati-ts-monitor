"""
ClickHouse client for historical full-fidelity log storage.

Optional dependency: clickhouse-connect. When unavailable or not configured,
ingest/query helpers no-op or raise ClickHouseUnavailable.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Sequence

logger = logging.getLogger(__name__)

DDL_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS full_log_snapshots (
        device_id String,
        vehicle String,
        vehicle_short String,
        segment String,
        ts_ms Int64,
        data String,
        ingested_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = ReplacingMergeTree(ingested_at)
    ORDER BY (vehicle_short, ts_ms, device_id, segment)
    """,
    """
    CREATE TABLE IF NOT EXISTS full_log_events (
        device_id String,
        vehicle String,
        vehicle_short String,
        segment String,
        ts_ms Int64,
        kind String,
        payload String,
        lat Nullable(Float64),
        lon Nullable(Float64),
        rssi_dbm Nullable(Float64),
        bssid Nullable(String),
        ingested_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = ReplacingMergeTree(ingested_at)
    ORDER BY (vehicle_short, ts_ms, kind, device_id, segment)
    """,
    """
    CREATE TABLE IF NOT EXISTS ingested_segments (
        device_id String,
        vehicle String,
        vehicle_short String,
        segment String,
        min_ts_ms Int64,
        max_ts_ms Int64,
        row_count UInt64,
        ingested_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = ReplacingMergeTree(ingested_at)
    ORDER BY (device_id, vehicle_short, segment)
    """,
)


class ClickHouseUnavailable(RuntimeError):
    """Raised when ClickHouse is not configured or the client library is missing."""


@dataclass
class ClickHouseConfig:
    host: str = "127.0.0.1"
    port: int = 8123
    database: str = "oati_logs"
    username: str = "default"
    password: str = ""
    enabled: bool = False

    @classmethod
    def from_args(
        cls,
        *,
        enabled: bool = False,
        host: str = "127.0.0.1",
        port: int = 8123,
        database: str = "oati_logs",
        username: str = "default",
        password: str = "",
    ) -> "ClickHouseConfig":
        return cls(
            enabled=bool(enabled),
            host=host or "127.0.0.1",
            port=int(port or 8123),
            database=database or "oati_logs",
            username=username or "default",
            password=password or "",
        )


class ClickHouseStore:
    """Thin wrapper around clickhouse-connect for ingest and viewer queries."""

    def __init__(self, config: ClickHouseConfig) -> None:
        self.config = config
        self._client = None
        if not config.enabled:
            return
        try:
            import clickhouse_connect  # type: ignore
        except ImportError as exc:
            raise ClickHouseUnavailable(
                "clickhouse-connect is not installed; pip install clickhouse-connect"
            ) from exc
        self._client = clickhouse_connect.get_client(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password,
            database="default",
        )
        self._ensure_schema()
        # Reconnect using the target database as default
        self._client = clickhouse_connect.get_client(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password,
            database=config.database,
        )

    @property
    def enabled(self) -> bool:
        return self._client is not None

    def _ensure_schema(self) -> None:
        assert self._client is not None
        db = self.config.database
        self._client.command(f"CREATE DATABASE IF NOT EXISTS {db}")
        self._client.command(f"USE {db}")
        for stmt in DDL_STATEMENTS:
            self._client.command(stmt.strip())
        logger.info("ClickHouse schema ready in database %s", db)

    def _require(self):
        if self._client is None:
            raise ClickHouseUnavailable("ClickHouse is not enabled")
        return self._client

    def is_segment_ingested(self, device_id: str, vehicle_short: str, segment: str) -> bool:
        client = self._require()
        result = client.query(
            """
            SELECT count() FROM ingested_segments FINAL
            WHERE device_id = {device_id:String}
              AND vehicle_short = {vehicle_short:String}
              AND segment = {segment:String}
            """,
            parameters={
                "device_id": device_id,
                "vehicle_short": vehicle_short,
                "segment": segment,
            },
        )
        return bool(result.result_rows and result.result_rows[0][0] > 0)

    def insert_snapshots(self, rows: Sequence[Sequence[Any]]) -> None:
        if not rows:
            return
        client = self._require()
        client.insert(
            "full_log_snapshots",
            rows,
            column_names=[
                "device_id",
                "vehicle",
                "vehicle_short",
                "segment",
                "ts_ms",
                "data",
            ],
            database=self.config.database,
        )

    def insert_events(self, rows: Sequence[Sequence[Any]]) -> None:
        if not rows:
            return
        client = self._require()
        client.insert(
            "full_log_events",
            rows,
            column_names=[
                "device_id",
                "vehicle",
                "vehicle_short",
                "segment",
                "ts_ms",
                "kind",
                "payload",
                "lat",
                "lon",
                "rssi_dbm",
                "bssid",
            ],
            database=self.config.database,
        )

    def mark_segment_ingested(
        self,
        *,
        device_id: str,
        vehicle: str,
        vehicle_short: str,
        segment: str,
        min_ts_ms: int,
        max_ts_ms: int,
        row_count: int,
    ) -> None:
        client = self._require()
        client.insert(
            "ingested_segments",
            [[device_id, vehicle, vehicle_short, segment, min_ts_ms, max_ts_ms, row_count]],
            column_names=[
                "device_id",
                "vehicle",
                "vehicle_short",
                "segment",
                "min_ts_ms",
                "max_ts_ms",
                "row_count",
            ],
            database=self.config.database,
        )

    def list_vehicles(self) -> List[dict]:
        client = self._require()
        result = client.query(
            f"""
            SELECT
                vehicle_short,
                any(vehicle) AS vehicle,
                min(min_ts_ms) AS min_ts_ms,
                max(max_ts_ms) AS max_ts_ms,
                count() AS segments
            FROM {self.config.database}.ingested_segments FINAL
            GROUP BY vehicle_short
            ORDER BY vehicle_short
            """
        )
        out = []
        for row in result.result_rows:
            out.append(
                {
                    "vehicle_short": row[0],
                    "vehicle": row[1],
                    "min_ts_ms": int(row[2]),
                    "max_ts_ms": int(row[3]),
                    "segments": int(row[4]),
                }
            )
        return out

    def activity_intervals(
        self,
        *,
        from_ms: int,
        to_ms: int,
        gap_ms: int = 120_000,
    ) -> List[dict]:
        """Return merged activity bars overlapping [from_ms, to_ms]."""
        client = self._require()
        result = client.query(
            f"""
            SELECT vehicle_short, vehicle, min_ts_ms, max_ts_ms
            FROM {self.config.database}.ingested_segments FINAL
            WHERE max_ts_ms >= {{from_ms:Int64}} AND min_ts_ms <= {{to_ms:Int64}}
            ORDER BY vehicle_short, min_ts_ms
            """,
            parameters={"from_ms": int(from_ms), "to_ms": int(to_ms)},
        )
        by_vehicle: dict[str, list[tuple[int, int, str]]] = {}
        for vehicle_short, vehicle, min_ts, max_ts in result.result_rows:
            by_vehicle.setdefault(str(vehicle_short), []).append(
                (int(min_ts), int(max_ts), str(vehicle or vehicle_short))
            )

        bars: List[dict] = []
        for vehicle_short, intervals in by_vehicle.items():
            intervals.sort(key=lambda x: x[0])
            merged: list[tuple[int, int, str]] = []
            for start, end, vehicle in intervals:
                if not merged:
                    merged.append((start, end, vehicle))
                    continue
                prev_start, prev_end, prev_vehicle = merged[-1]
                if start <= prev_end + gap_ms:
                    merged[-1] = (prev_start, max(prev_end, end), prev_vehicle)
                else:
                    merged.append((start, end, vehicle))
            for start, end, vehicle in merged:
                # Clip to requested window for client convenience
                bars.append(
                    {
                        "vehicle_short": vehicle_short,
                        "vehicle": vehicle,
                        "from_ms": max(start, int(from_ms)),
                        "to_ms": min(end, int(to_ms)),
                        "raw_from_ms": start,
                        "raw_to_ms": end,
                    }
                )
        return bars

    def nearest_snapshot(self, *, vehicle_short: str, ts_ms: int) -> Optional[dict]:
        client = self._require()
        result = client.query(
            f"""
            SELECT ts_ms, device_id, vehicle, vehicle_short, segment, data
            FROM {self.config.database}.full_log_snapshots FINAL
            WHERE vehicle_short = {{vehicle_short:String}}
              AND ts_ms <= {{ts_ms:Int64}}
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            parameters={"vehicle_short": vehicle_short, "ts_ms": int(ts_ms)},
        )
        if not result.result_rows:
            return None
        ts, device_id, vehicle, vs, segment, data = result.result_rows[0]
        try:
            payload = json.loads(data) if isinstance(data, str) else data
        except json.JSONDecodeError:
            payload = {}
        return {
            "ts_ms": int(ts),
            "device_id": device_id,
            "vehicle": vehicle,
            "vehicle_short": vs,
            "segment": segment,
            "data": payload,
        }

    def wifi_track(
        self,
        *,
        vehicle_short: str,
        from_ms: int,
        to_ms: int,
        limit: int = 50_000,
    ) -> dict:
        client = self._require()
        samples_result = client.query(
            f"""
            SELECT ts_ms, lat, lon, rssi_dbm, bssid, payload
            FROM {self.config.database}.full_log_events FINAL
            WHERE vehicle_short = {{vehicle_short:String}}
              AND kind = 'wifi_sample'
              AND ts_ms >= {{from_ms:Int64}} AND ts_ms <= {{to_ms:Int64}}
              AND lat IS NOT NULL AND lon IS NOT NULL
            ORDER BY ts_ms
            LIMIT {{limit:UInt32}}
            """,
            parameters={
                "vehicle_short": vehicle_short,
                "from_ms": int(from_ms),
                "to_ms": int(to_ms),
                "limit": int(limit),
            },
        )
        roam_result = client.query(
            f"""
            SELECT ts_ms, lat, lon, payload
            FROM {self.config.database}.full_log_events FINAL
            WHERE vehicle_short = {{vehicle_short:String}}
              AND kind IN ('wifi_roaming_event', 'roaming_event')
              AND ts_ms >= {{from_ms:Int64}} AND ts_ms <= {{to_ms:Int64}}
              AND lat IS NOT NULL AND lon IS NOT NULL
            ORDER BY ts_ms
            LIMIT {{limit:UInt32}}
            """,
            parameters={
                "vehicle_short": vehicle_short,
                "from_ms": int(from_ms),
                "to_ms": int(to_ms),
                "limit": int(limit),
            },
        )
        samples = []
        for ts_ms, lat, lon, rssi, bssid, payload in samples_result.result_rows:
            samples.append(
                {
                    "ts_ms": int(ts_ms),
                    "lat": float(lat),
                    "lon": float(lon),
                    "rssi_dbm": float(rssi) if rssi is not None else None,
                    "bssid": bssid,
                    "payload": _safe_json(payload),
                }
            )
        roaming = []
        for ts_ms, lat, lon, payload in roam_result.result_rows:
            pl = _safe_json(payload)
            roaming.append(
                {
                    "ts_ms": int(ts_ms),
                    "lat": float(lat),
                    "lon": float(lon),
                    "event": (pl or {}).get("event") if isinstance(pl, dict) else "event",
                    "details": (pl or {}).get("details") if isinstance(pl, dict) else pl,
                }
            )
        return {"samples": samples, "roaming": roaming}


def _safe_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def try_create_store(config: ClickHouseConfig) -> Optional[ClickHouseStore]:
    """Create a store if enabled; log and return None on failure."""
    if not config.enabled:
        logger.info("ClickHouse ingest disabled")
        return None
    try:
        store = ClickHouseStore(config)
        logger.info(
            "ClickHouse connected at %s:%s/%s",
            config.host,
            config.port,
            config.database,
        )
        return store
    except Exception as exc:
        logger.error("ClickHouse unavailable: %s", exc)
        return None
