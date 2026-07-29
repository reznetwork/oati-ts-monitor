"""
ClickHouse client for historical full-fidelity log storage.

Optional dependency: clickhouse-connect. When unavailable or not configured,
ingest/query helpers no-op or raise ClickHouseUnavailable.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from typing import Any, Iterable, List, Optional, Sequence

logger = logging.getLogger(__name__)


class ClickHouseUnavailable(RuntimeError):
    """Raised when ClickHouse is not configured or the client library is missing."""


def _validate_db_name(name: str) -> str:
    """Allow only safe ClickHouse identifiers for database names."""
    db = (name or "").strip()
    if not db or not all(c.isalnum() or c == "_" for c in db):
        raise ClickHouseUnavailable(f"Invalid ClickHouse database name: {name!r}")
    return db


def _ddl_statements(database: str, *, modern_h3_order: bool = True) -> tuple[str, ...]:
    """DDL with fully-qualified table names (HTTP has no persistent USE session)."""
    db = _validate_db_name(database)
    return (
        f"""
        CREATE TABLE IF NOT EXISTS {db}.full_log_snapshots (
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
        f"""
        CREATE TABLE IF NOT EXISTS {db}.full_log_events (
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
        f"""
        CREATE TABLE IF NOT EXISTS {db}.ingested_segments (
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
        f"""
        CREATE TABLE IF NOT EXISTS {db}.wifi_quality_samples (
            device_id String,
            vehicle String,
            vehicle_short String,
            segment String,
            ts_ms Int64,
            lat Float64,
            lon Float64,
            bssid String,
            rssi_dbm Nullable(Float64),
            signal_avg_dbm Nullable(Float64),
            noise_dbm Nullable(Float64),
            tx_rate_mbps Nullable(Float64),
            rx_rate_mbps Nullable(Float64),
            tx_traffic_mbps Nullable(Float64),
            rx_traffic_mbps Nullable(Float64),
            gateway_latency_ms Nullable(Float64),
            gateway_ok Nullable(UInt8),
            beacon_loss_count Nullable(Float64),
            roam_count UInt8 DEFAULT 0,
            ingested_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(ingested_at)
        ORDER BY (vehicle_short, ts_ms, device_id, segment, bssid, roam_count)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.wifi_quality_ingested_segments (
            device_id String,
            vehicle_short String,
            segment String,
            sample_count UInt64,
            ingested_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(ingested_at)
        ORDER BY (device_id, vehicle_short, segment)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {db}.wifi_quality_rollups (
            device_id String,
            segment String,
            bucket_ms Int64,
            h3_11 UInt64,
            vehicle_short String,
            bssid String,
            sample_count UInt64,
            rssi_sum Float64,
            rssi_count UInt64,
            latency_sum Float64,
            latency_count UInt64,
            gateway_ok_sum UInt64,
            gateway_ok_count UInt64,
            link_rate_sum Float64,
            link_rate_count UInt64,
            traffic_rate_sum Float64,
            traffic_rate_count UInt64,
            beacon_loss_sum Float64,
            beacon_loss_count UInt64,
            roam_count UInt64,
            ingested_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = ReplacingMergeTree(ingested_at)
        ORDER BY (device_id, vehicle_short, segment, bucket_ms, h3_11, bssid)
        """,
    )


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
    """Thin wrapper around clickhouse-connect for ingest and viewer queries.

    clickhouse-connect forbids concurrent queries on one client session, and
    aiohttp runs handlers/backfill via thread-pool workers. Each thread therefore
    gets its own client instance (threading.local).
    """

    def __init__(self, config: ClickHouseConfig) -> None:
        self.config = config
        self._enabled = False
        self._connect = None
        self._local = threading.local()
        self._schema_lock = threading.Lock()
        self._schema_ready = False
        self._modern_h3_order = True
        self._modern_h3_geo_order = True
        if not config.enabled:
            return
        try:
            import clickhouse_connect  # type: ignore
        except ImportError as exc:
            raise ClickHouseUnavailable(
                "clickhouse-connect is not installed; pip install clickhouse-connect"
            ) from exc
        self._connect = clickhouse_connect
        self._enabled = True
        # Create schema once on the constructing thread.
        self._ensure_schema()

    @property
    def enabled(self) -> bool:
        return self._enabled

    def _new_client(self, *, database: str):
        assert self._connect is not None
        return self._connect.get_client(
            host=self.config.host,
            port=self.config.port,
            username=self.config.username,
            password=self.config.password,
            database=database,
        )

    def _thread_client(self):
        """Return a ClickHouse client bound to the current thread."""
        if not self._enabled or self._connect is None:
            raise ClickHouseUnavailable("ClickHouse is not enabled")
        client = getattr(self._local, "client", None)
        if client is None:
            client = self._new_client(database=self.config.database)
            self._local.client = client
        return client

    def _ensure_schema(self) -> None:
        with self._schema_lock:
            if self._schema_ready:
                return
            db = _validate_db_name(self.config.database)
            # Bootstrap via default DB, then create qualified tables.
            bootstrap = self._new_client(database="default")
            try:
                bootstrap.command(f"CREATE DATABASE IF NOT EXISTS {db}")
                version_result = bootstrap.query("SELECT version()")
                version_text = str(version_result.result_rows[0][0])
                try:
                    major, minor = (int(v) for v in version_text.split(".")[:2])
                except (TypeError, ValueError):
                    major, minor = (25, 5)
                modern_h3_order = (major, minor) >= (25, 5)
                self._modern_h3_order = modern_h3_order
                self._modern_h3_geo_order = (major, minor) >= (25, 1)
                for stmt in _ddl_statements(db, modern_h3_order=modern_h3_order):
                    bootstrap.command(stmt.strip())
            finally:
                try:
                    bootstrap.close()
                except Exception:
                    pass
            self._schema_ready = True
            logger.info("ClickHouse schema ready in database %s", db)

    def _require(self):
        self._ensure_schema()
        return self._thread_client()

    def is_segment_ingested(self, device_id: str, vehicle_short: str, segment: str) -> bool:
        client = self._require()
        db = _validate_db_name(self.config.database)
        result = client.query(
            f"""
            SELECT count() FROM {db}.ingested_segments FINAL
            WHERE device_id = {{device_id:String}}
              AND vehicle_short = {{vehicle_short:String}}
              AND segment = {{segment:String}}
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

    def insert_wifi_quality_samples(self, rows: Sequence[Sequence[Any]]) -> None:
        """Insert normalized geospatial Wi-Fi observations used by the analyzer."""
        if not rows:
            return
        client = self._require()
        client.insert(
            "wifi_quality_samples",
            rows,
            column_names=[
                "device_id", "vehicle", "vehicle_short", "segment", "ts_ms",
                "lat", "lon", "bssid", "rssi_dbm", "signal_avg_dbm",
                "noise_dbm", "tx_rate_mbps", "rx_rate_mbps",
                "tx_traffic_mbps", "rx_traffic_mbps", "gateway_latency_ms",
                "gateway_ok", "beacon_loss_count", "roam_count",
            ],
            database=self.config.database,
        )

    def rebuild_quality_segment_rollup(
        self, device_id: str, vehicle_short: str, segment: str
    ) -> None:
        """Insert a deterministic, replaceable rollup for one source segment."""
        h3_expression = (
            "geoToH3(lat, lon, 11)" if self._modern_h3_order
            else "geoToH3(lon, lat, 11)"
        )
        self._require().command(
            f"""
            INSERT INTO {self.config.database}.wifi_quality_rollups
            (
                device_id,segment,bucket_ms,h3_11,vehicle_short,bssid,
                sample_count,rssi_sum,rssi_count,latency_sum,latency_count,
                gateway_ok_sum,gateway_ok_count,link_rate_sum,link_rate_count,
                traffic_rate_sum,traffic_rate_count,beacon_loss_sum,
                beacon_loss_count,roam_count
            )
            SELECT
                device_id,
                segment,
                intDiv(ts_ms,300000)*300000 AS bucket_ms,
                {h3_expression} AS h3_11,
                vehicle_short,bssid,countIf(roam_count=0),
                sum(ifNull(rssi_dbm,0.0)),count(rssi_dbm),
                sum(ifNull(gateway_latency_ms,0.0)),count(gateway_latency_ms),
                toUInt64(sum(ifNull(gateway_ok,0))),count(gateway_ok),
                sum(greatest(ifNull(tx_rate_mbps,0.0),ifNull(rx_rate_mbps,0.0))),
                countIf(tx_rate_mbps IS NOT NULL OR rx_rate_mbps IS NOT NULL),
                sum(greatest(ifNull(tx_traffic_mbps,0.0),ifNull(rx_traffic_mbps,0.0))),
                countIf(tx_traffic_mbps IS NOT NULL OR rx_traffic_mbps IS NOT NULL),
                sum(ifNull(beacon_loss_count,0.0)),count(beacon_loss_count),
                toUInt64(sum(roam_count))
            FROM {self.config.database}.wifi_quality_samples FINAL
            WHERE device_id={{device_id:String}}
              AND vehicle_short={{vehicle_short:String}}
              AND segment={{segment:String}}
            GROUP BY device_id,segment,bucket_ms,h3_11,vehicle_short,bssid
            """,
            parameters={
                "device_id": device_id, "vehicle_short": vehicle_short, "segment": segment
            },
        )

    def is_quality_segment_ingested(
        self, device_id: str, vehicle_short: str, segment: str
    ) -> bool:
        result = self._require().query(
            f"""
            SELECT count() FROM {self.config.database}.wifi_quality_ingested_segments FINAL
            WHERE device_id={{device_id:String}} AND vehicle_short={{vehicle_short:String}}
              AND segment={{segment:String}}
            """,
            parameters={
                "device_id": device_id, "vehicle_short": vehicle_short, "segment": segment
            },
        )
        return bool(result.result_rows and result.result_rows[0][0])

    def mark_quality_segment_ingested(
        self, device_id: str, vehicle_short: str, segment: str, sample_count: int
    ) -> None:
        self._require().insert(
            "wifi_quality_ingested_segments",
            [[device_id, vehicle_short, segment, int(sample_count)]],
            column_names=["device_id", "vehicle_short", "segment", "sample_count"],
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

    def discovered_bssids(
        self, *, device_id: Optional[str] = None, vehicle_short: Optional[str] = None,
        segment: Optional[str] = None
    ) -> List[dict]:
        """Return strongest geolocated observation for every known BSSID."""
        client = self._require()
        filters = ["bssid != ''", "bssid != 'unknown'"]
        params: dict[str, Any] = {}
        if device_id is not None:
            filters.append("device_id={device_id:String}")
            params["device_id"] = device_id
        if vehicle_short is not None:
            filters.append("vehicle_short={vehicle_short:String}")
            params["vehicle_short"] = vehicle_short
        if segment is not None:
            filters.append("segment={segment:String}")
            params["segment"] = segment
        result = client.query(
            f"""
            SELECT
                bssid,
                argMax(lat, ifNull(rssi_dbm, -999.0)) AS lat,
                argMax(lon, ifNull(rssi_dbm, -999.0)) AS lon,
                max(rssi_dbm) AS strongest_rssi,
                min(ts_ms) AS first_seen_ms,
                max(ts_ms) AS last_seen_ms
            FROM {self.config.database}.wifi_quality_samples FINAL
            WHERE {' AND '.join(filters)}
            GROUP BY bssid
            ORDER BY bssid
            """,
            parameters=params,
        )
        return [
            {
                "bssid": str(row[0]), "lat": float(row[1]), "lon": float(row[2]),
                "rssi_dbm": float(row[3]) if row[3] is not None else None,
                "first_seen_ms": int(row[4]), "last_seen_ms": int(row[5]),
            }
            for row in result.result_rows
        ]

    def wifi_quality_cells(
        self,
        *,
        from_ms: int,
        to_ms: int,
        vehicles: Sequence[str],
        bssids: Sequence[str],
        resolution: int,
        bounds: Optional[Sequence[float]] = None,
        limit: int = 12_000,
    ) -> List[dict]:
        """Aggregate normalized metric states into viewport-aware H3 cells."""
        client = self._require()
        resolution = max(4, min(11, int(resolution)))
        where = [
            "bucket_ms >= {from_ms:Int64}",
            "bucket_ms <= {to_ms:Int64}",
            "(empty({vehicles:Array(String)}) OR has({vehicles:Array(String)}, vehicle_short))",
            "(empty({bssids:Array(String)}) OR has({bssids:Array(String)}, bssid))",
        ]
        params: dict[str, Any] = {
            "from_ms": int(from_ms) // 300_000 * 300_000,
            "to_ms": int(to_ms) // 300_000 * 300_000,
            "vehicles": list(vehicles), "bssids": list(bssids),
            "resolution": resolution, "limit": max(1, min(int(limit), 20_000)),
        }
        if bounds and len(bounds) == 4:
            south, west, north, east = (float(v) for v in bounds)
            # Filter by the center of the fine-resolution source cell.
            lat_index, lon_index = ((1, 2) if self._modern_h3_geo_order else (2, 1))
            where.extend([
                f"tupleElement(h3ToGeo(h3_11), {lat_index}) BETWEEN {{south:Float64}} AND {{north:Float64}}",
                f"tupleElement(h3ToGeo(h3_11), {lon_index}) BETWEEN {{west:Float64}} AND {{east:Float64}}",
            ])
            params.update({"south": south, "west": west, "north": north, "east": east})
        result = client.query(
            f"""
            SELECT
                h3ToParent(h3_11, {{resolution:UInt8}}) AS cell,
                h3ToGeoBoundary(cell) AS boundary,
                groupUniqArray(20)(vehicle_short) AS vehicles,
                groupUniqArray(20)(bssid) AS bssids,
                sum(sample_count), sum(rssi_sum), sum(rssi_count),
                sum(latency_sum), sum(latency_count),
                sum(gateway_ok_sum), sum(gateway_ok_count),
                sum(link_rate_sum), sum(link_rate_count),
                sum(traffic_rate_sum), sum(traffic_rate_count),
                sum(beacon_loss_sum), sum(beacon_loss_count), sum(roam_count)
            FROM {self.config.database}.wifi_quality_rollups FINAL
            WHERE {' AND '.join(where)}
            GROUP BY cell
            ORDER BY sum(sample_count) DESC
            LIMIT {{limit:UInt32}}
            """,
            parameters=params,
        )
        fields = (
            "sample_count", "rssi_sum", "rssi_count", "latency_sum", "latency_count",
            "gateway_ok_sum", "gateway_ok_count", "link_rate_sum", "link_rate_count",
            "traffic_rate_sum", "traffic_rate_count", "beacon_loss_sum",
            "beacon_loss_count", "roam_count",
        )
        out: List[dict] = []
        for row in result.result_rows:
            boundary = [[float(p[0]), float(p[1])] for p in (row[1] or [])]
            item = {
                "cell": str(row[0]), "boundary": boundary,
                "vehicles": [str(v) for v in (row[2] or [])],
                "bssids": [str(v) for v in (row[3] or [])],
            }
            item.update({name: float(value or 0) for name, value in zip(fields, row[4:])})
            out.append(item)
        return out


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
