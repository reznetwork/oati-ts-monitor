"""SQLite-backed mutable metadata for the historical viewer."""

from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Optional


DEFAULT_PROFILE = {
    "name": "Operational default",
    "h3_resolution": 9,
    "metrics": {
        "rssi": {"enabled": True, "weight": 40, "poor": -90, "good": -55},
        "latency": {"enabled": True, "weight": 25, "poor": 300, "good": 30},
        "rate": {"enabled": True, "weight": 20, "poor": 1, "good": 50},
        "beacon_loss": {"enabled": True, "weight": 10, "poor": 10, "good": 0},
        "roaming": {"enabled": True, "weight": 5, "poor": 10, "good": 0},
    },
    "missing": "renormalize",
}


class MetadataStore:
    def __init__(self, path: str | Path) -> None:
        self.path = str(Path(path).expanduser().resolve())
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA journal_mode=WAL")
        return db

    @contextmanager
    def _db(self):
        db = self._connect()
        try:
            with db:
                yield db
        finally:
            db.close()

    def _ensure_schema(self) -> None:
        with self._db() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS base_stations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    lat REAL,
                    lon REAL,
                    coordinate_source TEXT NOT NULL DEFAULT 'manual',
                    archived INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS bssids (
                    bssid TEXT PRIMARY KEY,
                    base_station_id INTEGER NOT NULL REFERENCES base_stations(id),
                    strongest_rssi REAL,
                    strongest_lat REAL,
                    strongest_lon REAL,
                    first_seen_ms INTEGER,
                    last_seen_ms INTEGER,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS quality_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    definition TEXT NOT NULL,
                    is_default INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    token_hash TEXT PRIMARY KEY,
                    csrf_token TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT NOT NULL
                );
                """
            )
            if not db.execute("SELECT 1 FROM quality_profiles LIMIT 1").fetchone():
                now = int(time.time())
                db.execute(
                    "INSERT INTO quality_profiles(name,definition,is_default,created_at,updated_at) VALUES(?,?,?,?,?)",
                    (DEFAULT_PROFILE["name"], json.dumps(DEFAULT_PROFILE), 1, now, now),
                )

    @staticmethod
    def normalize_bssid(value: str) -> str:
        compact = "".join(c for c in str(value).lower() if c in "0123456789abcdef")
        if len(compact) != 12:
            raise ValueError("BSSID must contain 12 hexadecimal digits")
        return ":".join(compact[i : i + 2] for i in range(0, 12, 2))

    def sync_discovered(self, discovered: Iterable[dict[str, Any]]) -> int:
        """Create one station per unseen BSSID at its strongest sample."""
        created = 0
        now = int(time.time())
        with self._lock, self._db() as db:
            for item in discovered:
                try:
                    bssid = self.normalize_bssid(item["bssid"])
                    lat, lon = float(item["lat"]), float(item["lon"])
                except (KeyError, TypeError, ValueError):
                    continue
                rssi = item.get("rssi_dbm")
                row = db.execute("SELECT * FROM bssids WHERE bssid=?", (bssid,)).fetchone()
                if row is None:
                    cur = db.execute(
                        "INSERT INTO base_stations(name,lat,lon,coordinate_source,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                        (f"AP {bssid}", lat, lon, "strongest_sample", now, now),
                    )
                    db.execute(
                        """INSERT INTO bssids
                           (bssid,base_station_id,strongest_rssi,strongest_lat,strongest_lon,first_seen_ms,last_seen_ms,updated_at)
                           VALUES(?,?,?,?,?,?,?,?)""",
                        (bssid, cur.lastrowid, rssi, lat, lon, item.get("first_seen_ms"),
                         item.get("last_seen_ms"), now),
                    )
                    created += 1
                else:
                    old = row["strongest_rssi"]
                    stronger = rssi is not None and (old is None or float(rssi) > float(old))
                    db.execute(
                        """UPDATE bssids SET strongest_rssi=?, strongest_lat=?, strongest_lon=?,
                           first_seen_ms=coalesce(first_seen_ms,?), last_seen_ms=?, updated_at=? WHERE bssid=?""",
                        (
                            rssi if stronger else old,
                            lat if stronger else row["strongest_lat"],
                            lon if stronger else row["strongest_lon"],
                            item.get("first_seen_ms"), item.get("last_seen_ms"), now, bssid,
                        ),
                    )
                    if stronger:
                        db.execute(
                            """UPDATE base_stations SET lat=?,lon=?,updated_at=?
                               WHERE id=? AND coordinate_source='strongest_sample'""",
                            (lat, lon, now, row["base_station_id"]),
                        )
            if created:
                self._audit(db, "discover_bssids", {"created": created})
        return created

    def list_base_stations(self, include_archived: bool = False) -> list[dict[str, Any]]:
        where = "" if include_archived else "WHERE s.archived=0"
        with self._db() as db:
            stations = [dict(r) for r in db.execute(
                f"SELECT s.* FROM base_stations s {where} ORDER BY lower(s.name)"
            )]
            for station in stations:
                station["bssids"] = [dict(r) for r in db.execute(
                    "SELECT * FROM bssids WHERE base_station_id=? ORDER BY bssid", (station["id"],)
                )]
            return stations

    def create_station(self, name: str, lat: Optional[float], lon: Optional[float]) -> dict[str, Any]:
        name = str(name).strip()
        if not name:
            raise ValueError("Station name is required")
        if lat is not None:
            lat = float(lat)
            if not -90 <= lat <= 90:
                raise ValueError("Latitude is out of range")
        if lon is not None:
            lon = float(lon)
            if not -180 <= lon <= 180:
                raise ValueError("Longitude is out of range")
        now = int(time.time())
        with self._lock, self._db() as db:
            cur = db.execute(
                "INSERT INTO base_stations(name,lat,lon,coordinate_source,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                (name, lat, lon, "manual", now, now),
            )
            self._audit(db, "create_station", {"id": cur.lastrowid, "name": name})
            return self.get_station(int(cur.lastrowid), db=db)

    def get_station(self, station_id: int, *, db: Optional[sqlite3.Connection] = None) -> dict[str, Any]:
        own = db is None
        db = db or self._connect()
        try:
            row = db.execute("SELECT * FROM base_stations WHERE id=?", (int(station_id),)).fetchone()
            if row is None:
                raise KeyError("Base station not found")
            out = dict(row)
            out["bssids"] = [dict(r) for r in db.execute(
                "SELECT * FROM bssids WHERE base_station_id=? ORDER BY bssid", (station_id,)
            )]
            return out
        finally:
            if own:
                db.close()

    def update_station(self, station_id: int, values: dict[str, Any]) -> dict[str, Any]:
        with self._lock, self._db() as db:
            current = self.get_station(station_id, db=db)
            expected = int(values.get("updated_at", current["updated_at"]))
            name = str(values.get("name", current["name"])).strip()
            lat = values.get("lat", current["lat"])
            lon = values.get("lon", current["lon"])
            if not name:
                raise ValueError("Station name is required")
            if lat is not None and not -90 <= float(lat) <= 90:
                raise ValueError("Latitude is out of range")
            if lon is not None and not -180 <= float(lon) <= 180:
                raise ValueError("Longitude is out of range")
            now = int(time.time() * 1000)
            cur = db.execute(
                """UPDATE base_stations SET name=?,lat=?,lon=?,coordinate_source='manual',
                   archived=?,updated_at=? WHERE id=? AND updated_at=?""",
                (name, lat, lon, int(bool(values.get("archived", current["archived"]))),
                 now, station_id, expected),
            )
            if cur.rowcount == 0:
                raise ValueError("Station was modified by another session; reload before saving")
            self._audit(db, "update_station", {"id": station_id})
            return self.get_station(station_id, db=db)

    def assign_bssid(self, bssid: str, station_id: int) -> None:
        bssid = self.normalize_bssid(bssid)
        now = int(time.time())
        with self._lock, self._db() as db:
            if not db.execute("SELECT 1 FROM base_stations WHERE id=?", (station_id,)).fetchone():
                raise KeyError("Base station not found")
            if not db.execute("SELECT 1 FROM bssids WHERE bssid=?", (bssid,)).fetchone():
                raise KeyError("BSSID not found")
            db.execute("UPDATE bssids SET base_station_id=?,updated_at=? WHERE bssid=?",
                       (station_id, now, bssid))
            self._audit(db, "assign_bssid", {"bssid": bssid, "station_id": station_id})

    def merge_stations(self, source_id: int, target_id: int) -> None:
        if source_id == target_id:
            raise ValueError("Source and target stations must differ")
        now = int(time.time())
        with self._lock, self._db() as db:
            existing = {
                int(row[0]) for row in db.execute(
                    "SELECT id FROM base_stations WHERE id IN (?,?)", (source_id, target_id)
                )
            }
            if source_id not in existing or target_id not in existing:
                raise KeyError("Source or target base station not found")
            db.execute("UPDATE bssids SET base_station_id=?,updated_at=? WHERE base_station_id=?",
                       (target_id, now, source_id))
            db.execute("UPDATE base_stations SET archived=1,updated_at=? WHERE id=?", (now, source_id))
            self._audit(db, "merge_stations", {"source": source_id, "target": target_id})

    def list_profiles(self) -> list[dict[str, Any]]:
        with self._db() as db:
            out = []
            for row in db.execute("SELECT * FROM quality_profiles ORDER BY is_default DESC,lower(name)"):
                item = dict(row)
                item["definition"] = json.loads(item["definition"])
                out.append(item)
            return out

    def save_profile(self, definition: dict[str, Any], profile_id: Optional[int] = None) -> dict[str, Any]:
        name = str(definition.get("name") or "").strip()
        if not name or not isinstance(definition.get("metrics"), dict):
            raise ValueError("Profile name and metrics are required")
        if len(name) > 120:
            raise ValueError("Profile name is too long")
        h3_resolution = definition.get("h3_resolution")
        if h3_resolution not in (None, "", "auto"):
            try:
                h3_resolution = int(h3_resolution)
            except (TypeError, ValueError) as exc:
                raise ValueError("Hex resolution must be auto or an integer from 4 to 13") from exc
            if not 4 <= h3_resolution <= 13:
                raise ValueError("Hex resolution must be between 4 and 13")
            definition["h3_resolution"] = h3_resolution
        else:
            definition["h3_resolution"] = None
        known = {"rssi", "latency", "rate", "beacon_loss", "roaming"}
        enabled_weight = 0.0
        for key, metric in definition["metrics"].items():
            if key not in known or not isinstance(metric, dict):
                raise ValueError(f"Unknown quality metric: {key}")
            try:
                weight = float(metric.get("weight", 0))
                poor, good = float(metric["poor"]), float(metric["good"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"Invalid thresholds for metric: {key}") from exc
            if not all(math.isfinite(v) for v in (weight, poor, good)) or not 0 <= weight <= 1000:
                raise ValueError(f"Invalid numeric values for metric: {key}")
            if metric.get("enabled", True):
                enabled_weight += weight
        if enabled_weight <= 0:
            raise ValueError("At least one enabled metric must have a positive weight")
        now = int(time.time())
        encoded = json.dumps(definition, separators=(",", ":"), sort_keys=True)
        try:
            with self._lock, self._db() as db:
                if profile_id is None:
                    cur = db.execute(
                        "INSERT INTO quality_profiles(name,definition,created_at,updated_at) VALUES(?,?,?,?)",
                        (name, encoded, now, now),
                    )
                    profile_id = int(cur.lastrowid)
                else:
                    cur = db.execute(
                        "UPDATE quality_profiles SET name=?,definition=?,updated_at=? WHERE id=?",
                        (name, encoded, now, int(profile_id)),
                    )
                    if cur.rowcount == 0:
                        raise KeyError("Quality profile not found")
                self._audit(db, "save_profile", {"id": profile_id, "name": name})
        except sqlite3.IntegrityError as exc:
            raise ValueError("A quality profile with this name already exists") from exc
        return next(p for p in self.list_profiles() if p["id"] == profile_id)

    def create_session(self, token_hash: str, csrf_token: str, expires_at: int) -> None:
        now = int(time.time())
        with self._db() as db:
            db.execute("DELETE FROM sessions WHERE expires_at<?", (now,))
            db.execute("INSERT INTO sessions VALUES(?,?,?,?)",
                       (token_hash, csrf_token, now, int(expires_at)))

    def get_session(self, token_hash: str) -> Optional[dict[str, Any]]:
        now = int(time.time())
        with self._db() as db:
            row = db.execute(
                "SELECT * FROM sessions WHERE token_hash=? AND expires_at>?", (token_hash, now)
            ).fetchone()
            return dict(row) if row else None

    def delete_session(self, token_hash: str) -> None:
        with self._db() as db:
            db.execute("DELETE FROM sessions WHERE token_hash=?", (token_hash,))

    @staticmethod
    def _audit(db: sqlite3.Connection, action: str, details: dict[str, Any]) -> None:
        db.execute("INSERT INTO audit_log(ts,action,details) VALUES(?,?,?)",
                   (int(time.time()), action, json.dumps(details, sort_keys=True)))
