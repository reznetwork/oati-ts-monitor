"""Wi-Fi quality scoring and catalog-aware heatmap queries."""

from __future__ import annotations

import math
from typing import Any, Optional, Sequence

from .clickhouse_client import ClickHouseStore
from .metadata_store import DEFAULT_PROFILE, MetadataStore


SUM_FIELDS = (
    "sample_count", "rssi_sum", "rssi_count", "latency_sum", "latency_count",
    "gateway_ok_sum", "gateway_ok_count", "link_rate_sum", "link_rate_count",
    "traffic_rate_sum", "traffic_rate_count", "beacon_loss_sum",
    "beacon_loss_count", "roam_count",
)
QUALITY_METRICS = {"composite", "rssi", "latency", "rate", "beacon_loss", "roaming"}


def _average(cell: dict[str, Any], prefix: str) -> Optional[float]:
    count = float(cell.get(prefix + "_count") or 0)
    return float(cell.get(prefix + "_sum") or 0) / count if count > 0 else None


def _metric_score(value: Optional[float], poor: float, good: float) -> Optional[float]:
    if value is None or not math.isfinite(value):
        return None
    if poor == good:
        return 100.0 if value >= good else 0.0
    score = (value - poor) / (good - poor) * 100.0
    return max(0.0, min(100.0, score))


def score_cell(cell: dict[str, Any], definition: dict[str, Any]) -> dict[str, Any]:
    sample_count = int(cell.get("sample_count") or 0)
    traffic_rate = _average(cell, "traffic_rate")
    values = {
        "rssi": _average(cell, "rssi"),
        "latency": _average(cell, "latency"),
        "rate": traffic_rate if traffic_rate is not None else _average(cell, "link_rate"),
        "beacon_loss": _average(cell, "beacon_loss"),
        "roaming": (float(cell.get("roam_count") or 0) * 1000.0 / sample_count)
        if sample_count else None,
    }
    gateway_ok = _average(cell, "gateway_ok")
    metrics = definition.get("metrics") if isinstance(definition.get("metrics"), dict) else {}
    weighted = weight_total = 0.0
    breakdown: dict[str, Any] = {}
    for name, value in values.items():
        config = metrics.get(name) if isinstance(metrics.get(name), dict) else {}
        if not config.get("enabled", True):
            continue
        try:
            weight = max(0.0, float(config.get("weight", 0)))
            score = _metric_score(value, float(config["poor"]), float(config["good"]))
        except (KeyError, TypeError, ValueError):
            continue
        if name == "latency" and score is not None and gateway_ok is not None:
            score *= max(0.0, min(1.0, gateway_ok))
        breakdown[name] = {
            "value": round(value, 3) if value is not None else None,
            "score": round(score, 1) if score is not None else None,
            "weight": weight,
        }
        if score is not None:
            weighted += score * weight
            weight_total += weight
    composite = weighted / weight_total if weight_total else None
    out = dict(cell)
    out.update(
        {
            "score": round(composite, 1) if composite is not None else None,
            "metrics": breakdown,
            "gateway_availability": round(gateway_ok * 100, 1) if gateway_ok is not None else None,
            "confidence": round(min(1.0, math.log10(sample_count + 1) / 3.0), 3),
        }
    )
    return out


def _metric_value(cell: dict[str, Any], metric: str, *, raw: bool = False) -> Optional[float]:
    if metric == "composite":
        return cell.get("score")
    details = cell.get("metrics", {}).get(metric, {})
    return details.get("value" if raw else "score")


def _point_in_polygon(point: Sequence[float], polygon: Sequence[Sequence[float]]) -> bool:
    """Return whether a [lat, lon] point lies inside a [lat, lon] ring."""
    if len(point) != 2 or len(polygon) < 3:
        return False
    lat, lon = float(point[0]), float(point[1])
    inside = False
    previous = polygon[-1]
    for current in polygon:
        y1, x1 = float(previous[0]), float(previous[1])
        y2, x2 = float(current[0]), float(current[1])
        if (y1 > lat) != (y2 > lat):
            crossing = (x2 - x1) * (lat - y1) / (y2 - y1) + x1
            if lon < crossing:
                inside = not inside
        previous = current
    return inside


def _merge_cells(cells: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Merge additive ClickHouse rollup states for one spatial cell."""
    if not cells:
        return {}
    merged = {
        "cell": cells[0]["cell"],
        "boundary": cells[0].get("boundary", []),
        "center": cells[0].get("center"),
        "vehicles": sorted({
            str(vehicle) for cell in cells for vehicle in cell.get("vehicles", [])
        }),
        "bssids": sorted({
            str(bssid) for cell in cells for bssid in cell.get("bssids", [])
        }),
    }
    for field in SUM_FIELDS:
        merged[field] = sum(float(cell.get(field) or 0) for cell in cells)
    station_ids = {
        cell.get("station_id") for cell in cells if cell.get("station_id") is not None
    }
    station_names = {
        str(cell["station_name"]) for cell in cells if cell.get("station_name")
    }
    merged["station_ids"] = sorted(station_ids)
    merged["station_names"] = sorted(station_names)
    return merged


class WiFiQualityService:
    def __init__(
        self, clickhouse: ClickHouseStore, metadata: MetadataStore, *, max_days: int = 90
    ) -> None:
        self.clickhouse = clickhouse
        self.metadata = metadata
        self.max_days = max(1, int(max_days))

    def sync_catalog(
        self, *, device_id: Optional[str] = None, vehicle_short: Optional[str] = None,
        segment: Optional[str] = None
    ) -> int:
        return self.metadata.sync_discovered(
            self.clickhouse.discovered_bssids(
                device_id=device_id, vehicle_short=vehicle_short, segment=segment
            )
        )

    def profile(self, profile_id: Optional[int]) -> dict[str, Any]:
        profiles = self.metadata.list_profiles()
        if profile_id is not None:
            for profile in profiles:
                if int(profile["id"]) == int(profile_id):
                    return profile["definition"]
            raise KeyError("Quality profile not found")
        for profile in profiles:
            if profile.get("is_default"):
                return profile["definition"]
        return profiles[0]["definition"] if profiles else DEFAULT_PROFILE

    def heatmap(
        self,
        *,
        from_ms: int,
        to_ms: int,
        vehicles: Sequence[str] = (),
        station_ids: Sequence[int] = (),
        profile_id: Optional[int] = None,
        resolution: Optional[int] = None,
        bounds: Optional[Sequence[float]] = None,
        allow_large_range: bool = False,
        display: str = "h3",
        station_mode: str = "aggregate",
        metric: str = "composite",
        value_mode: str = "score",
        polygon: Optional[Sequence[Sequence[float]]] = None,
    ) -> dict[str, Any]:
        if from_ms >= to_ms:
            raise ValueError("from_ms must be earlier than to_ms")
        if display not in {"h3", "seamless"}:
            raise ValueError("display must be h3 or seamless")
        if station_mode not in {"aggregate", "best", "separate"}:
            raise ValueError("station_mode must be aggregate, best, or separate")
        if metric not in QUALITY_METRICS:
            raise ValueError("Unknown quality metric")
        if value_mode not in {"score", "raw"}:
            raise ValueError("value_mode must be score or raw")
        if metric == "composite" and value_mode == "raw":
            raise ValueError("Composite is only available as a normalized score")
        if bounds is not None and (
            len(bounds) != 4
            or not (-90 <= float(bounds[0]) <= float(bounds[2]) <= 90)
            or not (-180 <= float(bounds[1]) <= float(bounds[3]) <= 180)
        ):
            raise ValueError("bounds must be south,west,north,east")
        if polygon is not None:
            if len(polygon) < 3:
                raise ValueError("polygon must contain at least three points")
            for point in polygon:
                if (
                    len(point) != 2
                    or not -90 <= float(point[0]) <= 90
                    or not -180 <= float(point[1]) <= 180
                ):
                    raise ValueError("polygon coordinates are out of range")
        days = (to_ms - from_ms) / 86_400_000
        if days > self.max_days and not allow_large_range:
            raise OverflowError(
                f"Range is {days:.1f} days; explicit override is required above {self.max_days} days"
            )
        stations = self.metadata.list_base_stations(include_archived=True)
        selected = {int(v) for v in station_ids}
        bssids: list[str] = []
        if station_ids:
            for station in stations:
                if int(station["id"]) in selected:
                    bssids.extend(item["bssid"] for item in station["bssids"])
            if not bssids:
                return {
                    "display": display, "station_mode": station_mode,
                    "cells": [], "surfaces": [], "summary": self._summary([]),
                    "warning": None,
                }
        definition = self.profile(profile_id)
        profile_resolution = definition.get("h3_resolution", 9)
        if profile_resolution is not None:
            resolution = int(profile_resolution)
        if resolution is None:
            resolution = 9
        raw = self.clickhouse.wifi_quality_cells(
            from_ms=from_ms, to_ms=to_ms, vehicles=vehicles, bssids=bssids,
            resolution=resolution, bounds=bounds,
            group_by_bssid=display == "seamless",
        )
        if polygon is not None:
            raw = [
                cell for cell in raw
                if cell.get("center") and _point_in_polygon(cell["center"], polygon)
            ]
        surfaces: list[dict[str, Any]] = []
        if display == "h3":
            cells = [score_cell(cell, definition) for cell in raw]
        else:
            cells, surfaces = self._seamless_surfaces(
                raw, stations, definition, station_mode=station_mode, metric=metric
            )
        warning = (
            f"Large {days:.1f}-day query exceeds the configured {self.max_days}-day range"
            if days > self.max_days else None
        )
        return {
            "display": display,
            "station_mode": station_mode,
            "metric": metric,
            "value_mode": value_mode,
            "cells": cells,
            "surfaces": surfaces,
            "summary": self._summary(cells),
            "profile": definition,
            "warning": warning,
            "range_days": round(days, 2),
        }

    @staticmethod
    def _seamless_surfaces(
        raw: Sequence[dict[str, Any]],
        stations: Sequence[dict[str, Any]],
        definition: dict[str, Any],
        *,
        station_mode: str,
        metric: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        bssid_catalog: dict[str, tuple[Optional[int], str]] = {}
        for station in stations:
            identity = (int(station["id"]), str(station["name"]))
            for item in station.get("bssids", []):
                bssid_catalog[str(item["bssid"]).lower()] = identity

        by_station_cell: dict[tuple[str, str], list[dict[str, Any]]] = {}
        identities: dict[str, tuple[Optional[int], str]] = {}
        for cell in raw:
            bssid = str(cell.get("grouped_bssid") or "").lower()
            station_id, station_name = bssid_catalog.get(bssid, (None, bssid or "Unknown"))
            station_key = str(station_id) if station_id is not None else f"bssid:{bssid}"
            identities[station_key] = (station_id, station_name)
            tagged = dict(cell)
            tagged.update({"station_id": station_id, "station_name": station_name})
            by_station_cell.setdefault((station_key, str(cell["cell"])), []).append(tagged)

        station_cells: dict[str, list[dict[str, Any]]] = {}
        for (station_key, _), grouped in by_station_cell.items():
            merged = _merge_cells(grouped)
            station_id, station_name = identities[station_key]
            merged.update({"station_id": station_id, "station_name": station_name})
            station_cells.setdefault(station_key, []).append(score_cell(merged, definition))

        if station_mode == "separate":
            surfaces = [
                {
                    "key": key,
                    "station_id": identities[key][0],
                    "station_name": identities[key][1],
                    "cells": sorted(items, key=lambda item: item["cell"]),
                    "summary": WiFiQualityService._summary(items),
                }
                for key, items in sorted(
                    station_cells.items(), key=lambda pair: identities[pair[0]][1].lower()
                )
            ]
            all_by_cell: dict[str, list[dict[str, Any]]] = {}
            for items in station_cells.values():
                for cell in items:
                    all_by_cell.setdefault(str(cell["cell"]), []).append(cell)
            combined = [
                score_cell(_merge_cells(items), definition)
                for items in all_by_cell.values()
            ]
            return combined, surfaces

        by_cell: dict[str, list[dict[str, Any]]] = {}
        for items in station_cells.values():
            for cell in items:
                by_cell.setdefault(str(cell["cell"]), []).append(cell)
        if station_mode == "best":
            cells = []
            for items in by_cell.values():
                available = [
                    cell for cell in items if _metric_value(cell, metric) is not None
                ]
                if available:
                    cells.append(max(
                        available, key=lambda cell: float(_metric_value(cell, metric) or 0)
                    ))
        else:
            cells = [
                score_cell(_merge_cells(items), definition) for items in by_cell.values()
            ]
        surface_name = "Best serving station" if station_mode == "best" else "All selected stations"
        surfaces = [{
            "key": station_mode,
            "station_id": None,
            "station_name": surface_name,
            "cells": sorted(cells, key=lambda item: item["cell"]),
            "summary": WiFiQualityService._summary(cells),
        }]
        return cells, surfaces

    @staticmethod
    def _summary(cells: Sequence[dict[str, Any]]) -> dict[str, Any]:
        samples = sum(int(c.get("sample_count") or 0) for c in cells)
        scores = [
            (float(c["score"]), int(c.get("sample_count") or 0))
            for c in cells if c.get("score") is not None
        ]
        weighted = sum(score * count for score, count in scores)
        weight = sum(count for _, count in scores)
        confidence = (
            sum(float(c.get("confidence") or 0) * int(c.get("sample_count") or 0) for c in cells)
            / samples if samples else 0
        )
        return {
            "score": round(weighted / weight, 1) if weight else None,
            "samples": samples,
            "cells": len(cells),
            "roams": int(sum(float(c.get("roam_count") or 0) for c in cells)),
            "confidence": round(confidence * 100, 1),
        }
