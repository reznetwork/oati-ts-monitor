"""Wi-Fi quality scoring and catalog-aware heatmap queries."""

from __future__ import annotations

import math
from typing import Any, Optional, Sequence

from .clickhouse_client import ClickHouseStore
from .metadata_store import DEFAULT_PROFILE, MetadataStore


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
        resolution: int = 9,
        bounds: Optional[Sequence[float]] = None,
        allow_large_range: bool = False,
    ) -> dict[str, Any]:
        if from_ms >= to_ms:
            raise ValueError("from_ms must be earlier than to_ms")
        days = (to_ms - from_ms) / 86_400_000
        if days > self.max_days and not allow_large_range:
            raise OverflowError(
                f"Range is {days:.1f} days; explicit override is required above {self.max_days} days"
            )
        bssids: list[str] = []
        if station_ids:
            selected = {int(v) for v in station_ids}
            for station in self.metadata.list_base_stations(include_archived=True):
                if int(station["id"]) in selected:
                    bssids.extend(item["bssid"] for item in station["bssids"])
            if not bssids:
                return {"cells": [], "summary": self._summary([]), "warning": None}
        raw = self.clickhouse.wifi_quality_cells(
            from_ms=from_ms, to_ms=to_ms, vehicles=vehicles, bssids=bssids,
            resolution=resolution, bounds=bounds,
        )
        definition = self.profile(profile_id)
        cells = [score_cell(cell, definition) for cell in raw]
        warning = (
            f"Large {days:.1f}-day query exceeds the configured {self.max_days}-day range"
            if days > self.max_days else None
        )
        return {
            "cells": cells,
            "summary": self._summary(cells),
            "profile": definition,
            "warning": warning,
            "range_days": round(days, 2),
        }

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
