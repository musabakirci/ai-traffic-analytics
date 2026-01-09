from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from app.common.schemas import Detection


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def floor_to_bucket(value: datetime, bucket_seconds: int) -> datetime:
    if bucket_seconds <= 0:
        raise ValueError("bucket_seconds must be > 0")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    timestamp = int(value.timestamp())
    floored = timestamp - (timestamp % bucket_seconds)
    return datetime.fromtimestamp(floored, tz=timezone.utc)


def map_vehicle_class(label: str, class_map: dict[str, str]) -> str | None:
    key = label.strip().lower()
    mapped = class_map.get(key)
    if mapped is None:
        return None
    mapped = mapped.strip().lower()
    if mapped in {"", "ignore", "none", "null"}:
        return None
    return mapped


def normalize_detections(
    detections: Iterable[Detection], class_map: dict[str, str]
) -> list[Detection]:
    normalized: list[Detection] = []
    for detection in detections:
        mapped = map_vehicle_class(detection.class_name, class_map)
        if not mapped:
            continue
        normalized.append(
            Detection(class_name=mapped, confidence=detection.confidence, bbox=detection.bbox)
        )
    return normalized
