from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class Detection:
    class_name: str
    confidence: float
    bbox: tuple[float, float, float, float] | None = None

    def area(self) -> float | None:
        if not self.bbox:
            return None
        x1, y1, x2, y2 = self.bbox
        return max(0.0, x2 - x1) * max(0.0, y2 - y1)


@dataclass(frozen=True)
class BucketAggregate:
    bucket_index: int
    bucket_ts: str
    counts: dict[str, int]
    total_vehicles: int
    bbox_occupancy: float | None


def detections_by_class(detections: Iterable[Detection]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for detection in detections:
        counts[detection.class_name] = counts.get(detection.class_name, 0) + 1
    return counts
