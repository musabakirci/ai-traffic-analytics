from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable

from app.common.schemas import BucketAggregate, Detection, detections_by_class
from app.common.utils import floor_to_bucket, to_utc_iso


@dataclass
class _BucketAccumulator:
    counts: dict[str, int] = field(default_factory=dict)
    total_vehicles: int = 0
    frames: int = 0
    occupancy_sum: float = 0.0
    occupancy_frames: int = 0


@dataclass
class FrameAggregator:
    bucket_seconds: int
    buckets: dict[int, _BucketAccumulator] = field(default_factory=dict)

    def add_frame(
        self,
        timestamp_sec: float,
        detections: Iterable[Detection],
        frame_size: tuple[int, int] | None,
    ) -> None:
        bucket_index = int(timestamp_sec // self.bucket_seconds)
        bucket = self.buckets.setdefault(bucket_index, _BucketAccumulator())
        bucket.frames += 1
        unique = dedupe_detections(detections)
        counts = detections_by_class(unique)
        for class_name, count in counts.items():
            bucket.counts[class_name] = bucket.counts.get(class_name, 0) + count
        bucket.total_vehicles += len(unique)
        occupancy = compute_bbox_occupancy(unique, frame_size)
        if occupancy is not None:
            bucket.occupancy_sum += occupancy
            bucket.occupancy_frames += 1

    def finalize(self, start_time: datetime) -> list[BucketAggregate]:
        start_time = floor_to_bucket(start_time, self.bucket_seconds)
        aggregates: list[BucketAggregate] = []
        for bucket_index in sorted(self.buckets.keys()):
            bucket = self.buckets[bucket_index]
            bucket_ts = start_time + timedelta(seconds=bucket_index * self.bucket_seconds)
            bucket_ts = floor_to_bucket(bucket_ts, self.bucket_seconds)
            occupancy = None
            if bucket.occupancy_frames:
                occupancy = bucket.occupancy_sum / bucket.occupancy_frames
            aggregates.append(
                BucketAggregate(
                    bucket_index=bucket_index,
                    bucket_ts=to_utc_iso(bucket_ts),
                    counts=dict(bucket.counts),
                    total_vehicles=bucket.total_vehicles,
                    bbox_occupancy=occupancy,
                )
            )
        return aggregates


def dedupe_detections(detections: Iterable[Detection]) -> list[Detection]:
    unique: list[Detection] = []
    seen: set[tuple[str, tuple[float, float, float, float] | None]] = set()
    for detection in detections:
        if detection.bbox:
            bbox_key = tuple(round(value, 1) for value in detection.bbox)
        else:
            bbox_key = None
        key = (detection.class_name, bbox_key)
        if key in seen:
            continue
        seen.add(key)
        unique.append(detection)
    return unique


def compute_bbox_occupancy(
    detections: Iterable[Detection], frame_size: tuple[int, int] | None
) -> float | None:
    if not frame_size:
        return None
    width, height = frame_size
    if width <= 0 or height <= 0:
        return None
    frame_area = float(width * height)
    total_area = 0.0
    has_bbox = False
    for detection in detections:
        area = detection.area()
        if area is None:
            continue
        has_bbox = True
        total_area += max(0.0, area)
    if not has_bbox:
        return None
    occupancy = min(1.0, total_area / frame_area)
    return occupancy
