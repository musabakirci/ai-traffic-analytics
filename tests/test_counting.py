from datetime import datetime, timezone

from app.common.schemas import Detection
import pytest

from app.counting.aggregation import FrameAggregator, compute_bbox_occupancy, dedupe_detections


def test_dedupe_detections_removes_duplicates():
    det1 = Detection(class_name="car", confidence=0.9, bbox=(0, 0, 10, 10))
    det2 = Detection(class_name="car", confidence=0.8, bbox=(0, 0, 10, 10))
    det3 = Detection(class_name="truck", confidence=0.7, bbox=(0, 0, 10, 10))
    unique = dedupe_detections([det1, det2, det3])
    assert len(unique) == 2


def test_frame_aggregation_buckets_counts():
    aggregator = FrameAggregator(bucket_seconds=60)
    frame_size = (100, 100)
    aggregator.add_frame(5.0, [Detection("car", 0.9, (0, 0, 10, 10))], frame_size)
    aggregator.add_frame(10.0, [Detection("truck", 0.9, (0, 0, 10, 10))], frame_size)
    aggregator.add_frame(70.0, [Detection("car", 0.9, (0, 0, 10, 10))], frame_size)
    buckets = aggregator.finalize(datetime(2024, 1, 1, tzinfo=timezone.utc))
    assert len(buckets) == 2
    first = buckets[0]
    second = buckets[1]
    assert first.total_vehicles == 2
    assert first.counts["car"] == 1
    assert first.counts["truck"] == 1
    assert second.total_vehicles == 1
    assert second.counts["car"] == 1


def test_bbox_occupancy_calculation():
    detections = [Detection("car", 0.9, (0, 0, 10, 10))]
    occupancy = compute_bbox_occupancy(detections, (100, 100))
    assert occupancy == pytest.approx(0.01)
