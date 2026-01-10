from datetime import datetime, timezone

from app.common.schemas import Detection
from app.counting.aggregation import FrameAggregator
from app.density.metrics import compute_density_score
from app.emissions.factors import estimate_co2_kg


def _synthetic_frames():
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    frame_size = (100, 100)
    frames = [
        (
            5.0,
            [
                Detection("car", 0.9, (0, 0, 10, 10)),
                Detection("truck", 0.9, (10, 10, 20, 20)),
            ],
        ),
        (30.0, [Detection("car", 0.9, (0, 0, 10, 10))]),
        (
            65.0,
            [
                Detection("bus", 0.9, (0, 0, 10, 10)),
                Detection("motorcycle", 0.9, (10, 10, 20, 20)),
            ],
        ),
    ]
    return start_time, frame_size, frames


def test_aggregation_determinism():
    start_time, frame_size, frames = _synthetic_frames()
    aggregator = FrameAggregator(bucket_seconds=60)
    for timestamp, detections in frames:
        aggregator.add_frame(timestamp, detections, frame_size)
    buckets = aggregator.finalize(start_time)
    assert len(buckets) == 2

    bucket0 = buckets[0]
    assert bucket0.bucket_index == 0
    assert bucket0.counts == {"car": 2, "truck": 1}
    assert bucket0.total_vehicles == 3
    assert sum(bucket0.counts.values()) == bucket0.total_vehicles

    bucket1 = buckets[1]
    assert bucket1.bucket_index == 1
    assert bucket1.counts == {"bus": 1, "motorcycle": 1}
    assert bucket1.total_vehicles == 2
    assert sum(bucket1.counts.values()) == bucket1.total_vehicles


def test_density_and_emissions_determinism():
    start_time, frame_size, frames = _synthetic_frames()
    aggregator = FrameAggregator(bucket_seconds=60)
    for timestamp, detections in frames:
        aggregator.add_frame(timestamp, detections, frame_size)
    buckets = aggregator.finalize(start_time)

    factors = {"car": 1.0, "truck": 2.0, "bus": 3.0, "motorcycle": 0.5}
    bucket0 = buckets[0]
    bucket1 = buckets[1]

    density0 = compute_density_score(bucket0.total_vehicles, 4, low_max=0.33, medium_max=0.66)
    density1 = compute_density_score(bucket1.total_vehicles, 4, low_max=0.33, medium_max=0.66)
    assert density0.density_score == 0.75
    assert density0.density_level == "high"
    assert density1.density_score == 0.5
    assert density1.density_level == "medium"

    co2_0 = estimate_co2_kg(bucket0.counts, factors, bucket_seconds=60)
    co2_1 = estimate_co2_kg(bucket1.counts, factors, bucket_seconds=60)
    assert co2_0 == 4.0
    assert co2_1 == 3.5
