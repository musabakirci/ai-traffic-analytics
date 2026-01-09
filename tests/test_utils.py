from datetime import datetime, timezone

from app.common.utils import floor_to_bucket, map_vehicle_class


def test_floor_to_bucket_aligns_to_epoch_boundary() -> None:
    value = datetime(2024, 1, 1, 12, 3, 17, tzinfo=timezone.utc)
    floored = floor_to_bucket(value, 60)
    assert floored == datetime(2024, 1, 1, 12, 3, 0, tzinfo=timezone.utc)


def test_map_vehicle_class_ignore_and_unknown() -> None:
    assert map_vehicle_class("bicycle", {"bicycle": "ignore"}) is None
    assert map_vehicle_class("unknown", {"car": "car"}) is None
