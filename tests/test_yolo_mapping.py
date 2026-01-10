from app.detection.yolo import map_yolo_class


def test_map_yolo_class_allows_known_vehicle() -> None:
    assert map_yolo_class("car", {}) == "car"


def test_map_yolo_class_rejects_unknown() -> None:
    assert map_yolo_class("person", {}) is None


def test_map_yolo_class_respects_class_map() -> None:
    assert map_yolo_class("sedan", {"sedan": "car"}) == "car"
    assert map_yolo_class("sedan", {"sedan": "person"}) is None
