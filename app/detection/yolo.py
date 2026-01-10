from __future__ import annotations

import numpy as np

from app.common.schemas import Detection
from app.common.utils import map_vehicle_class
from app.detection.base import Detector
from app.detection.visualizer import FrameVisualizer


ALLOWED_CLASSES = {"car", "bus", "truck", "motorcycle"}


def map_yolo_class(label: str, class_map: dict[str, str]) -> str | None:
    mapped = map_vehicle_class(label, class_map)
    if mapped is None:
        mapped = label.strip().lower()
    if mapped not in ALLOWED_CLASSES:
        return None
    return mapped


class YOLODetector(Detector):
    def __init__(
        self,
        model_path: str,
        device: str = "cpu",
        confidence_threshold: float = 0.25,
        class_map: dict[str, str] | None = None,
        visualize: bool = False,
        visualize_every_n: int = 1,
        display_resize_width: int | None = None,
        save_annotated_video: bool = False,
        annotated_output_path: str | None = None,
    ) -> None:
        try:
            from ultralytics import YOLO  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            raise ImportError("ultralytics is not installed") from exc
        if not model_path:
            raise ValueError("model_path is required for YOLODetector")
        self.model = YOLO(model_path)
        self.device = device
        self.confidence_threshold = confidence_threshold
        self.class_map = class_map or {}
        self.allowed_classes = ALLOWED_CLASSES
        if visualize or save_annotated_video:
            self.visualizer = FrameVisualizer(
                every_n=visualize_every_n,
                display_resize_width=display_resize_width,
                save_annotated_video=save_annotated_video,
                annotated_output_path=annotated_output_path,
                enable_display=visualize,
            )
        else:
            self.visualizer = None

    def detect(self, frame: np.ndarray) -> list[Detection]:
        results = self.model.predict(
            frame, verbose=False, device=self.device, conf=self.confidence_threshold
        )
        if not results:
            return []
        result = results[0]
        detections: list[Detection] = []
        names = result.names or {}
        for box in result.boxes:
            class_idx = int(box.cls[0])
            label = names.get(class_idx, str(class_idx))
            confidence = float(box.conf[0])
            if confidence < self.confidence_threshold:
                continue
            mapped = map_yolo_class(label, self.class_map)
            if mapped is None or mapped not in self.allowed_classes:
                continue
            x1, y1, x2, y2 = map(float, box.xyxy[0].tolist())
            detections.append(
                Detection(
                    class_name=mapped,
                    confidence=confidence,
                    bbox=(x1, y1, x2, y2),
                )
            )
        if self.visualizer:
            self.visualizer.show(frame, detections)
        return detections

    def close(self) -> None:
        if self.visualizer:
            self.visualizer.close()
