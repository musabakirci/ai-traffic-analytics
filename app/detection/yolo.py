from __future__ import annotations

import numpy as np

from app.common.schemas import Detection
from app.detection.base import Detector


class YOLODetector(Detector):
    def __init__(self, model_path: str, device: str = "cpu") -> None:
        try:
            from ultralytics import YOLO  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            raise ImportError("ultralytics is not installed") from exc
        if not model_path:
            raise ValueError("model_path is required for YOLODetector")
        self.model = YOLO(model_path)
        self.device = device

    def detect(self, frame: np.ndarray) -> list[Detection]:
        results = self.model(frame, verbose=False, device=self.device)
        if not results:
            return []
        result = results[0]
        detections: list[Detection] = []
        names = result.names or {}
        for box in result.boxes:
            class_idx = int(box.cls[0])
            label = names.get(class_idx, str(class_idx))
            confidence = float(box.conf[0])
            x1, y1, x2, y2 = map(float, box.xyxy[0].tolist())
            detections.append(
                Detection(
                    class_name=label,
                    confidence=confidence,
                    bbox=(x1, y1, x2, y2),
                )
            )
        return detections
