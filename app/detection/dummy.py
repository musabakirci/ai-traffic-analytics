from __future__ import annotations

import random

import numpy as np

from app.common.schemas import Detection
from app.detection.base import Detector


class DummyDetector(Detector):
    def __init__(
        self,
        mode: str = "none",
        max_detections_per_frame: int = 5,
        seed: int = 42,
        classes: list[str] | None = None,
    ) -> None:
        self.mode = mode
        self.max_detections_per_frame = max_detections_per_frame
        self.random = random.Random(seed)
        self.classes = classes or ["car", "bus", "truck", "motorcycle"]

    def detect(self, frame: np.ndarray) -> list[Detection]:
        if self.mode == "none":
            return []
        height, width = frame.shape[:2]
        count = self.random.randint(0, self.max_detections_per_frame)
        detections: list[Detection] = []
        for _ in range(count):
            class_name = self.random.choice(self.classes)
            x1 = self.random.uniform(0, width * 0.7)
            y1 = self.random.uniform(0, height * 0.7)
            x2 = min(width, x1 + self.random.uniform(10, width * 0.3))
            y2 = min(height, y1 + self.random.uniform(10, height * 0.3))
            detections.append(
                Detection(
                    class_name=class_name,
                    confidence=self.random.uniform(0.3, 0.95),
                    bbox=(x1, y1, x2, y2),
                )
            )
        return detections
