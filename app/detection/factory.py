from __future__ import annotations

import logging

from app.common.config import DetectorConfig
from app.detection.base import Detector
from app.detection.dummy import DummyDetector
from app.detection.yolo import YOLODetector

logger = logging.getLogger(__name__)


def create_detector(
    config: DetectorConfig, class_names: list[str], class_map: dict[str, str] | None = None
) -> Detector:
    if config.name == "yolo":
        if not config.model_path:
            logger.warning("YOLO detector requested but model_path is not set; using DummyDetector")
            return DummyDetector(
                mode=config.dummy.mode,
                max_detections_per_frame=config.dummy.max_detections_per_frame,
                seed=config.dummy.seed,
                classes=class_names,
            )
        try:
            return YOLODetector(
                model_path=config.model_path,
                device=config.device,
                confidence_threshold=config.confidence_threshold,
                class_map=class_map,
                visualize=config.visualize,
                visualize_every_n=config.visualize_every_n,
                display_resize_width=config.display_resize_width,
                save_annotated_video=config.save_annotated_video,
                annotated_output_path=config.annotated_output_path,
            )
        except Exception as exc:
            logger.warning("YOLODetector unavailable (%s); falling back to DummyDetector", exc)
            return DummyDetector(
                mode=config.dummy.mode,
                max_detections_per_frame=config.dummy.max_detections_per_frame,
                seed=config.dummy.seed,
                classes=class_names,
            )
    return DummyDetector(
        mode=config.dummy.mode,
        max_detections_per_frame=config.dummy.max_detections_per_frame,
        seed=config.dummy.seed,
        classes=class_names,
    )
