from __future__ import annotations

import atexit
import logging
import time
from pathlib import Path
from typing import Iterable

import numpy as np

from app.common.schemas import Detection
from app.detection.base import StopProcessing

logger = logging.getLogger(__name__)

try:
    import cv2  # type: ignore

    _HAS_CV2 = True
except Exception:  # pragma: no cover - optional dependency
    cv2 = None
    _HAS_CV2 = False


DEFAULT_OUTPUT_FPS = 15.0


class FrameVisualizer:
    def __init__(
        self,
        window_name: str = "YOLO Detections",
        every_n: int = 1,
        display_resize_width: int | None = None,
        save_annotated_video: bool = False,
        annotated_output_path: str | None = None,
        output_fps: float | None = None,
        enable_display: bool = True,
    ) -> None:
        self.window_name = window_name
        self.every_n = max(1, every_n)
        self.display_resize_width = display_resize_width
        self.save_annotated_video = save_annotated_video
        self.annotated_output_path = annotated_output_path
        self.output_fps = output_fps or DEFAULT_OUTPUT_FPS
        self.enable_display = enable_display
        self._frame_index = 0
        self._displayed_frames = 0
        self._last_fps_time = time.monotonic()
        self._enabled = _HAS_CV2
        self._writer = None
        if not self._enabled:
            if self.save_annotated_video:
                raise RuntimeError("OpenCV is required for annotated video recording")
            logger.warning("OpenCV not available; visualization disabled")
            return
        if self.save_annotated_video:
            if not self.annotated_output_path:
                raise ValueError("annotated_output_path is required when save_annotated_video is true")
            output_path = Path(self.annotated_output_path)
            if output_path.exists():
                raise FileExistsError(f"Annotated output already exists: {output_path}")
            output_path.parent.mkdir(parents=True, exist_ok=True)
        atexit.register(self.close)
        if self.enable_display:
            print("YOLO visualization enabled - press 'q' to stop.")

    def show(self, frame: np.ndarray, detections: Iterable[Detection]) -> None:
        if not self._enabled:
            return
        self._frame_index += 1
        should_display = self.enable_display and self._frame_index % self.every_n == 0
        should_record = self.save_annotated_video
        if not should_display and not should_record:
            return
        assert cv2 is not None
        annotated_frame = frame.copy()
        scale = 1.0
        for detection in detections:
            if detection.bbox is None:
                continue
            x1, y1, x2, y2 = detection.bbox
            x1, y1, x2, y2 = (int(round(value)) for value in (x1, y1, x2, y2))
            label = f"{detection.class_name} {detection.confidence:.2f}"
            cv2.rectangle(annotated_frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
            y_text = max(0, y1 - 5)
            cv2.putText(
                annotated_frame,
                label,
                (x1, y_text),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.5,
                (0, 255, 0),
                1,
                cv2.LINE_AA,
            )
        if should_record:
            self._write_frame(annotated_frame)
        if should_display:
            display_frame = annotated_frame
            if self.display_resize_width is not None:
                height, width = annotated_frame.shape[:2]
                if width > 0 and self.display_resize_width > 0:
                    scale = self.display_resize_width / float(width)
                    if scale != 1.0:
                        new_width = int(round(width * scale))
                        new_height = int(round(height * scale))
                        display_frame = cv2.resize(
                            annotated_frame,
                            (new_width, new_height),
                            interpolation=cv2.INTER_AREA,
                        )
            cv2.imshow(self.window_name, display_frame)
            self._displayed_frames += 1
            now = time.monotonic()
            elapsed = now - self._last_fps_time
            if elapsed >= 5.0:
                fps = self._displayed_frames / elapsed
                logger.info("Visualization FPS: %.2f", fps)
                self._displayed_frames = 0
                self._last_fps_time = now
            if cv2.waitKey(1) & 0xFF == ord("q"):
                self.close()
                raise StopProcessing("User requested stop")

    def _write_frame(self, frame: np.ndarray) -> None:
        assert cv2 is not None
        if self._writer is None:
            height, width = frame.shape[:2]
            fourcc = cv2.VideoWriter_fourcc(*"mp4v")
            self._writer = cv2.VideoWriter(
                str(self.annotated_output_path), fourcc, self.output_fps, (width, height)
            )
            if not self._writer.isOpened():
                self._writer.release()
                self._writer = cv2.VideoWriter(
                    str(self.annotated_output_path),
                    cv2.VideoWriter_fourcc(*"avc1"),
                    self.output_fps,
                    (width, height),
                )
            if not self._writer.isOpened():
                self._writer.release()
                self._writer = None
                raise RuntimeError("Unable to initialize annotated video writer")
        self._writer.write(frame)

    def close(self) -> None:
        if not self._enabled:
            return
        assert cv2 is not None
        if self._writer is not None:
            self._writer.release()
            self._writer = None
        cv2.destroyAllWindows()
