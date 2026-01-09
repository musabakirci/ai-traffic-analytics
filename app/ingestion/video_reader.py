from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import numpy as np

try:
    import cv2  # type: ignore

    _HAS_CV2 = True
except Exception:  # pragma: no cover - optional dependency
    cv2 = None
    _HAS_CV2 = False

try:
    import imageio.v2 as imageio  # type: ignore

    _HAS_IMAGEIO = True
except Exception:  # pragma: no cover - optional dependency
    imageio = None
    _HAS_IMAGEIO = False


class VideoReadError(RuntimeError):
    pass


def iter_sampled_frames(video_path: str, target_fps: float) -> Iterator[tuple[np.ndarray, float]]:
    if target_fps <= 0:
        raise ValueError("target_fps must be > 0")
    if _HAS_CV2:
        yield from _iter_sampled_frames_cv2(video_path, target_fps)
        return
    if _HAS_IMAGEIO:
        yield from _iter_sampled_frames_imageio(video_path, target_fps)
        return
    raise VideoReadError("Neither OpenCV nor imageio is available for video reading")


def _iter_sampled_frames_cv2(video_path: str, target_fps: float) -> Iterator[tuple[np.ndarray, float]]:
    assert cv2 is not None
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise VideoReadError(f"Unable to open video: {video_path}")
    fps = cap.get(cv2.CAP_PROP_FPS) or target_fps
    frame_interval = max(int(round(fps / target_fps)), 1)
    frame_index = 0
    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            if frame_index % frame_interval == 0:
                timestamp = frame_index / fps
                yield frame, timestamp
            frame_index += 1
    finally:
        cap.release()


def _iter_sampled_frames_imageio(
    video_path: str, target_fps: float
) -> Iterator[tuple[np.ndarray, float]]:
    assert imageio is not None
    reader = imageio.get_reader(video_path)
    meta: dict[str, Any] = reader.get_meta_data()
    fps = float(meta.get("fps") or target_fps)
    frame_interval = max(int(round(fps / target_fps)), 1)
    try:
        for frame_index, frame in enumerate(reader):
            if frame_index % frame_interval == 0:
                timestamp = frame_index / fps
                yield frame, timestamp
    finally:
        reader.close()
