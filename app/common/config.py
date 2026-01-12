from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import os

import yaml


DEFAULT_CONFIG: dict[str, Any] = {
    "frame_sampling_fps": 2.0,
    "bucket_seconds": 60,
    "vehicle_class_map": {
        "car": "car",
        "bus": "bus",
        "truck": "truck",
        "motorcycle": "motorcycle",
        "motorbike": "motorcycle",
    },
    "detector": {
        "name": "dummy",
        "type": "dummy",
        "model_path": None,
        "device": "cpu",
        "confidence_threshold": 0.25,
        "conf_threshold": 0.25,
        "visualize": False,
        "visualize_every_n": 1,
        "display_resize_width": None,
        "save_annotated_video": False,
        "annotated_output_path": None,
        "dummy": {
            "mode": "none",
            "max_detections_per_frame": 5,
            "seed": 42,
        },
    },
    "density": {
        "low_max": 0.33,
        "medium_max": 0.66,
        "default_max_vehicles": 30,
        "rolling_max": True,
        "max_vehicles_by_camera": {},
    },
    "emissions": {
        "factors": {
            "car": 0.25,
            "bus": 1.2,
            "truck": 1.0,
            "motorcycle": 0.1,
        },
        "sensitivity_pct": 10.0,
    },
    "data_paths": {
        "db_path": "data/db/traffic.db",
        "logs_dir": "data/logs",
    },
    "realtime": {
        "enabled": False,
        "websocket_url": "ws://localhost:8000/ws/live",
        "send_frames": True,
    },
}


@dataclass(frozen=True)
class DummyConfig:
    mode: str = "none"
    max_detections_per_frame: int = 5
    seed: int = 42


@dataclass(frozen=True)
class DetectorConfig:
    name: str = "dummy"
    model_path: str | None = None
    device: str = "cpu"
    confidence_threshold: float = 0.25
    visualize: bool = False
    visualize_every_n: int = 1
    display_resize_width: int | None = None
    save_annotated_video: bool = False
    annotated_output_path: str | None = None
    dummy: DummyConfig = field(default_factory=DummyConfig)

    @property
    def type(self) -> str:
        return self.name


@dataclass(frozen=True)
class DensityConfig:
    low_max: float = 0.33
    medium_max: float = 0.66
    default_max_vehicles: int = 30
    rolling_max: bool = True
    max_vehicles_by_camera: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class EmissionsConfig:
    factors: dict[str, float] = field(
        default_factory=lambda: {
            "car": 0.25,
            "bus": 1.2,
            "truck": 1.0,
            "motorcycle": 0.1,
        }
    )
    sensitivity_pct: float | None = 10.0


@dataclass(frozen=True)
class DataPaths:
    db_path: str = "data/db/traffic.db"
    logs_dir: str = "data/logs"


@dataclass(frozen=True)
class RealtimeConfig:
    enabled: bool = False
    websocket_url: str = "ws://localhost:8000/ws/live"
    send_frames: bool = True


@dataclass(frozen=True)
class AppConfig:
    frame_sampling_fps: float = 2.0
    bucket_seconds: int = 60
    vehicle_class_map: dict[str, str] = field(
        default_factory=lambda: {
            "car": "car",
            "bus": "bus",
            "truck": "truck",
            "motorcycle": "motorcycle",
            "motorbike": "motorcycle",
        }
    )
    detector: DetectorConfig = field(default_factory=DetectorConfig)
    density: DensityConfig = field(default_factory=DensityConfig)
    emissions: EmissionsConfig = field(default_factory=EmissionsConfig)
    data_paths: DataPaths = field(default_factory=DataPaths)
    realtime: RealtimeConfig = field(default_factory=RealtimeConfig)

    @property
    def db_url(self) -> str:
        env_db_url = os.getenv("TRAFFIC_AI_DB_URL")
        if env_db_url:
            return env_db_url
        db_path = self.data_paths.db_path
        if "://" in db_path:
            return db_path
        path = Path(db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        return f"sqlite:///{path.as_posix()}"


def deep_update(base: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_update(result[key], value)
        else:
            result[key] = value
    return result


def validate_config(config: AppConfig) -> None:
    if config.frame_sampling_fps <= 0:
        raise ValueError("frame_sampling_fps must be > 0")
    if config.bucket_seconds <= 0:
        raise ValueError("bucket_seconds must be > 0")
    if not (0.0 <= config.density.low_max < config.density.medium_max <= 1.0):
        raise ValueError("density thresholds must satisfy 0 <= low_max < medium_max <= 1")
    if any(value < 0 for value in config.emissions.factors.values()):
        raise ValueError("emission factors must be >= 0")
    if config.emissions.sensitivity_pct is not None and config.emissions.sensitivity_pct < 0:
        raise ValueError("sensitivity_pct must be >= 0")
    if not (0.0 <= config.detector.confidence_threshold <= 1.0):
        raise ValueError("detector.confidence_threshold must be between 0 and 1")
    if config.detector.visualize_every_n <= 0:
        raise ValueError("detector.visualize_every_n must be > 0")
    if config.detector.display_resize_width is not None and config.detector.display_resize_width <= 0:
        raise ValueError("detector.display_resize_width must be > 0 when set")
    if config.detector.save_annotated_video:
        output_path = (config.detector.annotated_output_path or "").strip()
        if not output_path:
            raise ValueError(
                "detector.annotated_output_path is required when save_annotated_video is true"
            )
        if not output_path.lower().endswith(".mp4"):
            raise ValueError("detector.annotated_output_path must end with .mp4")
    if config.realtime.enabled:
        websocket_url = config.realtime.websocket_url.strip()
        if not websocket_url:
            raise ValueError("realtime.websocket_url is required when realtime is enabled")


def load_config(path: str) -> AppConfig:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    merged = deep_update(DEFAULT_CONFIG, data)
    detector_dict = merged.get("detector", {})
    dummy_dict = detector_dict.get("dummy", {})
    density_dict = merged.get("density", {})
    emissions_dict = merged.get("emissions", {})
    data_paths_dict = merged.get("data_paths", {})
    realtime_dict = merged.get("realtime", {})
    config = AppConfig(
        frame_sampling_fps=float(merged.get("frame_sampling_fps", 2.0)),
        bucket_seconds=int(merged.get("bucket_seconds", 60)),
        vehicle_class_map={
            str(k).lower(): str(v) for k, v in merged.get("vehicle_class_map", {}).items()
        },
        detector=DetectorConfig(
            name=str(detector_dict.get("type", detector_dict.get("name", "dummy"))).lower(),
            model_path=detector_dict.get("model_path"),
            device=str(detector_dict.get("device", "cpu")),
            confidence_threshold=float(
                detector_dict.get("conf_threshold", detector_dict.get("confidence_threshold", 0.25))
            ),
            visualize=bool(detector_dict.get("visualize", False)),
            visualize_every_n=int(detector_dict.get("visualize_every_n", 1)),
            display_resize_width=(
                int(detector_dict["display_resize_width"])
                if detector_dict.get("display_resize_width") is not None
                else None
            ),
            save_annotated_video=bool(detector_dict.get("save_annotated_video", False)),
            annotated_output_path=detector_dict.get("annotated_output_path"),
            dummy=DummyConfig(
                mode=str(dummy_dict.get("mode", "none")).lower(),
                max_detections_per_frame=int(dummy_dict.get("max_detections_per_frame", 5)),
                seed=int(dummy_dict.get("seed", 42)),
            ),
        ),
        density=DensityConfig(
            low_max=float(density_dict.get("low_max", 0.33)),
            medium_max=float(density_dict.get("medium_max", 0.66)),
            default_max_vehicles=int(density_dict.get("default_max_vehicles", 30)),
            rolling_max=bool(density_dict.get("rolling_max", True)),
            max_vehicles_by_camera={
                str(k): int(v)
                for k, v in density_dict.get("max_vehicles_by_camera", {}).items()
            },
        ),
        emissions=EmissionsConfig(
            factors={
                str(k): float(v)
                for k, v in emissions_dict.get("factors", {}).items()
            },
            sensitivity_pct=(
                float(emissions_dict["sensitivity_pct"])
                if emissions_dict.get("sensitivity_pct") is not None
                else None
            ),
        ),
        data_paths=DataPaths(
            db_path=str(data_paths_dict.get("db_path", "data/db/traffic.db")),
            logs_dir=str(data_paths_dict.get("logs_dir", "data/logs")),
        ),
        realtime=RealtimeConfig(
            enabled=bool(realtime_dict.get("enabled", False)),
            websocket_url=str(
                realtime_dict.get("websocket_url", "ws://localhost:8000/ws/live")
            ),
            send_frames=bool(realtime_dict.get("send_frames", True)),
        ),
    )
    validate_config(config)
    return config
