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
        "model_path": None,
        "device": "cpu",
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
    dummy: DummyConfig = field(default_factory=DummyConfig)


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


def load_config(path: str) -> AppConfig:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    merged = deep_update(DEFAULT_CONFIG, data)
    detector_dict = merged.get("detector", {})
    dummy_dict = detector_dict.get("dummy", {})
    density_dict = merged.get("density", {})
    emissions_dict = merged.get("emissions", {})
    data_paths_dict = merged.get("data_paths", {})
    config = AppConfig(
        frame_sampling_fps=float(merged.get("frame_sampling_fps", 2.0)),
        bucket_seconds=int(merged.get("bucket_seconds", 60)),
        vehicle_class_map={
            str(k).lower(): str(v) for k, v in merged.get("vehicle_class_map", {}).items()
        },
        detector=DetectorConfig(
            name=str(detector_dict.get("name", "dummy")).lower(),
            model_path=detector_dict.get("model_path"),
            device=str(detector_dict.get("device", "cpu")),
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
    )
    validate_config(config)
    return config
