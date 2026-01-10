from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select
from sqlalchemy.orm import sessionmaker

from app.common.config import AppConfig
from app.common.schemas import Detection
from app.db.base import Base
from app.db.models import EmissionEstimate, PipelineRun, TrafficDensity, VehicleCount
from app.db.repositories import get_checkpoint
from app.pipeline import orchestrator


class IndexedDetector:
    def __init__(self, detections_by_index: dict[int, list[Detection]]) -> None:
        self._detections = detections_by_index
        self.calls = 0
        self.indexes: list[int] = []

    def detect(self, frame: np.ndarray) -> list[Detection]:
        index = int(frame[0, 0, 0])
        self.calls += 1
        self.indexes.append(index)
        return self._detections[index]

    def close(self) -> None:
        return None


def _make_config(db_path: Path) -> AppConfig:
    config = AppConfig()
    data_paths = replace(config.data_paths, db_path=str(db_path))
    emissions = replace(
        config.emissions,
        factors={"car": 1.0, "truck": 2.0, "bus": 3.0, "motorcycle": 0.5},
        sensitivity_pct=None,
    )
    density = replace(
        config.density,
        default_max_vehicles=4,
        rolling_max=False,
        max_vehicles_by_camera={},
    )
    return replace(
        config,
        data_paths=data_paths,
        emissions=emissions,
        density=density,
        frame_sampling_fps=2.0,
        bucket_seconds=60,
    )


def _create_checkpoint_table(engine) -> None:
    metadata = MetaData()
    Table(
        "processing_checkpoints",
        metadata,
        Column("run_id", String, primary_key=True),
        Column("last_bucket_ts", String, nullable=False),
        Column("bucket_index", Integer, nullable=False),
        Column("updated_at", String, nullable=False),
    )
    metadata.create_all(engine)


def _setup_db(db_url: str) -> None:
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    _create_checkpoint_table(engine)


def _fetch_outputs(db_url: str) -> dict[str, list[tuple]]:
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        counts = [
            tuple(row)
            for row in session.execute(
                select(VehicleCount.bucket_ts, VehicleCount.vehicle_type, VehicleCount.count)
            ).all()
        ]
        density = [
            tuple(row)
            for row in session.execute(
                select(
                    TrafficDensity.bucket_ts,
                    TrafficDensity.total_vehicles,
                    TrafficDensity.density_score,
                    TrafficDensity.density_level,
                )
            ).all()
        ]
        emissions = [
            tuple(row)
            for row in session.execute(
                select(
                    EmissionEstimate.bucket_ts,
                    EmissionEstimate.estimated_co2_kg,
                    EmissionEstimate.co2_low_kg,
                    EmissionEstimate.co2_high_kg,
                )
            ).all()
        ]
    return {
        "counts": sorted(counts),
        "density": sorted(density),
        "emissions": sorted(emissions),
    }


def test_resume_skips_committed_buckets_and_matches_clean_run(tmp_path, monkeypatch):
    timestamps = [0.0, 10.0, 70.0, 130.0]
    detections_by_index = {
        0: [Detection("car", 0.9, (0, 0, 10, 10))],
        1: [Detection("truck", 0.9, (0, 0, 10, 10))],
        2: [Detection("bus", 0.9, (0, 0, 10, 10))],
        3: [Detection("motorcycle", 0.9, (0, 0, 10, 10))],
    }

    def fake_iter_sampled_frames(video_path: str, target_fps: float):
        for index, ts in enumerate(timestamps):
            frame = np.zeros((10, 10, 3), dtype=np.uint8)
            frame[0, 0, 0] = index
            yield frame, ts

    detectors: list[IndexedDetector] = []

    def fake_create_detector(*_args, **_kwargs):
        detector = IndexedDetector(detections_by_index)
        detectors.append(detector)
        return detector

    monkeypatch.setattr(orchestrator, "iter_sampled_frames", fake_iter_sampled_frames)
    monkeypatch.setattr(orchestrator, "_get_video_metadata", lambda *_args: (None, None, None, None))
    monkeypatch.setattr(orchestrator, "create_detector", fake_create_detector)

    db_path = tmp_path / "resume.db"
    config = _make_config(db_path)
    _setup_db(config.db_url)

    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"")
    start_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    fail_bucket_ts = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc).isoformat()

    original_insert_emissions = orchestrator.insert_emissions

    def failing_insert_emissions(session, rows, run_id):
        if rows and rows[0]["bucket_ts"] == fail_bucket_ts:
            raise RuntimeError("forced failure")
        return original_insert_emissions(session, rows, run_id)

    monkeypatch.setattr(orchestrator, "insert_emissions", failing_insert_emissions)

    with pytest.raises(RuntimeError):
        orchestrator.run_pipeline(
            video_path=str(video_path),
            camera_id="CAM_001",
            config=config,
            start_time=start_time,
        )

    engine = create_engine(config.db_url)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        run = session.query(PipelineRun).first()
        assert run is not None
        assert run.status == "failed"
        checkpoint = get_checkpoint(session, run.run_id)
        assert checkpoint is not None
        assert checkpoint["bucket_index"] == 0

    monkeypatch.setattr(orchestrator, "insert_emissions", original_insert_emissions)

    orchestrator.run_pipeline(
        video_path=str(video_path),
        camera_id="CAM_001",
        config=config,
        start_time=start_time,
    )

    with Session() as session:
        run = session.query(PipelineRun).first()
        assert run is not None
        assert run.status == "completed"

    expected_calls = sum(1 for ts in timestamps if ts >= 60.0)
    assert detectors[1].calls == expected_calls

    clean_db_path = tmp_path / "clean.db"
    clean_config = _make_config(clean_db_path)
    _setup_db(clean_config.db_url)

    orchestrator.run_pipeline(
        video_path=str(video_path),
        camera_id="CAM_001",
        config=clean_config,
        start_time=start_time,
    )

    resumed_outputs = _fetch_outputs(config.db_url)
    clean_outputs = _fetch_outputs(clean_config.db_url)
    assert resumed_outputs == clean_outputs
