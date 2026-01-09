from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import inspect

from app.common.config import AppConfig
from app.common.utils import floor_to_bucket, normalize_detections
from app.counting.aggregation import FrameAggregator
from app.db.base import get_engine, get_session_factory
from app.db.repositories import (
    get_max_total_vehicles,
    insert_density,
    insert_emissions,
    insert_vehicle_counts,
    upsert_camera,
)
from app.density.metrics import compute_density_score
from app.detection.factory import create_detector
from app.emissions.factors import estimate_co2_kg
from app.emissions.sensitivity import sensitivity_interval
from app.ingestion.video_reader import iter_sampled_frames

logger = logging.getLogger(__name__)


def ensure_db_initialized(engine) -> None:
    inspector = inspect(engine)
    required_tables = {
        "traffic_cameras",
        "vehicle_counts",
        "traffic_density",
        "emission_estimates",
    }
    existing = set(inspector.get_table_names())
    missing = sorted(required_tables - existing)
    if missing:
        raise RuntimeError(
            "Database not initialized. Missing tables: "
            f"{', '.join(missing)}. Run: python -m app.db.init --config config.yaml"
        )


def run_pipeline(
    video_path: str,
    camera_id: str,
    config: AppConfig,
    source_video: str | None = None,
    camera_location: str | None = None,
    camera_latitude: float | None = None,
    camera_longitude: float | None = None,
    camera_notes: str | None = None,
    start_time: datetime | None = None,
) -> None:
    path = Path(video_path)
    if not path.exists():
        raise FileNotFoundError(f"Video not found: {video_path}")
    source_video = source_video or str(path)
    start_time = start_time or datetime.now(timezone.utc)
    start_time = floor_to_bucket(start_time, config.bucket_seconds)

    engine = get_engine(config)
    ensure_db_initialized(engine)
    session_factory = get_session_factory(engine)

    detector = create_detector(config.detector, list(config.emissions.factors.keys()))
    aggregator = FrameAggregator(bucket_seconds=config.bucket_seconds)

    frames_processed = 0
    for frame, timestamp_sec in iter_sampled_frames(str(path), config.frame_sampling_fps):
        frames_processed += 1
        detections = detector.detect(frame)
        normalized = normalize_detections(detections, config.vehicle_class_map)
        frame_size = (int(frame.shape[1]), int(frame.shape[0]))
        aggregator.add_frame(timestamp_sec, normalized, frame_size)

    if frames_processed == 0:
        logger.error(
            "No frames processed for %s with sampling fps %.2f",
            video_path,
            config.frame_sampling_fps,
        )
        raise RuntimeError("No frames processed; check video path and sampling fps")

    buckets = aggregator.finalize(start_time)
    if not buckets:
        logger.error(
            "No buckets produced for %s with sampling fps %.2f",
            video_path,
            config.frame_sampling_fps,
        )
        raise RuntimeError("No buckets produced; check input data and bucket settings")

    with session_factory() as session:
        upsert_camera(
            session,
            camera_id=camera_id,
            location=camera_location,
            latitude=camera_latitude,
            longitude=camera_longitude,
            notes=camera_notes,
        )

        max_seen = get_max_total_vehicles(session, camera_id)
        rolling_max = max_seen if max_seen is not None else config.density.default_max_vehicles

        counts_rows: list[dict] = []
        density_rows: list[dict] = []
        emission_rows: list[dict] = []

        vehicle_types = sorted(set(config.emissions.factors.keys()) | {"car", "bus", "truck", "motorcycle"})

        for bucket in buckets:
            if config.density.rolling_max:
                rolling_max = max(rolling_max, bucket.total_vehicles)
                max_for_bucket = rolling_max
            else:
                max_for_bucket = config.density.max_vehicles_by_camera.get(
                    camera_id, config.density.default_max_vehicles
                )
            density_result = compute_density_score(
                bucket.total_vehicles,
                max_for_bucket,
                config.density.low_max,
                config.density.medium_max,
            )

            counts_for_bucket = {vehicle: bucket.counts.get(vehicle, 0) for vehicle in vehicle_types}
            co2_estimate = estimate_co2_kg(
                counts_for_bucket, config.emissions.factors, config.bucket_seconds
            )
            co2_low = co2_high = None
            if config.emissions.sensitivity_pct is not None:
                co2_low, co2_high = sensitivity_interval(
                    co2_estimate, config.emissions.sensitivity_pct
                )

            for vehicle_type, count in counts_for_bucket.items():
                counts_rows.append(
                    {
                        "camera_id": camera_id,
                        "bucket_ts": bucket.bucket_ts,
                        "vehicle_type": vehicle_type,
                        "count": int(count),
                        "source_video": source_video,
                    }
                )

            density_rows.append(
                {
                    "camera_id": camera_id,
                    "bucket_ts": bucket.bucket_ts,
                    "total_vehicles": bucket.total_vehicles,
                    "density_score": density_result.density_score,
                    "density_level": density_result.density_level,
                    "bbox_occupancy": bucket.bbox_occupancy,
                    "source_video": source_video,
                }
            )

            emission_rows.append(
                {
                    "camera_id": camera_id,
                    "bucket_ts": bucket.bucket_ts,
                    "estimated_co2_kg": co2_estimate,
                    "co2_low_kg": co2_low,
                    "co2_high_kg": co2_high,
                    "source_video": source_video,
                }
            )

        insert_vehicle_counts(session, counts_rows)
        insert_density(session, density_rows)
        insert_emissions(session, emission_rows)

    logger.info(
        "Processed %s buckets for camera %s from %s",
        len(buckets),
        camera_id,
        video_path,
    )
