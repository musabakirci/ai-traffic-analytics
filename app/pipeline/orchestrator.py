from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import uuid

from sqlalchemy import inspect

from app.common.config import AppConfig
from app.common.utils import floor_to_bucket, normalize_detections, utc_now_iso
from app.counting.aggregation import FrameAggregator
from app.db.base import get_engine, get_session_factory
from app.db.models import PipelineRun
from app.db.repositories import (
    get_max_total_vehicles,
    get_checkpoint,
    insert_density,
    insert_emissions,
    insert_vehicle_counts,
    upsert_checkpoint,
    upsert_camera,
)
from app.density.metrics import compute_density_score
from app.detection.base import StopProcessing
from app.detection.factory import create_detector
from app.emissions.factors import estimate_co2_kg
from app.emissions.sensitivity import sensitivity_interval
from app.ingestion.video_reader import iter_sampled_frames
from app.realtime.client import RealtimeEventPublisher

logger = logging.getLogger(__name__)

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


def ensure_db_initialized(engine) -> None:
    inspector = inspect(engine)
    required_tables = {
        "traffic_cameras",
        "vehicle_counts",
        "traffic_density",
        "emission_estimates",
        "pipeline_runs",
    }
    existing = set(inspector.get_table_names())
    missing = sorted(required_tables - existing)
    if missing:
        raise RuntimeError(
            "Database not initialized. Missing tables: "
            f"{', '.join(missing)}. Run: python -m app.db.init --config config.yaml"
        )


def _stable_config_hash(config: AppConfig) -> str:
    payload = asdict(config)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _get_video_metadata(video_path: str) -> tuple[float | None, int | None, int | None, int | None]:
    if _HAS_CV2:
        assert cv2 is not None
        cap = cv2.VideoCapture(video_path)
        if cap.isOpened():
            fps = cap.get(cv2.CAP_PROP_FPS) or None
            frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT) or None
            width = cap.get(cv2.CAP_PROP_FRAME_WIDTH) or None
            height = cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or None
            cap.release()
            return (
                float(fps) if fps else None,
                int(frame_count) if frame_count else None,
                int(width) if width else None,
                int(height) if height else None,
            )
    if _HAS_IMAGEIO:
        assert imageio is not None
        reader = imageio.get_reader(video_path)
        try:
            meta = reader.get_meta_data()
            fps = meta.get("fps")
            size = meta.get("size")
            width = size[0] if isinstance(size, (list, tuple)) and len(size) >= 2 else None
            height = size[1] if isinstance(size, (list, tuple)) and len(size) >= 2 else None
            frame_count = meta.get("nframes")
            return (
                float(fps) if fps else None,
                int(frame_count) if frame_count else None,
                int(width) if width else None,
                int(height) if height else None,
            )
        finally:
            reader.close()
    return None, None, None, None


def _update_run_status(
    session, run_id: str, status: str, error_message: str | None = None
) -> None:
    run = session.get(PipelineRun, run_id)
    if not run:
        raise RuntimeError(f"pipeline_runs row missing for run_id={run_id}")
    run.status = status
    run.error_message = error_message
    run.ended_at = utc_now_iso()
    session.commit()


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
    config_hash = _stable_config_hash(config)

    engine = get_engine(config)
    ensure_db_initialized(engine)
    session_factory = get_session_factory(engine)

    video_fps, frame_count, width, height = _get_video_metadata(str(path))
    started_at = utc_now_iso()
    run_id: str | None = None
    run_created = False
    run_finalized = False
    resume_checkpoint: dict | None = None
    resume_after_sec: float | None = None

    with session_factory() as session:
        upsert_camera(
            session,
            camera_id=camera_id,
            location=camera_location,
            latitude=camera_latitude,
            longitude=camera_longitude,
            notes=camera_notes,
        )
        existing_completed = (
            session.query(PipelineRun)
            .filter(
                PipelineRun.camera_id == camera_id,
                PipelineRun.source_video == source_video,
                PipelineRun.config_hash == config_hash,
                PipelineRun.status == "completed",
            )
            .order_by(PipelineRun.started_at.desc())
            .first()
        )
        if existing_completed:
            logger.info(
                "Run already completed for camera_id=%s source_video=%s",
                camera_id,
                source_video,
            )
            return
        existing_resume = (
            session.query(PipelineRun)
            .filter(
                PipelineRun.camera_id == camera_id,
                PipelineRun.source_video == source_video,
                PipelineRun.config_hash == config_hash,
                PipelineRun.status.in_(["failed", "stopped"]),
            )
            .order_by(PipelineRun.started_at.desc())
            .first()
        )
        if existing_resume:
            # Resume flow: reuse run_id and transition to running.
            run_id = existing_resume.run_id
            existing_resume.status = "running"
            existing_resume.error_message = None
            existing_resume.ended_at = None
            session.commit()
            run_created = True
        else:
            # New run flow: create run_id and insert pipeline_runs.
            run_id = str(uuid.uuid4())
            if not run_id:
                raise RuntimeError("run_id generation failed")
            session.add(
                PipelineRun(
                    run_id=run_id,
                    camera_id=camera_id,
                    source_video=source_video,
                    config_hash=config_hash,
                    video_fps=video_fps,
                    frame_count=frame_count,
                    width=width,
                    height=height,
                    started_at=started_at,
                    status="running",
                    error_message=None,
                )
            )
            session.commit()
            run_created = True

    if not run_id:
        raise RuntimeError("run_id not initialized")

    with session_factory() as session:
        resume_checkpoint = get_checkpoint(session, run_id)
    if resume_checkpoint is not None:
        # Resume flow: skip buckets up to and including last committed index.
        resume_after_sec = (int(resume_checkpoint["bucket_index"]) + 1) * float(
            config.bucket_seconds
        )

    detector = None
    realtime_publisher = None
    aggregator = FrameAggregator(bucket_seconds=config.bucket_seconds)

    frames_processed = 0
    try:
        if config.realtime.enabled:
            realtime_publisher = RealtimeEventPublisher(
                config.realtime.websocket_url,
                max_width=config.detector.display_resize_width,
                include_frames=config.realtime.send_frames,
            )
        realtime_emitter = realtime_publisher.publish_detections if realtime_publisher else None
        detector = create_detector(
            config.detector,
            list(config.emissions.factors.keys()),
            config.vehicle_class_map,
            realtime_emitter=realtime_emitter,
        )
        for frame, timestamp_sec in iter_sampled_frames(str(path), config.frame_sampling_fps):
            if resume_after_sec is not None and timestamp_sec < resume_after_sec:
                continue
            frames_processed += 1
            try:
                detections = detector.detect(frame)
            except StopProcessing:
                logger.info("Processing stopped by user for %s", video_path)
                break
            normalized = normalize_detections(detections, config.vehicle_class_map)
            frame_size = (int(frame.shape[1]), int(frame.shape[0]))
            aggregator.add_frame(timestamp_sec, normalized, frame_size)
    except Exception as exc:
        if run_created and not run_finalized:
            try:
                with session_factory() as session:
                    # status transition: running -> failed.
                    _update_run_status(session, run_id, "failed", str(exc))
                    run_finalized = True
            except Exception:
                logger.exception("Failed to update run status for run_id=%s", run_id)
        raise
    finally:
        if detector is not None and hasattr(detector, "close"):
            detector.close()
        if realtime_publisher is not None:
            realtime_publisher.close()

    try:
        if frames_processed == 0 and resume_checkpoint is None:
            logger.error(
                "No frames processed for %s with sampling fps %.2f",
                video_path,
                config.frame_sampling_fps,
            )
            raise RuntimeError("No frames processed; check video path and sampling fps")

        buckets = aggregator.finalize(start_time)
        if not buckets and resume_checkpoint is None:
            logger.error(
                "No buckets produced for %s with sampling fps %.2f",
                video_path,
                config.frame_sampling_fps,
            )
            raise RuntimeError("No buckets produced; check input data and bucket settings")

        with session_factory() as session:
            max_seen = get_max_total_vehicles(session, camera_id)
        rolling_max = max_seen if max_seen is not None else config.density.default_max_vehicles

        vehicle_types = sorted(
            set(config.emissions.factors.keys()) | {"car", "bus", "truck", "motorcycle"}
        )

        # Bucket computation (pure logic).
        bucket_payloads: list[dict] = []
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

            counts_for_bucket = {
                vehicle: bucket.counts.get(vehicle, 0) for vehicle in vehicle_types
            }
            co2_estimate = estimate_co2_kg(
                counts_for_bucket, config.emissions.factors, config.bucket_seconds
            )
            co2_low = co2_high = None
            if config.emissions.sensitivity_pct is not None:
                co2_low, co2_high = sensitivity_interval(
                    co2_estimate, config.emissions.sensitivity_pct
                )

            counts_rows = [
                {
                    "run_id": run_id,
                    "camera_id": camera_id,
                    "bucket_ts": bucket.bucket_ts,
                    "vehicle_type": vehicle_type,
                    "count": int(count),
                    "source_video": source_video,
                }
                for vehicle_type, count in counts_for_bucket.items()
            ]

            density_row = {
                "run_id": run_id,
                "camera_id": camera_id,
                "bucket_ts": bucket.bucket_ts,
                "total_vehicles": bucket.total_vehicles,
                "density_score": density_result.density_score,
                "density_level": density_result.density_level,
                "bbox_occupancy": bucket.bbox_occupancy,
                "source_video": source_video,
            }

            emission_row = {
                "run_id": run_id,
                "camera_id": camera_id,
                "bucket_ts": bucket.bucket_ts,
                "estimated_co2_kg": co2_estimate,
                "co2_low_kg": co2_low,
                "co2_high_kg": co2_high,
                "source_video": source_video,
            }

            bucket_payloads.append(
                {
                    "bucket_index": bucket.bucket_index,
                    "bucket_ts": bucket.bucket_ts,
                    "counts_rows": counts_rows,
                    "density_row": density_row,
                    "emission_row": emission_row,
                }
            )

        if resume_checkpoint is not None:
            last_index = int(resume_checkpoint["bucket_index"])
            bucket_payloads = [
                payload for payload in bucket_payloads if payload["bucket_index"] > last_index
            ]
            if not bucket_payloads:
                with session_factory() as session:
                    # status transition: running -> completed.
                    _update_run_status(session, run_id, "completed")
                    run_finalized = True
                return

        # Bucket persistence (transactional).
        for payload in bucket_payloads:
            try:
                with session_factory() as session:
                    # Transaction start.
                    with session.begin():
                        insert_vehicle_counts(session, payload["counts_rows"], run_id)
                        insert_density(session, [payload["density_row"]], run_id)
                        insert_emissions(session, [payload["emission_row"]], run_id)
                        upsert_checkpoint(
                            session,
                            run_id,
                            payload["bucket_ts"],
                            payload["bucket_index"],
                        )
                    # Commit point.
            except Exception:
                # Rollback path: session.begin() rolls back on exception.
                logger.exception(
                    "Bucket transaction failed for run_id=%s bucket_ts=%s",
                    run_id,
                    payload["bucket_ts"],
                )
                raise

        with session_factory() as session:
            # status transition: running -> completed.
            _update_run_status(session, run_id, "completed")
            run_finalized = True
    except Exception as exc:
        if run_created and not run_finalized:
            try:
                with session_factory() as session:
                    # status transition: running -> failed.
                    _update_run_status(session, run_id, "failed", str(exc))
                    run_finalized = True
            except Exception:
                logger.exception("Failed to update run status for run_id=%s", run_id)
        raise

    logger.info(
        "Processed %s buckets for camera %s from %s",
        len(buckets),
        camera_id,
        video_path,
    )
