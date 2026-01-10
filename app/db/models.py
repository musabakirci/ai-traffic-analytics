from __future__ import annotations

from sqlalchemy import Column, Float, ForeignKey, Index, Integer, String, UniqueConstraint

from app.common.utils import utc_now_iso
from app.db.base import Base


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    run_id = Column(String, primary_key=True)
    camera_id = Column(String, ForeignKey("traffic_cameras.camera_id"), nullable=False)
    source_video = Column(String, nullable=False)
    config_hash = Column(String, nullable=False)
    video_fps = Column(Float, nullable=True)
    frame_count = Column(Integer, nullable=True)
    width = Column(Integer, nullable=True)
    height = Column(Integer, nullable=True)
    started_at = Column(String, default=utc_now_iso, nullable=False)
    ended_at = Column(String, nullable=True)
    # Allowed values: running, completed, failed, stopped.
    status = Column(String, nullable=False)
    error_message = Column(String, nullable=True)


class TrafficCamera(Base):
    __tablename__ = "traffic_cameras"

    camera_id = Column(String, primary_key=True)
    location = Column(String, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    notes = Column(String, nullable=True)
    created_at = Column(String, default=utc_now_iso, nullable=False)


class VehicleCount(Base):
    __tablename__ = "vehicle_counts"
    # NOTE: run_id is a breaking schema change required for run-level isolation.
    # NOTE: source_video is duplicated here for bucket-level auditing without joins.
    __table_args__ = (
        UniqueConstraint("run_id", "bucket_ts", "vehicle_type", name="uq_counts"),
        Index("idx_vehicle_counts_run_ts", "run_id", "bucket_ts"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, ForeignKey("pipeline_runs.run_id"), nullable=False)
    camera_id = Column(String, ForeignKey("traffic_cameras.camera_id"), nullable=False)
    bucket_ts = Column(String, nullable=False)
    vehicle_type = Column(String, nullable=False)
    count = Column(Integer, nullable=False)
    source_video = Column(String, nullable=False)
    created_at = Column(String, default=utc_now_iso, nullable=False)


class TrafficDensity(Base):
    __tablename__ = "traffic_density"
    # NOTE: run_id is a breaking schema change required for run-level isolation.
    # NOTE: source_video is duplicated here for bucket-level auditing without joins.
    __table_args__ = (
        UniqueConstraint("run_id", "bucket_ts", name="uq_density"),
        Index("idx_density_run_ts", "run_id", "bucket_ts"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, ForeignKey("pipeline_runs.run_id"), nullable=False)
    camera_id = Column(String, ForeignKey("traffic_cameras.camera_id"), nullable=False)
    bucket_ts = Column(String, nullable=False)
    total_vehicles = Column(Integer, nullable=False)
    density_score = Column(Float, nullable=False)
    density_level = Column(String, nullable=False)
    bbox_occupancy = Column(Float, nullable=True)
    source_video = Column(String, nullable=False)
    created_at = Column(String, default=utc_now_iso, nullable=False)


class EmissionEstimate(Base):
    __tablename__ = "emission_estimates"
    # NOTE: run_id is a breaking schema change required for run-level isolation.
    # NOTE: source_video is duplicated here for bucket-level auditing without joins.
    __table_args__ = (
        UniqueConstraint("run_id", "bucket_ts", name="uq_emissions"),
        Index("idx_emissions_run_ts", "run_id", "bucket_ts"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, ForeignKey("pipeline_runs.run_id"), nullable=False)
    camera_id = Column(String, ForeignKey("traffic_cameras.camera_id"), nullable=False)
    bucket_ts = Column(String, nullable=False)
    estimated_co2_kg = Column(Float, nullable=False)
    co2_low_kg = Column(Float, nullable=True)
    co2_high_kg = Column(Float, nullable=True)
    source_video = Column(String, nullable=False)
    created_at = Column(String, default=utc_now_iso, nullable=False)
    
class ProcessingCheckpoint(Base):
    __tablename__ = "processing_checkpoints"

    run_id = Column(
        String,
        ForeignKey("pipeline_runs.run_id"),
        primary_key=True
    )
    last_bucket_ts = Column(String, nullable=False)
    bucket_index = Column(Integer, nullable=False)
    updated_at = Column(String, default=utc_now_iso, nullable=False)
