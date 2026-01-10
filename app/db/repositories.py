from __future__ import annotations

import logging
from typing import Iterable

from sqlalchemy import MetaData, Table, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.common.utils import utc_now_iso
from app.db.models import EmissionEstimate, PipelineRun, TrafficCamera, TrafficDensity, VehicleCount

logger = logging.getLogger(__name__)


def upsert_camera(
    session: Session,
    camera_id: str,
    location: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    notes: str | None = None,
) -> None:
    existing = session.get(TrafficCamera, camera_id)
    if existing:
        if location is not None:
            existing.location = location
        if latitude is not None:
            existing.latitude = latitude
        if longitude is not None:
            existing.longitude = longitude
        if notes is not None:
            existing.notes = notes
        session.commit()
        return
    camera = TrafficCamera(
        camera_id=camera_id,
        location=location,
        latitude=latitude,
        longitude=longitude,
        notes=notes,
    )
    session.add(camera)
    session.commit()


def get_max_total_vehicles(session: Session, camera_id: str) -> int | None:
    stmt = select(func.max(TrafficDensity.total_vehicles)).where(
        TrafficDensity.camera_id == camera_id
    )
    result = session.execute(stmt).scalar_one_or_none()
    return int(result) if result is not None else None


def ensure_run_exists(session: Session, run_id: str) -> None:
    if not run_id:
        raise ValueError("run_id is required for bucket writes")
    existing = session.get(PipelineRun, run_id)
    if not existing:
        raise ValueError(f"run_id does not exist: {run_id}")


def _normalize_rows(rows_list: list[dict], run_id: str, table_name: str) -> list[dict]:
    normalized: list[dict] = []
    for row in rows_list:
        row_run_id = row.get("run_id")
        if row_run_id is None:
            row = dict(row)
            row["run_id"] = run_id
        elif row_run_id != run_id:
            raise ValueError(
                f"{table_name} row run_id mismatch: {row_run_id} != {run_id}"
            )
        normalized.append(row)
    return normalized


def _get_checkpoint_table(session: Session) -> Table:
    bind = session.get_bind()
    if bind is None:
        raise RuntimeError("Session is not bound to an engine")
    metadata = MetaData()
    return Table("processing_checkpoints", metadata, autoload_with=bind)


def get_checkpoint(session: Session, run_id: str) -> dict | None:
    if not run_id:
        raise ValueError("run_id is required for checkpoint reads")
    ensure_run_exists(session, run_id)
    table = _get_checkpoint_table(session)
    stmt = select(
        table.c.run_id,
        table.c.last_bucket_ts,
        table.c.bucket_index,
        table.c.updated_at,
    ).where(table.c.run_id == run_id)
    result = session.execute(stmt).mappings().first()
    return dict(result) if result else None


def upsert_checkpoint(
    session: Session, run_id: str, bucket_ts: str, bucket_index: int
) -> None:
    if not run_id:
        raise ValueError("run_id is required for checkpoint writes")
    ensure_run_exists(session, run_id)
    table = _get_checkpoint_table(session)
    values = {
        "run_id": run_id,
        "last_bucket_ts": bucket_ts,
        "bucket_index": int(bucket_index),
        "updated_at": utc_now_iso(),
    }
    if session.bind and session.bind.dialect.name == "sqlite":
        stmt = sqlite_insert(table).values(values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["run_id"],
            set_={
                "last_bucket_ts": stmt.excluded.last_bucket_ts,
                "bucket_index": stmt.excluded.bucket_index,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        try:
            session.execute(stmt)
        except IntegrityError:
            logger.exception("Upsert failed for checkpoint run_id=%s", run_id)
            raise
        return
    if session.bind and session.bind.dialect.name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = pg_insert(table).values(values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["run_id"],
            set_={
                "last_bucket_ts": stmt.excluded.last_bucket_ts,
                "bucket_index": stmt.excluded.bucket_index,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        try:
            session.execute(stmt)
        except IntegrityError:
            logger.exception("Upsert failed for checkpoint run_id=%s", run_id)
            raise
        return
    try:
        session.execute(table.insert().values(values))
    except IntegrityError:
        logger.exception("Insert failed for checkpoint run_id=%s", run_id)
        raise


def insert_vehicle_counts(session: Session, rows: Iterable[dict], run_id: str) -> None:
    rows_list = list(rows)
    if not rows_list:
        return
    if not run_id:
        raise ValueError("run_id is required for vehicle_counts writes")
    ensure_run_exists(session, run_id)
    rows_list = _normalize_rows(rows_list, run_id, "vehicle_counts")
    # Caller manages transaction boundaries for atomic bucket writes.
    if session.bind and session.bind.dialect.name == "sqlite":
        stmt = sqlite_insert(VehicleCount).values(rows_list)
        stmt = stmt.on_conflict_do_update(
            index_elements=["run_id", "bucket_ts", "vehicle_type"],
            set_={
                "count": stmt.excluded.count,
                "source_video": stmt.excluded.source_video,
            },
        )
        try:
            session.execute(stmt)
        except IntegrityError:
            logger.exception("Upsert failed for vehicle_counts rows=%s", rows_list)
            raise
        return
    if session.bind and session.bind.dialect.name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = pg_insert(VehicleCount).values(rows_list)
        stmt = stmt.on_conflict_do_update(
            index_elements=["run_id", "bucket_ts", "vehicle_type"],
            set_={
                "count": stmt.excluded.count,
                "source_video": stmt.excluded.source_video,
            },
        )
        try:
            session.execute(stmt)
        except IntegrityError:
            logger.exception("Upsert failed for vehicle_counts rows=%s", rows_list)
            raise
        return
    for row in rows_list:
        try:
            session.add(VehicleCount(**row))
        except IntegrityError:
            logger.exception("Insert failed for vehicle_counts row=%s", row)
            raise
    try:
        session.flush()
    except IntegrityError:
        logger.exception("Insert failed for vehicle_counts rows=%s", rows_list)
        raise


def insert_density(session: Session, rows: Iterable[dict], run_id: str) -> None:
    rows_list = list(rows)
    if not rows_list:
        return
    if not run_id:
        raise ValueError("run_id is required for traffic_density writes")
    ensure_run_exists(session, run_id)
    rows_list = _normalize_rows(rows_list, run_id, "traffic_density")
    # Caller manages transaction boundaries for atomic bucket writes.
    if session.bind and session.bind.dialect.name == "sqlite":
        stmt = sqlite_insert(TrafficDensity).values(rows_list)
        stmt = stmt.on_conflict_do_update(
            index_elements=["run_id", "bucket_ts"],
            set_={
                "total_vehicles": stmt.excluded.total_vehicles,
                "density_score": stmt.excluded.density_score,
                "density_level": stmt.excluded.density_level,
                "bbox_occupancy": stmt.excluded.bbox_occupancy,
                "source_video": stmt.excluded.source_video,
            },
        )
        try:
            session.execute(stmt)
        except IntegrityError:
            logger.exception("Upsert failed for traffic_density rows=%s", rows_list)
            raise
        return
    if session.bind and session.bind.dialect.name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = pg_insert(TrafficDensity).values(rows_list)
        stmt = stmt.on_conflict_do_update(
            index_elements=["run_id", "bucket_ts"],
            set_={
                "total_vehicles": stmt.excluded.total_vehicles,
                "density_score": stmt.excluded.density_score,
                "density_level": stmt.excluded.density_level,
                "bbox_occupancy": stmt.excluded.bbox_occupancy,
                "source_video": stmt.excluded.source_video,
            },
        )
        try:
            session.execute(stmt)
        except IntegrityError:
            logger.exception("Upsert failed for traffic_density rows=%s", rows_list)
            raise
        return
    for row in rows_list:
        try:
            session.add(TrafficDensity(**row))
        except IntegrityError:
            logger.exception("Insert failed for traffic_density row=%s", row)
            raise
    try:
        session.flush()
    except IntegrityError:
        logger.exception("Insert failed for traffic_density rows=%s", rows_list)
        raise


def insert_emissions(session: Session, rows: Iterable[dict], run_id: str) -> None:
    rows_list = list(rows)
    if not rows_list:
        return
    if not run_id:
        raise ValueError("run_id is required for emission_estimates writes")
    ensure_run_exists(session, run_id)
    rows_list = _normalize_rows(rows_list, run_id, "emission_estimates")
    # Caller manages transaction boundaries for atomic bucket writes.
    if session.bind and session.bind.dialect.name == "sqlite":
        stmt = sqlite_insert(EmissionEstimate).values(rows_list)
        stmt = stmt.on_conflict_do_update(
            index_elements=["run_id", "bucket_ts"],
            set_={
                "estimated_co2_kg": stmt.excluded.estimated_co2_kg,
                "co2_low_kg": stmt.excluded.co2_low_kg,
                "co2_high_kg": stmt.excluded.co2_high_kg,
                "source_video": stmt.excluded.source_video,
            },
        )
        try:
            session.execute(stmt)
        except IntegrityError:
            logger.exception("Upsert failed for emission_estimates rows=%s", rows_list)
            raise
        return
    if session.bind and session.bind.dialect.name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = pg_insert(EmissionEstimate).values(rows_list)
        stmt = stmt.on_conflict_do_update(
            index_elements=["run_id", "bucket_ts"],
            set_={
                "estimated_co2_kg": stmt.excluded.estimated_co2_kg,
                "co2_low_kg": stmt.excluded.co2_low_kg,
                "co2_high_kg": stmt.excluded.co2_high_kg,
                "source_video": stmt.excluded.source_video,
            },
        )
        try:
            session.execute(stmt)
        except IntegrityError:
            logger.exception("Upsert failed for emission_estimates rows=%s", rows_list)
            raise
        return
    for row in rows_list:
        try:
            session.add(EmissionEstimate(**row))
        except IntegrityError:
            logger.exception("Insert failed for emission_estimates row=%s", row)
            raise
    try:
        session.flush()
    except IntegrityError:
        logger.exception("Insert failed for emission_estimates rows=%s", rows_list)
        raise
