from __future__ import annotations

import logging
from typing import Iterable

from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.db.models import EmissionEstimate, TrafficCamera, TrafficDensity, VehicleCount

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


def insert_vehicle_counts(session: Session, rows: Iterable[dict]) -> None:
    rows_list = list(rows)
    if not rows_list:
        return
    if session.bind and session.bind.dialect.name == "sqlite":
        stmt = sqlite_insert(VehicleCount).values(rows_list)
        stmt = stmt.on_conflict_do_update(
            index_elements=["camera_id", "bucket_ts", "vehicle_type"],
            set_={
                "count": stmt.excluded.count,
                "source_video": stmt.excluded.source_video,
            },
        )
        try:
            session.execute(stmt)
            session.commit()
        except IntegrityError:
            session.rollback()
            logger.exception("Upsert failed for vehicle_counts rows=%s", rows_list)
            raise
        return
    for row in rows_list:
        try:
            session.add(VehicleCount(**row))
            session.commit()
        except IntegrityError:
            session.rollback()
            logger.exception("Insert failed for vehicle_counts row=%s", row)
            raise


def insert_density(session: Session, rows: Iterable[dict]) -> None:
    rows_list = list(rows)
    if not rows_list:
        return
    if session.bind and session.bind.dialect.name == "sqlite":
        stmt = sqlite_insert(TrafficDensity).values(rows_list)
        stmt = stmt.on_conflict_do_update(
            index_elements=["camera_id", "bucket_ts"],
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
            session.commit()
        except IntegrityError:
            session.rollback()
            logger.exception("Upsert failed for traffic_density rows=%s", rows_list)
            raise
        return
    for row in rows_list:
        try:
            session.add(TrafficDensity(**row))
            session.commit()
        except IntegrityError:
            session.rollback()
            logger.exception("Insert failed for traffic_density row=%s", row)
            raise


def insert_emissions(session: Session, rows: Iterable[dict]) -> None:
    rows_list = list(rows)
    if not rows_list:
        return
    if session.bind and session.bind.dialect.name == "sqlite":
        stmt = sqlite_insert(EmissionEstimate).values(rows_list)
        stmt = stmt.on_conflict_do_update(
            index_elements=["camera_id", "bucket_ts"],
            set_={
                "estimated_co2_kg": stmt.excluded.estimated_co2_kg,
                "co2_low_kg": stmt.excluded.co2_low_kg,
                "co2_high_kg": stmt.excluded.co2_high_kg,
                "source_video": stmt.excluded.source_video,
            },
        )
        try:
            session.execute(stmt)
            session.commit()
        except IntegrityError:
            session.rollback()
            logger.exception("Upsert failed for emission_estimates rows=%s", rows_list)
            raise
        return
    for row in rows_list:
        try:
            session.add(EmissionEstimate(**row))
            session.commit()
        except IntegrityError:
            session.rollback()
            logger.exception("Insert failed for emission_estimates row=%s", row)
            raise
