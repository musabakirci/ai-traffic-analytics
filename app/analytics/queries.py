from __future__ import annotations

from typing import Any, Sequence

import pandas as pd
from sqlalchemy.engine import Engine


def _build_where(
    camera_ids: Sequence[str] | None,
    start_ts: str | None,
    end_ts: str | None,
    vehicle_type: str | None = None,
) -> tuple[str, dict[str, Any]]:
    clauses = ["1=1"]
    params: dict[str, Any] = {}
    if camera_ids:
        placeholders = []
        for idx, camera_id in enumerate(camera_ids):
            key = f"camera_id_{idx}"
            placeholders.append(f":{key}")
            params[key] = camera_id
        clauses.append(f"camera_id IN ({', '.join(placeholders)})")
    if start_ts:
        clauses.append("bucket_ts >= :start_ts")
        params["start_ts"] = start_ts
    if end_ts:
        clauses.append("bucket_ts <= :end_ts")
        params["end_ts"] = end_ts
    if vehicle_type:
        clauses.append("vehicle_type = :vehicle_type")
        params["vehicle_type"] = vehicle_type
    return " AND ".join(clauses), params


def _load_table(
    engine: Engine,
    table: str,
    camera_id: str | None = None,
    start_ts: str | None = None,
    end_ts: str | None = None,
    source_video: str | None = None,
) -> pd.DataFrame:
    clauses = ["1=1"]
    params: dict[str, Any] = {}
    if camera_id:
        clauses.append("camera_id = :camera_id")
        params["camera_id"] = camera_id
    if start_ts:
        clauses.append("bucket_ts >= :start_ts")
        params["start_ts"] = start_ts
    if end_ts:
        clauses.append("bucket_ts <= :end_ts")
        params["end_ts"] = end_ts
    if source_video:
        clauses.append("source_video = :source_video")
        params["source_video"] = source_video
    where_clause = " AND ".join(clauses)
    query = f"SELECT * FROM {table} WHERE {where_clause}"
    return pd.read_sql(query, engine, params=params)


def load_vehicle_counts(
    engine: Engine,
    camera_id: str | None = None,
    start_ts: str | None = None,
    end_ts: str | None = None,
    source_video: str | None = None,
) -> pd.DataFrame:
    return _load_table(engine, "vehicle_counts", camera_id, start_ts, end_ts, source_video)


def load_density(
    engine: Engine,
    camera_id: str | None = None,
    start_ts: str | None = None,
    end_ts: str | None = None,
    source_video: str | None = None,
) -> pd.DataFrame:
    return _load_table(engine, "traffic_density", camera_id, start_ts, end_ts, source_video)


def load_emissions(
    engine: Engine,
    camera_id: str | None = None,
    start_ts: str | None = None,
    end_ts: str | None = None,
    source_video: str | None = None,
) -> pd.DataFrame:
    return _load_table(engine, "emission_estimates", camera_id, start_ts, end_ts, source_video)


def load_camera_ids(engine: Engine) -> list[str]:
    df = pd.read_sql("SELECT DISTINCT camera_id FROM traffic_cameras ORDER BY camera_id", engine)
    return df["camera_id"].tolist()


def load_vehicle_types(engine: Engine) -> list[str]:
    df = pd.read_sql(
        "SELECT DISTINCT vehicle_type FROM vehicle_counts ORDER BY vehicle_type", engine
    )
    return df["vehicle_type"].tolist()


def load_vehicle_timeseries(
    engine: Engine,
    camera_ids: Sequence[str] | None,
    start_ts: str | None,
    end_ts: str | None,
    vehicle_type: str | None = None,
) -> pd.DataFrame:
    where_clause, params = _build_where(camera_ids, start_ts, end_ts, vehicle_type)
    query = (
        "SELECT bucket_ts, SUM(count) AS total_count "
        "FROM vehicle_counts "
        f"WHERE {where_clause} "
        "GROUP BY bucket_ts "
        "ORDER BY bucket_ts"
    )
    return pd.read_sql(query, engine, params=params)


def load_vehicle_counts_by_class(
    engine: Engine,
    camera_ids: Sequence[str] | None,
    start_ts: str | None,
    end_ts: str | None,
    vehicle_type: str | None = None,
) -> pd.DataFrame:
    where_clause, params = _build_where(camera_ids, start_ts, end_ts, vehicle_type)
    query = (
        "SELECT vehicle_type, SUM(count) AS total_count "
        "FROM vehicle_counts "
        f"WHERE {where_clause} "
        "GROUP BY vehicle_type "
        "ORDER BY total_count DESC"
    )
    return pd.read_sql(query, engine, params=params)


def load_emissions_timeseries(
    engine: Engine,
    camera_ids: Sequence[str] | None,
    start_ts: str | None,
    end_ts: str | None,
) -> pd.DataFrame:
    where_clause, params = _build_where(camera_ids, start_ts, end_ts, None)
    query = (
        "SELECT bucket_ts, SUM(estimated_co2_kg) AS total_co2 "
        "FROM emission_estimates "
        f"WHERE {where_clause} "
        "GROUP BY bucket_ts "
        "ORDER BY bucket_ts"
    )
    return pd.read_sql(query, engine, params=params)


def load_density_distribution(
    engine: Engine,
    camera_ids: Sequence[str] | None,
    start_ts: str | None,
    end_ts: str | None,
) -> pd.DataFrame:
    where_clause, params = _build_where(camera_ids, start_ts, end_ts, None)
    query = (
        "SELECT density_level, COUNT(*) AS bucket_count "
        "FROM traffic_density "
        f"WHERE {where_clause} "
        "GROUP BY density_level"
    )
    return pd.read_sql(query, engine, params=params)


def load_kpis(
    engine: Engine,
    camera_ids: Sequence[str] | None,
    start_ts: str | None,
    end_ts: str | None,
    vehicle_type: str | None = None,
) -> dict[str, Any]:
    where_counts, params_counts = _build_where(camera_ids, start_ts, end_ts, vehicle_type)
    counts_query = f"SELECT SUM(count) AS total_count FROM vehicle_counts WHERE {where_counts}"
    counts_df = pd.read_sql(counts_query, engine, params=params_counts)
    total_count = counts_df["total_count"].iloc[0] if not counts_df.empty else None

    where_density, params_density = _build_where(camera_ids, start_ts, end_ts, None)
    density_query = (
        "SELECT AVG(density_score) AS avg_density "
        f"FROM traffic_density WHERE {where_density}"
    )
    density_df = pd.read_sql(density_query, engine, params=params_density)
    avg_density = density_df["avg_density"].iloc[0] if not density_df.empty else None

    dominant_query = (
        "SELECT density_level, COUNT(*) AS bucket_count "
        f"FROM traffic_density WHERE {where_density} "
        "GROUP BY density_level "
        "ORDER BY bucket_count DESC LIMIT 1"
    )
    dominant_df = pd.read_sql(dominant_query, engine, params=params_density)
    dominant_density = (
        dominant_df["density_level"].iloc[0] if not dominant_df.empty else None
    )

    emissions_query = (
        "SELECT SUM(estimated_co2_kg) AS total_co2 "
        f"FROM emission_estimates WHERE {where_density}"
    )
    emissions_df = pd.read_sql(emissions_query, engine, params=params_density)
    total_co2 = emissions_df["total_co2"].iloc[0] if not emissions_df.empty else None

    return {
        "total_vehicles": total_count,
        "avg_density": avg_density,
        "dominant_density": dominant_density,
        "total_co2": total_co2,
    }
