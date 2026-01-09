from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy.engine import Engine


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
