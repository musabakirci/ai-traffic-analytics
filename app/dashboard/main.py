from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, time, timedelta, timezone
import os
import sys

import pandas as pd
import streamlit as st

from app.analytics.queries import (
    load_camera_ids,
    load_density_distribution,
    load_emissions_timeseries,
    load_kpis,
    load_vehicle_counts_by_class,
    load_vehicle_timeseries,
    load_vehicle_types,
)
from app.common.config import load_config
from app.db.base import get_engine

logger = logging.getLogger(__name__)


def _is_running_with_streamlit() -> bool:
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        return get_script_run_ctx() is not None
    except Exception:
        return False


def _run_streamlit() -> int:
    from streamlit.web.cli import main as stcli

    sys.argv = ["streamlit", "run", __file__, "--"] + sys.argv[1:]
    try:
        return int(stcli() or 0)
    except SystemExit as exc:
        return int(exc.code or 0)


def _get_config_path() -> str:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config", default=os.getenv("TRAFFIC_AI_CONFIG", "config.yaml"))
    args, _ = parser.parse_known_args()
    return args.config


def _coerce_date_range(date_range: date | tuple[date, date] | list | None) -> tuple[date, date]:
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        return date_range[0], date_range[1]
    if isinstance(date_range, date):
        return date_range, date_range
    today = date.today()
    return today, today


def _build_datetime_range(
    start_date: date, start_time: time, end_date: date, end_time: time
) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(start_date, start_time, tzinfo=timezone.utc)
    end_dt = datetime.combine(end_date, end_time, tzinfo=timezone.utc)
    return start_dt, end_dt


def main() -> int:
    try:
        if not _is_running_with_streamlit():
            return _run_streamlit()
        st.set_page_config(page_title="Akilli Trafik Analizi", layout="wide")
        config = load_config(_get_config_path())
        engine = get_engine(config)

        st.title("Akilli Trafik Yogunlugu ve Emisyon Analizi")

        camera_options = load_camera_ids(engine)
        if not camera_options:
            st.info("No data found. Run the pipeline to populate the database.")
            return 0

        st.sidebar.header("Filters")
        selected_cameras = st.sidebar.multiselect(
            "Camera ID", options=camera_options, default=camera_options
        )
        default_range = (date.today() - timedelta(days=7), date.today())
        date_range = st.sidebar.date_input("Date range", value=default_range)
        start_date, end_date = _coerce_date_range(date_range)
        start_time = st.sidebar.time_input("Start time (UTC)", value=time(0, 0))
        end_time = st.sidebar.time_input("End time (UTC)", value=time(23, 59))

        vehicle_types = load_vehicle_types(engine)
        vehicle_type_options = ["All"] + vehicle_types
        selected_vehicle_type = st.sidebar.selectbox(
            "Vehicle class (optional)", options=vehicle_type_options
        )
        vehicle_filter = None if selected_vehicle_type == "All" else selected_vehicle_type
        st.sidebar.caption("Vehicle class filter applies to count metrics only.")

        if not selected_cameras:
            st.warning("Select at least one camera.")
            return 0

        start_dt, end_dt = _build_datetime_range(start_date, start_time, end_date, end_time)
        if start_dt > end_dt:
            st.error("Start datetime must be before end datetime.")
            return 0
        start_ts = start_dt.isoformat()
        end_ts = end_dt.isoformat()

        kpis = load_kpis(engine, selected_cameras, start_ts, end_ts, vehicle_filter)
        vehicle_ts = load_vehicle_timeseries(
            engine, selected_cameras, start_ts, end_ts, vehicle_filter
        )
        counts_by_class = load_vehicle_counts_by_class(
            engine, selected_cameras, start_ts, end_ts, vehicle_filter
        )
        emissions_ts = load_emissions_timeseries(engine, selected_cameras, start_ts, end_ts)
        density_dist = load_density_distribution(engine, selected_cameras, start_ts, end_ts)

        if vehicle_ts.empty and emissions_ts.empty and density_dist.empty:
            st.warning("No data available for the selected filters.")
            return 0

        total_vehicles = kpis.get("total_vehicles") or 0
        avg_density = kpis.get("avg_density")
        dominant_density = kpis.get("dominant_density") or "N/A"
        total_co2 = kpis.get("total_co2") or 0

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Vehicles", f"{int(total_vehicles)}")
        col2.metric("Average Density", f"{avg_density:.2f}" if avg_density else "N/A")
        col3.metric("Dominant Density", dominant_density)
        col4.metric("Total CO2 (kg)", f"{float(total_co2):.2f}")

        st.subheader("Vehicle Count Over Time")
        if vehicle_ts.empty:
            st.info("No vehicle count data available for the selected filters.")
        else:
            vehicle_ts["bucket_ts"] = pd.to_datetime(vehicle_ts["bucket_ts"], utc=True)
            st.line_chart(vehicle_ts, x="bucket_ts", y="total_count", use_container_width=True)

        st.subheader("Vehicle Count by Class")
        if counts_by_class.empty:
            st.info("No vehicle class data available for the selected filters.")
        else:
            class_chart = counts_by_class.set_index("vehicle_type")["total_count"]
            st.bar_chart(class_chart, use_container_width=True)

        st.subheader("CO2 Emissions Over Time")
        if emissions_ts.empty:
            st.info("No emissions data available for the selected filters.")
        else:
            emissions_ts["bucket_ts"] = pd.to_datetime(emissions_ts["bucket_ts"], utc=True)
            st.area_chart(emissions_ts, x="bucket_ts", y="total_co2", use_container_width=True)

        st.subheader("Density Category Distribution")
        if density_dist.empty:
            st.info("No density data available for the selected filters.")
        else:
            dist_chart = density_dist.set_index("density_level")["bucket_count"]
            st.bar_chart(dist_chart, use_container_width=True)

        with st.expander("Methodology"):
            st.markdown(
                """
                **Bucketed counts**: Vehicle counts are aggregated into fixed time buckets and summed
                from the stored `vehicle_counts` table.

                **Density**: Density categories come from stored `traffic_density` records using
                thresholds low [0, 0.33], medium (0.33, 0.66], high (0.66, 1].

                **Emissions**: CO2 estimates are factor-based (kg per vehicle per minute) and stored
                in `emission_estimates`, scaled by the bucket duration.
                """
            )
        return 0
    except Exception:
        logger.exception("Dashboard failed", extra={"config": _get_config_path()})
        return 1


if __name__ == "__main__":
    if _is_running_with_streamlit():
        main()
    else:
        raise SystemExit(main())
