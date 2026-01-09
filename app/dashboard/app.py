from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, time, timedelta, timezone
import os
import sys

import pandas as pd
import streamlit as st

from app.analytics.queries import load_density, load_emissions, load_vehicle_counts
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


def _date_range_to_iso(
    date_range: tuple[datetime, datetime] | list | None,
) -> tuple[str | None, str | None]:
    if not isinstance(date_range, (list, tuple)) or len(date_range) != 2:
        return None, None
    start_date, end_date = date_range
    start_dt = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    end_dt = datetime.combine(end_date, time.max, tzinfo=timezone.utc)
    return start_dt.isoformat(), end_dt.isoformat()


def main() -> int:
    try:
        if not _is_running_with_streamlit():
            return _run_streamlit()
        st.set_page_config(page_title="Akilli Trafik Analizi", layout="wide")
        config = load_config(_get_config_path())
        engine = get_engine(config)

        st.title("Akilli Trafik Yogunlugu ve Emisyon Analizi")

        camera_options = pd.read_sql(
            "SELECT DISTINCT camera_id FROM traffic_cameras ORDER BY camera_id", engine
        )["camera_id"].tolist()
        if not camera_options:
            st.info("No data found. Run the pipeline to populate the database.")
            return 0

        st.sidebar.header("Filters")
        camera_id = st.sidebar.selectbox("Camera", options=camera_options)
        default_range = (date.today() - timedelta(days=7), date.today())
        date_range = st.sidebar.date_input("Date range", value=default_range)
        source_videos = pd.read_sql(
            "SELECT DISTINCT source_video FROM vehicle_counts ORDER BY source_video", engine
        )["source_video"].tolist()
        source_video = st.sidebar.selectbox(
            "Source video (optional)", options=["All"] + source_videos
        )
        source_filter = None if source_video == "All" else source_video

        start_ts, end_ts = _date_range_to_iso(date_range)

        counts = load_vehicle_counts(engine, camera_id, start_ts, end_ts, source_filter)
        density = load_density(engine, camera_id, start_ts, end_ts, source_filter)
        emissions = load_emissions(engine, camera_id, start_ts, end_ts, source_filter)

        if counts.empty or density.empty or emissions.empty:
            st.warning("No data available for the selected filters.")
            return 0

        counts["bucket_ts"] = pd.to_datetime(counts["bucket_ts"], utc=True)
        density["bucket_ts"] = pd.to_datetime(density["bucket_ts"], utc=True)
        emissions["bucket_ts"] = pd.to_datetime(emissions["bucket_ts"], utc=True)

        total_vehicles = int(counts["count"].sum())
        avg_density = float(density["density_score"].mean())
        total_co2 = float(emissions["estimated_co2_kg"].sum())

        hourly = (
            counts.assign(hour=counts["bucket_ts"].dt.floor("H"))
            .groupby("hour")["count"]
            .sum()
            .sort_values(ascending=False)
        )
        peak_hour = (
            hourly.index[0].strftime("%Y-%m-%d %H:%M UTC") if not hourly.empty else "N/A"
        )

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Vehicles", f"{total_vehicles}")
        col2.metric("Peak Hour", peak_hour)
        col3.metric("Average Density", f"{avg_density:.2f}")
        col4.metric("Total CO2 (kg)", f"{total_co2:.2f}")

        st.subheader("Traffic Volume Over Time")
        vehicles_ts = counts.groupby("bucket_ts")["count"].sum().reset_index()
        st.line_chart(vehicles_ts, x="bucket_ts", y="count", use_container_width=True)

        st.subheader("Density Score Over Time")
        st.line_chart(density, x="bucket_ts", y="density_score", use_container_width=True)

        st.subheader("CO2 Emissions Over Time")
        if emissions["co2_low_kg"].notna().any():
            import altair as alt

            band = (
                alt.Chart(emissions)
                .mark_area(opacity=0.2)
                .encode(
                    x=alt.X("bucket_ts:T", title="Time"),
                    y=alt.Y("co2_low_kg:Q", title="CO2 kg"),
                    y2="co2_high_kg:Q",
                )
            )
            line = (
                alt.Chart(emissions)
                .mark_line(color="#2c7fb8")
                .encode(x="bucket_ts:T", y="estimated_co2_kg:Q")
            )
            st.altair_chart(band + line, use_container_width=True)
        else:
            st.line_chart(
                emissions, x="bucket_ts", y="estimated_co2_kg", use_container_width=True
            )

        st.subheader("Vehicle Type Breakdown")
        counts["hour"] = counts["bucket_ts"].dt.floor("H")
        pivot = (
            counts.pivot_table(
                index="hour", columns="vehicle_type", values="count", aggfunc="sum"
            )
            .fillna(0)
            .sort_index()
        )
        st.area_chart(pivot, use_container_width=True)

        with st.expander("Methodology"):
            st.markdown(
                """
                **Density**: For each camera, density score is normalized by the maximum vehicles per bucket
                observed so far (rolling max) or a configured maximum. Thresholds: low [0, 0.33],
                medium (0.33, 0.66], high (0.66, 1].

                **Emissions**: CO2 estimates are factor-based (kg per vehicle per minute). Totals are
                scaled by the bucket duration. Sensitivity analysis applies +/- percentage bands.
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
