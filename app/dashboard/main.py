from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, time, timedelta, timezone
import os
import sys

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.analytics.queries import (
    load_camera_ids,
    load_density,
    load_kpis,
    load_vehicle_counts,
    load_vehicle_counts_by_class,
    load_vehicle_types,
)
from app.common.config import load_config
from app.db.base import get_engine

logger = logging.getLogger(__name__)

VEHICLE_COLORS = {
    "car": "#1f77b4",
    "truck": "#d62728",
    "bus": "#ff7f0e",
    "motorcycle": "#2ca02c",
}
DEFAULT_VEHICLE_COLOR = "#6c757d"
VEHICLE_ORDER = ["car", "truck", "bus", "motorcycle"]


def _vehicle_color(vehicle_type: str) -> str:
    return VEHICLE_COLORS.get(vehicle_type, DEFAULT_VEHICLE_COLOR)


def _ordered_vehicle_types(types: list[str]) -> list[str]:
    ordered = [vehicle_type for vehicle_type in VEHICLE_ORDER if vehicle_type in types]
    extras = sorted([vehicle_type for vehicle_type in types if vehicle_type not in ordered])
    return ordered + extras


def _format_delta(current: float | None, previous: float | None) -> tuple[str, str]:
    if previous is None or previous == 0 or current is None:
        return "—", "gray"
    pct = ((current - previous) / previous) * 100.0
    if pct > 0:
        return f"▲ {abs(pct):.1f}%", "green"
    if pct < 0:
        return f"▼ {abs(pct):.1f}%", "red"
    return "■ 0.0%", "gray"


def _format_ts(value: pd.Timestamp | None) -> str:
    if value is None:
        return "N/A"
    return value.strftime("%Y-%m-%d %H:%M")


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
        counts_df_all = load_vehicle_counts(engine, None, start_ts, end_ts)
        counts_by_class = load_vehicle_counts_by_class(
            engine, selected_cameras, start_ts, end_ts, vehicle_filter
        )
        density_df = load_density(engine, None, start_ts, end_ts, None)
        if not counts_df_all.empty and selected_cameras:
            counts_df_all = counts_df_all[counts_df_all["camera_id"].isin(selected_cameras)]
        counts_df_counts = counts_df_all
        if vehicle_filter and not counts_df_counts.empty:
            counts_df_counts = counts_df_counts[counts_df_counts["vehicle_type"] == vehicle_filter]
        if not density_df.empty and selected_cameras:
            density_df = density_df[density_df["camera_id"].isin(selected_cameras)]

        if counts_df_all.empty:
            st.warning("No data available for the selected filters.")
            return 0

        total_vehicles = kpis.get("total_vehicles") or 0
        avg_density = kpis.get("avg_density")
        total_co2 = kpis.get("total_co2") or 0

        total_series = None
        if not counts_df_counts.empty:
            total_series = (
                counts_df_counts.groupby("bucket_ts")["count"].sum().sort_index()
            )
        prev_total = total_series.iloc[-2] if total_series is not None and len(total_series) > 1 else None
        curr_total = total_series.iloc[-1] if total_series is not None and len(total_series) > 0 else None

        density_series = None
        if not density_df.empty:
            density_df = density_df.copy()
            density_df["bucket_ts"] = pd.to_datetime(density_df["bucket_ts"], utc=True)
            density_series = (
                density_df.groupby("bucket_ts")["density_score"].mean().sort_index()
            )
        prev_density = (
            density_series.iloc[-2] if density_series is not None and len(density_series) > 1 else None
        )
        curr_density = (
            density_series.iloc[-1] if density_series is not None and len(density_series) > 0 else None
        )

        co2_series = None
        if not counts_df_all.empty:
            factors = config.emissions.factors
            bucket_minutes = config.bucket_seconds / 60.0
            co2_tmp = counts_df_all.copy()
            co2_tmp["co2_kg"] = co2_tmp.apply(
                lambda row: float(row["count"])
                * float(factors.get(row["vehicle_type"], 0.0))
                * bucket_minutes,
                axis=1,
            )
            co2_series = (
                co2_tmp.groupby("bucket_ts")["co2_kg"].sum().sort_index()
            )
        prev_co2 = co2_series.iloc[-2] if co2_series is not None and len(co2_series) > 1 else None
        curr_co2 = co2_series.iloc[-1] if co2_series is not None and len(co2_series) > 0 else None

        busiest_ts = None
        if total_series is not None and not total_series.empty:
            busiest_ts = pd.to_datetime(total_series.idxmax(), utc=True)
        highest_co2_ts = None
        if co2_series is not None and not co2_series.empty:
            highest_co2_ts = pd.to_datetime(co2_series.idxmax(), utc=True)
        dominant_vehicle = (
            counts_by_class["vehicle_type"].iloc[0]
            if not counts_by_class.empty
            else "N/A"
        )
        st.info(
            f"**Insight:** Busiest interval: {_format_ts(busiest_ts)} | "
            f"Highest CO2 interval: {_format_ts(highest_co2_ts)} | "
            f"Dominant vehicle type: {dominant_vehicle}"
        )


        col1, col2, col3 = st.columns(3)
        col1.metric("Total Vehicles", f"{int(total_vehicles)}")
        delta_text, delta_color = _format_delta(
            float(curr_total) if curr_total is not None else None,
            float(prev_total) if prev_total is not None else None,
        )
        col1.markdown(f":{delta_color}[{delta_text} vs previous bucket]")

        col2.metric("Average Density", f"{avg_density:.2f}" if avg_density is not None else "N/A")
        delta_text, delta_color = _format_delta(
            float(curr_density) if curr_density is not None else None,
            float(prev_density) if prev_density is not None else None,
        )
        col2.markdown(f":{delta_color}[{delta_text} vs previous bucket]")

        col3.metric("Total CO2 Emissions (kg)", f"{float(total_co2):.2f}")
        delta_text, delta_color = _format_delta(
            float(curr_co2) if curr_co2 is not None else None,
            float(prev_co2) if prev_co2 is not None else None,
        )
        col3.markdown(f":{delta_color}[{delta_text} vs previous bucket]")

        st.subheader("Vehicle Count Over Time")
        if counts_df_counts.empty:
            st.info("No vehicle count data available for the selected filters.")
        else:
            counts_df_counts = counts_df_counts.copy()
            counts_df_counts["bucket_ts"] = pd.to_datetime(
                counts_df_counts["bucket_ts"], utc=True
            )
            grouped = (
                counts_df_counts.groupby(["bucket_ts", "vehicle_type"], as_index=False)["count"]
                .sum()
            )
            pivot = grouped.pivot(
                index="bucket_ts", columns="vehicle_type", values="count"
            ).fillna(0.0)
            pivot = pivot.sort_index()
            ordered_types = _ordered_vehicle_types(list(pivot.columns))
            fig_counts = go.Figure()
            for vehicle_type in ordered_types:
                fig_counts.add_trace(
                    go.Scatter(
                        x=pivot.index,
                        y=pivot[vehicle_type],
                        mode="lines",
                        name=vehicle_type,
                        line=dict(color=_vehicle_color(vehicle_type), width=2.5),
                        meta=vehicle_type,
                        hovertemplate=(
                            "Vehicle: %{meta}<br>"
                            "Time: %{x|%Y-%m-%d %H:%M}<br>"
                            "Count: %{y:.0f}<extra></extra>"
                        ),
                    )
                )
            total_per_bucket = pivot.sum(axis=1)
            if not total_per_bucket.empty:
                peak_ts = total_per_bucket.idxmax()
                peak_val = float(total_per_bucket.max())
                fig_counts.add_annotation(
                    x=peak_ts,
                    y=peak_val,
                    text="Peak traffic",
                    showarrow=True,
                    arrowhead=2,
                    ax=0,
                    ay=-40,
                )
            fig_counts.update_layout(
                template="plotly_white",
                legend_title_text="Vehicle Type",
                xaxis_title="Time",
                yaxis_title="Vehicles per Bucket",
            )
            st.plotly_chart(fig_counts, use_container_width=True)

        col_left, col_right = st.columns(2)
        col_left.subheader("Vehicle Distribution")
        if counts_by_class.empty:
            col_left.info("No vehicle class data available for the selected filters.")
        else:
            labels = counts_by_class["vehicle_type"].tolist()
            values = counts_by_class["total_count"].tolist()
            colors = [_vehicle_color(label) for label in labels]
            fig_donut = go.Figure(
                go.Pie(
                    labels=labels,
                    values=values,
                    hole=0.55,
                    textinfo="percent+label",
                    marker=dict(colors=colors),
                    hovertemplate=(
                        "Vehicle: %{label}<br>"
                        "Share: %{percent}<br>"
                        "Count: %{value}<extra></extra>"
                    ),
                )
            )
            fig_donut.update_layout(template="plotly_white", showlegend=False)
            col_left.plotly_chart(fig_donut, use_container_width=True)

        col_right.subheader("Traffic Density")
        density_value = float(avg_density) if avg_density is not None else 0.0
        fig_density = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=density_value,
                number={"valueformat": ".2f"},
                gauge={
                    "axis": {"range": [0, 1]},
                    "bar": {"color": "#34495e"},
                    "steps": [
                        {"range": [0.0, 0.33], "color": "#2ecc71"},
                        {"range": [0.33, 0.66], "color": "#f1c40f"},
                        {"range": [0.66, 1.0], "color": "#e74c3c"},
                    ],
                },
                title={"text": "Traffic Density"},
            )
        )
        fig_density.update_layout(template="plotly_white", height=350)
        col_right.plotly_chart(fig_density, use_container_width=True)
        if avg_density is None:
            col_right.caption("Density data unavailable for the selected filters.")
        elif avg_density <= config.density.low_max:
            col_right.caption("Traffic flowing smoothly.")
        elif avg_density <= config.density.medium_max:
            col_right.caption("Moderate congestion.")
        else:
            col_right.caption("Heavy congestion detected.")

        st.subheader("CO2 Emissions by Vehicle Type")
        if counts_df_all.empty:
            st.info("No emissions data available for the selected filters.")
        else:
            factors = config.emissions.factors
            bucket_minutes = config.bucket_seconds / 60.0
            co2_df = counts_df_all.copy()
            co2_df["bucket_ts"] = pd.to_datetime(co2_df["bucket_ts"], utc=True)
            co2_df["co2_kg"] = co2_df.apply(
                lambda row: float(row["count"])
                * float(factors.get(row["vehicle_type"], 0.0))
                * bucket_minutes,
                axis=1,
            )
            co2_grouped = (
                co2_df.groupby(["bucket_ts", "vehicle_type"], as_index=False)["co2_kg"]
                .sum()
            )
            co2_pivot = co2_grouped.pivot(
                index="bucket_ts", columns="vehicle_type", values="co2_kg"
            ).fillna(0.0)
            co2_pivot = co2_pivot.sort_index()
            ordered_types = _ordered_vehicle_types(list(co2_pivot.columns))
            total_co2_series = co2_pivot.sum(axis=1)
            fig_co2 = go.Figure()
            for vehicle_type in ordered_types:
                percent = (
                    co2_pivot[vehicle_type] / total_co2_series.replace(0.0, pd.NA)
                ).fillna(0.0)
                fig_co2.add_trace(
                    go.Scatter(
                        x=co2_pivot.index,
                        y=co2_pivot[vehicle_type],
                        mode="lines",
                        stackgroup="one",
                        name=vehicle_type,
                        line=dict(color=_vehicle_color(vehicle_type), width=2),
                        meta=vehicle_type,
                        customdata=percent,
                        hovertemplate=(
                            "Vehicle: %{meta}<br>"
                            "Time: %{x|%Y-%m-%d %H:%M}<br>"
                            "CO2: %{y:.3f} kg<br>"
                            "CO2 contribution: %{customdata:.1%}<extra></extra>"
                        ),
                    )
                )
            if not total_co2_series.empty:
                peak_ts = total_co2_series.idxmax()
                peak_val = float(total_co2_series.max())
                fig_co2.add_annotation(
                    x=peak_ts,
                    y=peak_val,
                    text="Highest emission interval",
                    showarrow=True,
                    arrowhead=2,
                    ax=0,
                    ay=-40,
                )
            fig_co2.update_layout(
                template="plotly_white",
                legend_title_text="Vehicle Type",
                xaxis_title="Time",
                yaxis_title="kg CO2",
            )
            st.plotly_chart(fig_co2, use_container_width=True)

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
