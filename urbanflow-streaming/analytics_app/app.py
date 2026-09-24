"""Streamlit analytics over Gold only (no ETL logic)."""
from __future__ import annotations

import os
from pathlib import Path

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

try:
    from .data_access import (
        HIGH_DEMAND_RATIO,
        LIVE_COLUMNS,
        GoldReader,
        chart_mode_for,
        prepare_live_view,
    )
    from .zones import attach_demand_to_geojson, load_taxi_zone_geojson, load_zone_lookup
    from .session_cache import load_once_per_session
    from .hud import render_glass_cockpit
except ImportError:
    from data_access import (
        HIGH_DEMAND_RATIO,
        LIVE_COLUMNS,
        GoldReader,
        chart_mode_for,
        prepare_live_view,
    )
    from zones import attach_demand_to_geojson, load_taxi_zone_geojson, load_zone_lookup
    from session_cache import load_once_per_session
    from hud import render_glass_cockpit

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HISTORICAL = ROOT / "urbanflow-foundation/fixtures/gold/historical_zone_baseline.jsonl"
DEFAULT_LIVE = ROOT / "urbanflow-foundation/fixtures/gold/expected_live_zone_metrics.jsonl"
DEFAULT_LOOKUP = ROOT / "urbanflow-foundation/fixtures/bronze/zone_lookup.csv"
INTEGRATED_LOOKUP = "s3a://urbanflow/bronze/zone_lookup/taxi_zone_lookup.csv"
TABLE_COLS = [
    "zone_label",
    "window_label",
    "current_trip_count",
    "historical_mean_trip_count",
    "demand_ratio",
    "z_score",
    "is_high_demand",
]


def _paths() -> tuple[str, str, str]:
    with st.sidebar.expander("Data sources", expanded=False):
        historical = st.text_input(
            "Historical Gold",
            os.getenv("URBANFLOW_HISTORICAL_PATH", str(DEFAULT_HISTORICAL)),
        )
        live = st.text_input(
            "Live Gold",
            os.getenv("URBANFLOW_LIVE_PATH", str(DEFAULT_LIVE)),
        )
        default_lookup = INTEGRATED_LOOKUP if live.startswith(("s3://", "s3a://")) else str(DEFAULT_LOOKUP)
        lookup = st.text_input("Zone lookup", os.getenv("URBANFLOW_ZONE_LOOKUP_PATH", default_lookup))
    return historical, live, lookup


def _render_system_links() -> None:
    with st.sidebar.expander("System navigation", expanded=False):
        st.markdown(
            """
            - [FastAPI Health API :8000](http://localhost:8000/v1/streaming/health)
            - [FastAPI OpenAPI Docs :8000](http://localhost:8000/docs)
            - [Spark Driver Web UI :4040](http://localhost:4040) *(Active streaming execution, stages, & query)*
            - [Spark Master Web UI :8080](http://localhost:8080) *(Standalone cluster daemon - idle during local[2] streaming)*
            - [Spark Worker 1 Web UI :8081](http://localhost:8081) *(Standalone cluster worker - idle during local[2] streaming)*
            - [Spark Worker 2 Web UI :8082](http://localhost:8082) *(Standalone cluster worker - idle during local[2] streaming)*
            - [SeaweedFS S3 API :8333](http://localhost:8333) *(S3 API endpoint, not a web console)*
            - [SeaweedFS Filer :8888](http://localhost:8888)
            - [SeaweedFS Master UI :9333](http://localhost:9333)
            """
        )


def _ratio_color() -> alt.Color:
    return alt.Color(
        "demand_ratio:Q",
        scale=alt.Scale(domain=[0, 1, HIGH_DEMAND_RATIO, 3], range=["#2a9d8f", "#e9c46a", "#e76f51", "#c1121f"]),
        legend=alt.Legend(title="Demand ratio"),
    )


def current_activity_chart(frame: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(frame)
        .mark_bar()
        .encode(
            x=alt.X("zone_label:N", sort="-y", title="Pickup zone"),
            y=alt.Y("current_trip_count:Q", title="Current trips"),
            color=_ratio_color(),
            tooltip=["zone_label", "current_trip_count", "historical_mean_trip_count", "demand_ratio", "window_label"],
        )
        .properties(height=320)
    )


def grouped_comparison_chart(frame: pd.DataFrame) -> alt.Chart:
    long = frame.melt(
        id_vars=["zone_label", "window_label"],
        value_vars=["current_trip_count", "historical_mean_trip_count"],
        var_name="series",
        value_name="trips",
    )
    long["series"] = long["series"].map(
        {
            "current_trip_count": "Current",
            "historical_mean_trip_count": "Historical mean",
        }
    )
    return (
        alt.Chart(long)
        .mark_bar()
        .encode(
            x=alt.X("zone_label:N", title="Pickup zone"),
            y=alt.Y("trips:Q", title="Trips"),
            color=alt.Color("series:N", title=""),
            xOffset="series:N",
            tooltip=["zone_label", "series", "trips", "window_label"],
        )
        .properties(height=320)
    )


def line_comparison_chart(frame: pd.DataFrame, category: str) -> alt.Chart:
    long = frame.melt(
        id_vars=["window_start_local", category],
        value_vars=["current_trip_count", "historical_mean_trip_count"],
        var_name="series",
        value_name="trips",
    )
    long["series"] = long["series"].map(
        {
            "current_trip_count": "Current",
            "historical_mean_trip_count": "Historical mean",
        }
    )
    return (
        alt.Chart(long)
        .mark_line(point=True)
        .encode(
            x=alt.X("window_start_local:T", title="Window start (America/New_York)"),
            y=alt.Y("trips:Q", title="Trips"),
            color=alt.Color("series:N", title=""),
            strokeDash=alt.StrokeDash(f"{category}:N", title="Zone"),
            tooltip=["window_start_local", category, "series", "trips"],
        )
        .properties(height=320)
    )


def demand_chart(frame: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(frame)
        .mark_bar()
        .encode(
            x=alt.X("zone_label:N", sort="-y", title="Pickup zone"),
            y=alt.Y("demand_ratio:Q", title="Demand ratio"),
            color=_ratio_color(),
            tooltip=["zone_label", "demand_ratio", "current_trip_count", "historical_mean_trip_count", "z_score"],
        )
        .properties(height=320)
    )


def nyc_map(frame: pd.DataFrame) -> pdk.Deck:
    geo = attach_demand_to_geojson(load_taxi_zone_geojson(), frame)
    return pdk.Deck(
        layers=[
            pdk.Layer(
                "GeoJsonLayer",
                data=geo,
                opacity=0.9,
                stroked=True,
                filled=True,
                get_fill_color="[properties.fill_r, properties.fill_g, properties.fill_b, properties.fill_a]",
                get_line_color=[70, 70, 70],
                line_width_min_pixels=0.7,
                pickable=True,
                auto_highlight=True,
            )
        ],
        initial_view_state=pdk.ViewState(
            latitude=40.75,
            longitude=-73.98,
            zoom=11,
            pitch=0,
        ),
        tooltip={
            "html": "<b>{zone_label}</b><br/>Trips {current_trip_count}<br/>Demand ratio {demand_ratio}",
            "style": {"backgroundColor": "#111", "color": "white"},
        },
        map_provider="carto",
        map_style="light",
    )


def get_cached_nyc_map(latest: pd.DataFrame, selected_ids: tuple[int, ...]) -> pdk.Deck:
    ts = str(latest["window_start_local"].max()) if not latest.empty else "empty"
    cache_key = f"map_{ts}_{hash(selected_ids)}"
    if st.session_state.get("cached_map_key") != cache_key or "cached_map_deck" not in st.session_state:
        st.session_state["cached_map_deck"] = nyc_map(latest)
        st.session_state["cached_map_key"] = cache_key
    return st.session_state["cached_map_deck"]


def show_table(frame: pd.DataFrame, label: str = "Detailed data table") -> None:
    count = len(frame)
    with st.expander(f"📋 {label} ({count:,} rows)", expanded=False):
        pretty = frame.loc[:, [c for c in TABLE_COLS if c in frame.columns]].rename(
            columns={
                "zone_label": "Zone",
                "window_label": "Window (NY)",
                "current_trip_count": "Current trips",
                "historical_mean_trip_count": "Historical mean",
                "demand_ratio": "Demand ratio",
                "z_score": "Z-score",
                "is_high_demand": "High demand",
            }
        )
        if count > 500:
            st.caption(f"Showing first 500 of {count:,} rows to keep rendering fast.")
            st.dataframe(pretty.head(500), width=1200, hide_index=True)
        else:
            st.dataframe(pretty, width=1200, hide_index=True)


st.set_page_config(page_title="UrbanFlow Live", layout="wide")
st.markdown(
    """
    <style>
    @media (max-width: 640px) {
      div[data-testid="stTabs"] div[role="tablist"] {
        flex-wrap: nowrap;
        gap: 0;
      }
      div[data-testid="stTabs"] button[role="tab"] {
        flex: 1 1 0;
        min-width: 0;
        padding: 0.35rem 0.15rem;
        line-height: 1.15;
        text-align: center;
        white-space: normal;
      }
    }
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("UrbanFlow — Live mobility analytics")
st.caption(
    "Five-minute pickup demand vs the historical Gold baseline · clocks are America/New_York · "
    f"high demand is demand ratio > {HIGH_DEMAND_RATIO}"
)

historical_path, live_path, lookup_path = _paths()
reader = GoldReader(historical_path, live_path)

try:
    historical, lookup = load_once_per_session(
        st.session_state,
        (historical_path, lookup_path),
        lambda: (reader.historical(), load_zone_lookup(lookup_path)),
    )
except Exception as exc:
    st.error(f"Unable to read Gold baseline: {exc}")
    st.stop()

if not lookup.empty and "pickup_zone_id" in lookup.columns and "zone_name" in lookup.columns:
    id_to_name = (
        lookup.drop_duplicates("pickup_zone_id")
        .set_index("pickup_zone_id")["zone_name"]
        .to_dict()
    )
else:
    id_to_name = {}

zone_ids = sorted(id_to_name)
selected_ids = st.sidebar.multiselect(
    "Pickup zones",
    options=zone_ids,
    default=zone_ids,
    format_func=lambda z: f"{z}  {id_to_name[z]}",
)
selected_labels = [f"{z} — {id_to_name[z]}" for z in selected_ids]

auto_refresh = st.sidebar.toggle("⚡ Auto-refresh (every 10s)", value=True)

_render_system_links()


@st.fragment(run_every=10 if auto_refresh else None)
def live_dashboard() -> None:
    try:
        live = reader.live()
    except Exception as exc:
        st.warning(f"Temporary Gold read pause: {exc}")
        live = pd.DataFrame(columns=LIVE_COLUMNS)

    render_glass_cockpit(live)

    if live.empty:
        st.info("⏳ Waiting for streaming micro-batches to materialize in Live Gold... (auto-refreshing every 10s)")
        return

    # Keep a rolling window of recent windows for interactive dashboard responsiveness
    # (e.g. most recent 120 five-minute windows = 10 hours of virtual replay)
    if "window_start" in live.columns:
        distinct_starts = live["window_start"].drop_duplicates().sort_values()
        if len(distinct_starts) > 120:
            recent_cutoff = distinct_starts.iloc[-120]
            live_recent = live[live.window_start >= recent_cutoff]
        else:
            live_recent = live
    else:
        live_recent = live

    view = prepare_live_view(live_recent, lookup)
    if view.empty:
        st.info("⏳ Waiting for streaming micro-batches to materialize in Live Gold... (auto-refreshing every 10s)")
        return

    filtered = view[view.pickup_zone_id.isin(selected_ids)] if selected_ids else view.iloc[0:0]

    latest = (
        filtered[filtered.window_start_local == filtered.window_start_local.max()]
        if not filtered.empty
        else filtered
    )
    high = int(latest["is_high_demand"].sum()) if not latest.empty else 0
    latest_label = latest.window_label.iloc[0] if not latest.empty else "—"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Data mode", "Fixture Gold" if live_path.endswith(".jsonl") else "Integrated Gold")
    clock = latest.window_start_local.max().strftime("%H:%M") if not latest.empty else "—"
    c2.metric("Latest window (NY)", clock, help=latest_label)
    c3.metric("Current trips", int(latest.current_trip_count.sum()) if not latest.empty else 0)
    c4.metric("High-demand zones", high, help=f"demand_ratio > {HIGH_DEMAND_RATIO}")

    tab_overview, tab_comparison, tab_explorer, tab_unusual = st.tabs(
        ["Overview", "Current vs historical", "Zone explorer", "Unusual zones"],
        on_change="rerun",
        key="main_dashboard_tabs",
    )

    if tab_overview.open:
        with tab_overview:
            st.subheader("New York taxi-zone demand")
            deck = get_cached_nyc_map(latest, tuple(selected_ids))
            st.pydeck_chart(deck, width=1200, height=520)
            st.caption(
                "TLC taxi-zone polygons. Red = high demand (ratio > 1.5), gold ≈ history, "
                "teal = below history, grey = no live Gold row."
            )
            st.altair_chart(current_activity_chart(latest), width=1200)
            show_table(latest, "Overview zones")

    elif tab_comparison.open:
        with tab_comparison:
            st.subheader("Current trips vs historical mean")
            if filtered.empty:
                st.info("No rows in the current filters.")
            elif chart_mode_for(filtered) == "grouped":
                st.caption("One time window in this slice, so this is a grouped bar — not a trend line.")
                st.altair_chart(grouped_comparison_chart(latest), width=1200)
            else:
                if len(selected_ids) > 5:
                    st.caption(
                        f"Aggregating total demand across all {len(selected_ids)} selected zones over time. "
                        "(Select ≤ 5 zones in the sidebar to view individual zone lines)."
                    )
                    agg = (
                        filtered.groupby(["window_start_local"], as_index=False)
                        .agg({"current_trip_count": "sum", "historical_mean_trip_count": "sum"})
                    )
                    agg["zone_label"] = "Total (All Selected Zones)"
                    st.altair_chart(line_comparison_chart(agg, "zone_label"), width=1200)
                else:
                    st.altair_chart(line_comparison_chart(filtered, "zone_label"), width=1200)
            show_table(filtered, "Comparison slice")

    elif tab_explorer.open:
        with tab_explorer:
            zone = st.selectbox(
                "Zone",
                selected_labels or [f"{z} — {id_to_name[z]}" for z in zone_ids],
                key="explorer_zone_select",
            )
            zone_rows = filtered[filtered.zone_label == zone]
            if zone_rows.empty:
                st.info("No rows for this zone.")
            else:
                row = zone_rows.iloc[-1]
                m1, m2, m3 = st.columns(3)
                m1.metric("Current trips", int(row.current_trip_count))
                m2.metric(
                    "Historical mean",
                    float(row.historical_mean_trip_count)
                    if pd.notna(row.historical_mean_trip_count)
                    else "—",
                )
                m3.metric(
                    "Demand ratio",
                    float(row.demand_ratio) if pd.notna(row.demand_ratio) else "—",
                )
                if chart_mode_for(zone_rows) == "grouped":
                    st.caption("Only one window — bars compare this zone to history. A line needs two or more windows.")
                    st.altair_chart(grouped_comparison_chart(zone_rows), width=1200)
                else:
                    st.altair_chart(line_comparison_chart(zone_rows, "zone_label"), width=1200)
                show_table(zone_rows, f"Rows for {zone}")

    elif tab_unusual.open:
        with tab_unusual:
            ranked = latest.sort_values("demand_ratio", ascending=False, na_position="last")
            st.caption(
                f"High demand is demand_ratio > {HIGH_DEMAND_RATIO}. "
                "Rows without a usable historical mean stay visible and rank last."
            )
            plot = ranked.dropna(subset=["demand_ratio"])
            if plot.empty:
                st.info("No demand-ratio values to rank.")
            else:
                st.altair_chart(demand_chart(plot), width=1200)
            show_table(ranked, "Ranked unusual zones")


live_dashboard()

hist_expander = st.expander("Historical baseline", expanded=False, on_change="rerun", key="hist_baseline_expander")
if hist_expander.open:
    with hist_expander:
        hist = historical[historical.pickup_zone_id.isin(selected_ids)] if selected_ids else historical
        named = lookup.loc[:, ["pickup_zone_id", "zone_name"]]
        hist = hist.merge(named, on="pickup_zone_id", how="left")
        count = len(hist)
        if count > 500:
            st.caption(f"Showing first 500 of {count:,} historical baseline rows to keep rendering fast.")
            st.dataframe(hist.head(500), width=1200, hide_index=True)
        else:
            st.dataframe(hist, width=1200, hide_index=True)


