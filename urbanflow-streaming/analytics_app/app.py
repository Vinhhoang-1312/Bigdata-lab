"""Streamlit analytics over Gold only (no ETL logic) — Map-First Historical Replay Console."""
from __future__ import annotations

from datetime import timedelta
import os
from pathlib import Path
import time

import altair as alt
import pandas as pd
import pydeck as pdk
from pydeck.bindings import json_tools as _pdk_json_tools
import streamlit as st

# Compact Deck serialization: eliminate pydeck default indent=2 which bloated GeoJSON payloads to 10MB
def _compact_deck_serialize(serializable: object) -> str:
    import json
    return json.dumps(
        serializable,
        sort_keys=True,
        default=_pdk_json_tools.default_serialize,
        separators=(",", ":"),
    )

_pdk_json_tools.serialize = _compact_deck_serialize

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
    from .hud import (
        get_pipeline_telemetry,
        render_observability_drawer,
        render_stream_status_panel,
        status_badge,
    )
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
    from hud import (
        get_pipeline_telemetry,
        render_observability_drawer,
        render_stream_status_panel,
        status_badge,
    )

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HISTORICAL = ROOT / "urbanflow-foundation/fixtures/gold/historical_zone_baseline.jsonl"
DEFAULT_LIVE = ROOT / "urbanflow-foundation/fixtures/gold/expected_live_zone_metrics.jsonl"
DEFAULT_LOOKUP = ROOT / "urbanflow-foundation/fixtures/bronze/zone_lookup.csv"
INTEGRATED_LOOKUP = "s3a://urbanflow/bronze/zone_lookup/taxi_zone_lookup.csv"
TABLE_COLS = [
    "pickup_zone_id",
    "zone_name",
    "borough",
    "window_label",
    "current_trip_count",
    "historical_mean_trip_count",
    "demand_ratio",
    "z_score",
    "is_high_demand",
]


def _render_system_links() -> None:
    with st.expander("🔗 System navigation links", expanded=False):
        st.markdown(
            """
            **Primary Application Endpoints:**
            - [Streamlit Mobility Dashboard :8501](http://localhost:8501)
            - [Spark Driver Web UI :4040](http://localhost:4040) *(Active streaming execution)*
            - [FastAPI Health API :8000](http://localhost:8000/v1/streaming/health)
            - [FastAPI OpenAPI Docs :8000](http://localhost:8000/docs)
            - [SeaweedFS Filer :8888](http://localhost:8888)
            """
        )
        with st.expander("🛠️ Background Cluster Daemons (Operator)", expanded=False):
            st.markdown(
                """
                - [SeaweedFS S3 API :8333](http://localhost:8333) *(S3 API endpoint, not console)*
                - [Spark Master Web UI :8080](http://localhost:8080) *(Standalone cluster daemon, idle during local[2] streaming)*
                - [Spark Worker 1 Web UI :8081](http://localhost:8081)
                - [Spark Worker 2 Web UI :8082](http://localhost:8082)
                - [SeaweedFS Master UI :9333](http://localhost:9333)
                """
            )


def _ratio_color() -> alt.Color:
    return alt.Color(
        "demand_ratio:Q",
        scale=alt.Scale(domain=[0, 1, HIGH_DEMAND_RATIO, 3], range=["#10b981", "#f59e0b", "#ef4444", "#991b1b"]),
        legend=alt.Legend(title="Demand ratio"),
    )


def current_activity_chart(frame: pd.DataFrame) -> alt.Chart:
    # Cap to top 15 zones by volume so the chart is readable and visually balanced
    if frame["zone_label"].nunique() > 15:
        top_labels = frame.sort_values("current_trip_count", ascending=False).head(15)["zone_label"]
        frame = frame[frame.zone_label.isin(top_labels)]
    return (
        alt.Chart(frame)
        .mark_bar()
        .encode(
            x=alt.X("zone_label:N", sort="-y", title="Pickup zone (Top 15 by volume)"),
            y=alt.Y("current_trip_count:Q", title="Replay trips"),
            color=_ratio_color(),
            tooltip=["zone_label", "current_trip_count", "historical_mean_trip_count", "demand_ratio", "window_label"],
        )
        .properties(height=280)
    )


def grouped_comparison_chart(frame: pd.DataFrame) -> alt.Chart:
    # Cap to top 15 zones by volume so comparison bars remain legible
    if frame["zone_label"].nunique() > 15:
        top_labels = frame.sort_values("current_trip_count", ascending=False).head(15)["zone_label"]
        frame = frame[frame.zone_label.isin(top_labels)]
    long = frame.melt(
        id_vars=["zone_label", "window_label"],
        value_vars=["current_trip_count", "historical_mean_trip_count"],
        var_name="series",
        value_name="trips",
    )
    long["series"] = long["series"].map(
        {
            "current_trip_count": "Replay trips",
            "historical_mean_trip_count": "Historical mean",
        }
    )
    return (
        alt.Chart(long)
        .mark_bar()
        .encode(
            x=alt.X("zone_label:N", title="Pickup zone (Top 15)"),
            y=alt.Y("trips:Q", title="Trips"),
            color=alt.Color("series:N", title=""),
            xOffset="series:N",
            tooltip=["zone_label", "series", "trips", "window_label"],
        )
        .properties(height=280)
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
            "current_trip_count": "Replay trips",
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
        .properties(height=280)
    )


def surge_zones_chart(frame: pd.DataFrame) -> alt.Chart | None:
    """Top 15 zones by demand ratio (unusual surge activity)."""
    valid = frame[frame["demand_ratio"].notna() & (frame["current_trip_count"] > 0)]
    if valid.empty:
        return None
    top = valid.sort_values("demand_ratio", ascending=False).head(15)
    return (
        alt.Chart(top)
        .mark_bar()
        .encode(
            x=alt.X("demand_ratio:Q", title="Demand Ratio (Surge vs Baseline)"),
            y=alt.Y("zone_label:N", sort="-x", title="Pickup Zone (Top 15 Surge)"),
            color=_ratio_color(),
            tooltip=["zone_label", "demand_ratio", "current_trip_count", "historical_mean_trip_count", "z_score"],
        )
        .properties(height=280)
    )


def show_table(frame: pd.DataFrame, label: str = "Detailed data table", key: str | None = None) -> None:
    count = len(frame)
    exp_key = key or f"tbl_{abs(hash(label))}"
    expander = st.expander(f"📋 {label} ({count:,} rows)", expanded=False, on_change="rerun", key=exp_key)
    if st.session_state.get(exp_key, False):
        with expander:
            pretty_cols = [c for c in TABLE_COLS if c in frame.columns]
            pretty = frame.loc[:, pretty_cols].rename(
                columns={
                    "pickup_zone_id": "Zone ID",
                    "zone_name": "Zone",
                    "borough": "Borough",
                    "window_label": "Window (NY)",
                    "current_trip_count": "Replay trips",
                    "historical_mean_trip_count": "Historical mean",
                    "demand_ratio": "Demand ratio",
                    "z_score": "Z-score",
                    "is_high_demand": "Surge",
                }
            )
            st.dataframe(pretty.head(500) if count > 500 else pretty, hide_index=True)


def build_deck_map(
    frame: pd.DataFrame,
    metric: str = "demand_ratio",
    basemap: str = "dark",
    enable_3d: bool = False,
    window_id: str | None = None,
) -> pdk.Deck:
    is_dark = basemap != "light"
    geo = attach_demand_to_geojson(
        load_taxi_zone_geojson(),
        frame,
        metric=metric,
        dark_mode=is_dark,
    )
    layer_suffix = f"{abs(hash(str(window_id) + metric + str(enable_3d))) % 100000}"
    return pdk.Deck(
        layers=[
            pdk.Layer(
                "GeoJsonLayer",
                id=f"taxi-zones-layer-{layer_suffix}",
                data=geo,
                opacity=0.92,
                stroked=True,
                filled=True,
                extruded=enable_3d,
                wireframe=enable_3d,
                get_fill_color="[properties.fill_r, properties.fill_g, properties.fill_b, properties.fill_a]",
                get_line_color="[properties.line_r, properties.line_g, properties.line_b, properties.line_a]",
                line_width_min_pixels=1.2,
                get_elevation="properties.elevation",
                elevation_scale=1.0 if enable_3d else 0.0,
                update_triggers={
                    "get_fill_color": [str(window_id), metric],
                    "get_line_color": [str(window_id)],
                    "get_elevation": [str(window_id), metric, enable_3d],
                },
                pickable=True,
                auto_highlight=True,
                highlight_color=[255, 255, 255, 75],
            )
        ],
        initial_view_state=pdk.ViewState(
            latitude=40.75,
            longitude=-73.97,
            zoom=10.8,
            pitch=45 if enable_3d else 0,
            bearing=15 if enable_3d else 0,
            max_zoom=16,
            min_zoom=9,
        ),
        tooltip={
            "html": """
                <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 6px 10px; min-width: 180px;">
                    <div style="font-size: 13px; font-weight: 700; color: #f8fafc; margin-bottom: 2px;">{zone_name}</div>
                    <div style="font-size: 11px; color: #94a3b8; margin-bottom: 8px;">Zone #{location_id} · {borough}</div>
                    <div style="display: flex; justify-content: space-between; font-size: 11px; margin-bottom: 3px;">
                        <span style="color: #cbd5e1;">Replay Trips:</span>
                        <b style="color: #38bdf8;">{current_trip_count_str}</b>
                    </div>
                    <div style="display: flex; justify-content: space-between; font-size: 11px; margin-bottom: 3px;">
                        <span style="color: #cbd5e1;">Historical Mean:</span>
                        <span style="color: #f1f5f9;">{historical_mean_str}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; font-size: 11px; margin-bottom: 3px;">
                        <span style="color: #cbd5e1;">Demand Ratio:</span>
                        <b style="color: #f59e0b;">{demand_ratio_str}</b>
                    </div>
                    <div style="display: flex; justify-content: space-between; font-size: 11px;">
                        <span style="color: #cbd5e1;">Z-Score:</span>
                        <span style="color: #f1f5f9;">{z_score_str}</span>
                    </div>
                </div>
            """,
            "style": {
                "backgroundColor": "rgba(15, 23, 42, 0.95)",
                "border": "1px solid #334155",
                "borderRadius": "6px",
                "boxShadow": "0 4px 12px rgba(0, 0, 0, 0.5)",
                "zIndex": "1000",
            },
        },
        map_provider="carto",
        map_style=basemap,
    )


def get_cached_nyc_map(
    window_slice: pd.DataFrame,
    window_key: str,
    selected_ids: tuple[int, ...],
    metric: str = "demand_ratio",
    basemap: str = "dark",
    enable_3d: bool = False,
) -> pdk.Deck:
    cache_key = f"map_{window_key}_{hash(selected_ids)}_{metric}_{basemap}_{enable_3d}"
    if st.session_state.get("cached_map_key") != cache_key or "cached_map_deck" not in st.session_state:
        st.session_state["cached_map_deck"] = build_deck_map(
            window_slice,
            metric=metric,
            basemap=basemap,
            enable_3d=enable_3d,
            window_id=window_key,
        )
        st.session_state["cached_map_key"] = cache_key
    return st.session_state["cached_map_deck"]


# Page configuration and layout setup
st.set_page_config(
    page_title="UrbanFlow — Historical Replay Console",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Custom Mission-Control Dark Aesthetic
st.html(
    """
    <style>
    /* 1. Eliminate Streamlit default fixed header, top decoration, and toolbar so they never obscure the UI */
    header[data-testid="stHeader"],
    [data-testid="stHeader"],
    .stAppHeader,
    [data-testid="stAppHeader"],
    [data-testid="stDecoration"],
    [data-testid="stToolbar"],
    #MainMenu {
        display: none !important;
        visibility: hidden !important;
        height: 0px !important;
        max-height: 0px !important;
        min-height: 0px !important;
        padding: 0px !important;
        margin: 0px !important;
        opacity: 0 !important;
        pointer-events: none !important;
    }

    /* 2. Global background and viewport consistency */
    html, body, .stApp {
        background-color: #0b0f19 !important;
        margin: 0 !important;
        padding: 0 !important;
    }

    /* 3. Compact mission console layout with clean breathing room */
    .block-container {
        padding-top: 2rem !important;
        padding-bottom: 1.5rem !important;
        padding-left: 1.25rem !important;
        padding-right: 1.25rem !important;
        max-width: 100% !important;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.35rem !important;
        font-weight: 700 !important;
    }
    div[data-testid="stMetricLabel"] {
        font-size: 0.8rem !important;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        color: #94a3b8 !important;
    }
    div[data-testid="stMetricHelp"] {
        color: #64748b !important;
    }
    </style>
    """
)

# Data source path resolution
historical_default = os.getenv("URBANFLOW_HISTORICAL_PATH", str(DEFAULT_HISTORICAL))
live_default = os.getenv("URBANFLOW_LIVE_PATH", str(DEFAULT_LIVE))
lookup_default = INTEGRATED_LOOKUP if live_default.startswith(("s3://", "s3a://")) else str(DEFAULT_LOOKUP)
lookup_path = os.getenv("URBANFLOW_ZONE_LOOKUP_PATH", lookup_default)
historical_path = historical_default
live_path = live_default

reader = GoldReader(historical_path, live_path)

try:
    lookup = load_once_per_session(
        st.session_state,
        lookup_path,
        lambda: load_zone_lookup(lookup_path),
    )
except Exception as exc:
    st.error(f"Unable to read Zone lookup: {exc}")
    st.stop()

id_to_name = (
    lookup.drop_duplicates("pickup_zone_id").set_index("pickup_zone_id")["zone_name"].to_dict()
    if not lookup.empty and "pickup_zone_id" in lookup.columns and "zone_name" in lookup.columns
    else {}
)
zone_ids = sorted(id_to_name)

try:
    live = reader.live()
except Exception as exc:
    st.warning(f"Temporary Gold read pause: {exc}")
    live = pd.DataFrame(columns=LIVE_COLUMNS)

# If Live Gold is empty (initial startup / reset), render auto-refreshing waiting room
if live.empty:
    st.markdown(
        """
        <div style="background: linear-gradient(90deg, #0f172a 0%, #1e293b 100%); padding: 10px 18px; border-radius: 6px; border: 1px solid #334155; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">
            <div style="display: flex; align-items: center; gap: 10px;">
                <span style="font-size: 18px; font-weight: 800; letter-spacing: -0.02em; color: #f8fafc;">🚕 URBANFLOW</span>
                <span style="background: #0284c7; color: #ffffff; padding: 2px 10px; border-radius: 9999px; font-size: 11px; font-weight: 700; letter-spacing: 0.08em;">HISTORICAL REPLAY</span>
                <span style="color: #94a3b8; font-size: 12px; font-weight: 500;">NYC TLC Yellow Taxi · Jan–Feb 2023 Frozen Baseline</span>
            </div>
            <div style="display: flex; align-items: center; gap: 14px; font-size: 12px;">
                <div style="color: #cbd5e1;"><b>Status:</b> <span style="color: #f59e0b; font-weight: 600;">INITIALIZING STREAM</span></div>
                <div>🔴 :red[**INITIALIZING**]</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    @st.fragment(run_every=3)
    def render_waiting_room():
        pipe_curr = get_pipeline_telemetry(None)
        st.info("⏳ Initializing streaming pipeline... Waiting for first micro-batch to materialize in Live Gold (polling storage every 3s)...")
        render_stream_status_panel(pipe_curr)
        try:
            check_live = reader.live()
            if not check_live.empty:
                st.rerun()
        except Exception:
            pass

    render_waiting_room()
    _render_system_links()
    st.stop()

# Rolling window of recent windows for interactive responsiveness
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
raw_windows = sorted(view["window_start_local"].drop_duplicates().tolist()) if not view.empty else []

if raw_windows:
    # Strictly uniform 5-minute grid guaranteeing monotonic time progression without gaps
    grid_start = raw_windows[0]
    grid_end = raw_windows[-1]
    uniform_grid = list(pd.date_range(start=grid_start, end=grid_end, freq="5min"))
    # Limit rolling buffer to at most 120 windows (10 hours) for memory and rendering performance
    if len(uniform_grid) > 120:
        distinct_windows = uniform_grid[-120:]
    else:
        distinct_windows = uniform_grid
else:
    distinct_windows = []

latest_window_ts = distinct_windows[-1] if distinct_windows else None
speedup = os.getenv("SPEEDUP") or os.getenv("URBANFLOW_SPEEDUP", "60")
window_labels = [w.strftime("%Y-%m-%d %H:%M NY") for w in distinct_windows]
latest_label = window_labels[-1] if window_labels else None

# State management for virtual time replay scrubber and chronological playback
is_playing = st.session_state.setdefault("replay_is_playing", False)
play_speed = float(st.session_state.setdefault("replay_play_speed", 1.5))
follow_stream = st.session_state.setdefault("follow_stream", True)
current_selected = st.session_state.get("active_window_label")

if not is_playing and (follow_stream or not current_selected or current_selected not in window_labels):
    active_label = latest_label
    st.session_state["active_window_label"] = latest_label
else:
    active_label = current_selected if current_selected in window_labels else latest_label

active_idx = window_labels.index(active_label) if active_label in window_labels else (len(window_labels) - 1 if window_labels else 0)
active_window = distinct_windows[active_idx] if distinct_windows else latest_window_ts

clock_time_default = latest_window_ts.strftime("%Y-%m-%d %H:%M") if latest_window_ts else "—"
if latest_window_ts:
    win_end_default = pd.Timestamp(latest_window_ts) + timedelta(minutes=5)
    window_span_default = f"{latest_window_ts.strftime('%H:%M')} – {win_end_default.strftime('%H:%M')} NY"
else:
    window_span_default = "—"

auto_refresh_enabled = st.session_state.get("control_auto_refresh", True)

# 1. FULL-WIDTH HEADER COMMAND DECK BANNER (Auto-refreshes virtual clock & health badge every 10s)
@st.fragment(run_every=10 if auto_refresh_enabled else None)
def render_header_banner():
    pipe_curr = get_pipeline_telemetry(None)
    state = pipe_curr["health"]["state"]
    tel = pipe_curr["telemetry"]
    max_ev = tel.get("max_event_time") if tel else None
    if max_ev:
        try:
            ev_dt = pd.Timestamp(max_ev).tz_convert("America/New_York")
            clock_str = f"{ev_dt.strftime('%Y-%m-%d %H:%M')} NY"
            win_end = ev_dt + timedelta(minutes=5)
            span_str = f"{ev_dt.strftime('%H:%M')} – {win_end.strftime('%H:%M')} NY"
        except Exception:
            clock_str = f"{clock_time_default} NY"
            span_str = window_span_default
    else:
        clock_str = f"{clock_time_default} NY"
        span_str = window_span_default

    # Auto-advance page if follow_stream is enabled, not currently playing, and a new micro-batch has materialized
    if auto_refresh_enabled and st.session_state.get("follow_stream", True) and not st.session_state.get("replay_is_playing", False) and tel:
        curr_b_id = tel.get("batch_id", -1)
        prev_b_id = st.session_state.get("last_seen_batch_id")
        if prev_b_id is not None and curr_b_id > prev_b_id:
            st.session_state["last_seen_batch_id"] = curr_b_id
            st.rerun()
        st.session_state["last_seen_batch_id"] = curr_b_id

    st.markdown(
        f"""
        <div style="background: linear-gradient(90deg, #0f172a 0%, #1e293b 100%); padding: 10px 18px; border-radius: 6px; border: 1px solid #334155; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px;">
            <div style="display: flex; align-items: center; gap: 10px;">
                <span style="font-size: 18px; font-weight: 800; letter-spacing: -0.02em; color: #f8fafc;">🚕 URBANFLOW</span>
                <span style="background: #0284c7; color: #ffffff; padding: 2px 10px; border-radius: 9999px; font-size: 11px; font-weight: 700; letter-spacing: 0.08em;">HISTORICAL REPLAY</span>
                <span style="color: #94a3b8; font-size: 12px; font-weight: 500;">NYC TLC Yellow Taxi · Jan–Feb 2023 Frozen Baseline</span>
            </div>
            <div style="display: flex; align-items: center; gap: 14px; font-size: 12px;">
                <div style="color: #cbd5e1;"><b>Replay Clock:</b> <span style="color: #38bdf8; font-weight: 600;">{clock_str}</span></div>
                <div style="color: #cbd5e1;"><b>Stream Head:</b> <span style="color: #f1f5f9;">{span_str}</span></div>
                <div style="background: #1e293b; border: 1px solid #475569; padding: 2px 8px; border-radius: 4px; color: #a5f3fc; font-weight: 600;">⚡ {speedup}× Virtual Time</div>
                <div>{status_badge(state)}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

render_header_banner()

# Sequential Chronological Playback Ticker (Older → Newer)
@st.fragment(run_every=play_speed if is_playing else None)
def render_playback_ticker():
    if not st.session_state.get("replay_is_playing", False) or len(window_labels) <= 1:
        return
    now = time.time()
    last_tick = st.session_state.get("last_playback_tick_time", 0.0)
    if now - last_tick >= (play_speed * 0.85):
        st.session_state["last_playback_tick_time"] = now
        curr = st.session_state.get("active_window_label")
        try:
            c_idx = window_labels.index(curr) if curr in window_labels else 0
        except ValueError:
            c_idx = 0
        if c_idx < len(window_labels) - 1:
            st.session_state["active_window_label"] = window_labels[c_idx + 1]
            st.session_state["follow_stream"] = (c_idx + 1 == len(window_labels) - 1)
            st.rerun(scope="app")
        else:
            # Reached live head! Halt playback cleanly at newest window
            st.session_state["replay_is_playing"] = False
            st.session_state["follow_stream"] = True
            st.rerun(scope="app")

render_playback_ticker()

# 2. THREE-COLUMN MAIN GRID: Controls (2.2) | Central NYC Map (6.8) | Insights & Status (3.0)
col_controls, col_map, col_insights = st.columns([2.2, 6.8, 3.0], gap="small")

with col_controls:
    with st.container(border=True):
        st.markdown(
            "<div style='font-size: 13px; font-weight: 700; letter-spacing: 0.05em; color: #94a3b8; text-transform: uppercase; margin-bottom: 6px;'>"
            "🎛️ CONTROLS</div>",
            unsafe_allow_html=True,
        )

        st.markdown("<div style='font-size: 12px; font-weight: 600; color: #cbd5e1;'>Choropleth Metric</div>", unsafe_allow_html=True)
        selected_metric = st.selectbox(
            "Map Metric",
            options=["demand_ratio", "current_trip_count", "z_score"],
            format_func=lambda m: {
                "demand_ratio": "Demand Ratio (Surge vs Baseline)",
                "current_trip_count": "Replay Trips (Volume)",
                "z_score": "Z-Score (Statistical Anomaly)",
            }.get(m, m),
            key="control_metric",
            label_visibility="collapsed",
        )

        c_basemap, c_3d = st.columns(2)
        with c_basemap:
            st.markdown("<div style='font-size: 12px; font-weight: 600; color: #cbd5e1;'>Basemap</div>", unsafe_allow_html=True)
            basemap = st.selectbox(
                "Basemap Style",
                options=["dark", "light", "road"],
                format_func=lambda s: {"dark": "🌙 Dark Ops", "light": "☀️ Day Light", "road": "🗺️ Road"}.get(s, s),
                key="control_basemap",
                label_visibility="collapsed",
            )
        with c_3d:
            st.markdown("<div style='font-size: 12px; font-weight: 600; color: #cbd5e1;'>Elevation</div>", unsafe_allow_html=True)
            enable_3d = st.checkbox("3D Columns", value=False, key="control_3d", help="Elevate active zones in 3D based on trip volume")

        st.divider()

        # Borough Filter
        st.markdown("<div style='font-size: 12px; font-weight: 600; color: #cbd5e1;'>Borough Filter</div>", unsafe_allow_html=True)
        borough_options = ["All Boroughs"]
        if "borough" in lookup.columns:
            existing_boroughs = sorted([b for b in lookup["borough"].dropna().unique() if b])
            borough_options.extend(existing_boroughs)
        selected_borough = st.selectbox("Borough", options=borough_options, key="control_borough", label_visibility="collapsed")

        if selected_borough != "All Boroughs" and "borough" in lookup.columns:
            borough_zone_ids = sorted(lookup[lookup.borough == selected_borough]["pickup_zone_id"].unique().tolist())
        else:
            borough_zone_ids = zone_ids

        with st.expander("Filter Specific Zones", expanded=False):
            specific_zones = st.multiselect(
                "Specific zones",
                options=borough_zone_ids,
                default=[],
                format_func=lambda z: f"{z} {id_to_name.get(z, '')}",
                key="control_specific_zones",
                label_visibility="collapsed",
                help="Leave empty to include all zones in this borough",
            )

        active_zone_ids = [z for z in specific_zones if z in borough_zone_ids] if specific_zones else borough_zone_ids

        st.divider()

        auto_refresh = st.toggle("⚡ Auto-refresh replay (10s)", value=True, key="control_auto_refresh")

        _render_system_links()

        with st.expander("⚙️ Storage paths", expanded=False):
            st.caption(f"Historical: {historical_path}")
            st.caption(f"Live: {live_path}")

filtered = view[view.pickup_zone_id.isin(active_zone_ids)] if active_zone_ids else view

window_slice = (
    filtered[filtered.window_start_local == active_window]
    if not filtered.empty and active_window is not None
    else filtered
)

def nav_step_to(target_label: str, follow: bool = False, playing: bool = False):
    st.session_state["active_window_label"] = target_label
    st.session_state["follow_stream"] = follow
    st.session_state["replay_is_playing"] = playing
    if playing:
        st.session_state["last_playback_tick_time"] = time.time()

with col_map:
    # 1. Mission Control Replay Navigation Bar directly above the map
    nav_old, nav_prev, nav_play, nav_info, nav_next, nav_live = st.columns([1.0, 1.0, 1.2, 3.6, 1.0, 1.2])
    with nav_old:
        st.button(
            "⏮️ Oldest",
            key="btn_nav_oldest",
            help="Jump to earliest replay window in buffer",
            disabled=len(distinct_windows) <= 1 or (active_idx == 0 and not is_playing),
            on_click=nav_step_to,
            args=(window_labels[0], False, False),
        )
    with nav_prev:
        prev_idx = max(0, active_idx - 1)
        st.button(
            "◀ -5m",
            key="btn_nav_prev",
            help="Step back 5 minutes",
            disabled=len(distinct_windows) <= 1 or (active_idx == 0 and not is_playing),
            on_click=nav_step_to,
            args=(window_labels[prev_idx], False, False),
        )
    with nav_play:
        if is_playing:
            st.button(
                "⏸️ Pause",
                key="btn_nav_pause",
                help="Pause automatic chronological replay",
                on_click=nav_step_to,
                args=(active_label, False, False),
            )
        else:
            target_start = window_labels[0] if active_idx >= len(window_labels) - 1 else active_label
            st.button(
                "▶ Play",
                key="btn_nav_play",
                help="Play forward chronologically (older to newer in 5-min steps)",
                disabled=len(distinct_windows) <= 1,
                on_click=nav_step_to,
                args=(target_start, False, True),
            )
    with nav_info:
        if is_playing:
            status_tag = "▶ PLAYING"
            status_color = "#38bdf8"
        elif follow_stream:
            status_tag = "🟢 LIVE HEAD"
            status_color = "#10b981"
        else:
            status_tag = "⏸️ LOCKED"
            status_color = "#f59e0b"
        total_w_count = len(distinct_windows)
        pos_str = f"({active_idx + 1}/{total_w_count})" if total_w_count > 0 else ""
        st.markdown(
            f"<div style='text-align: center; background: #0f172a; padding: 5px 8px; border-radius: 6px; border: 1px solid #334155; font-size: 11px;'>"
            f"<span style='color: #cbd5e1;'>Window: </span><b style='color: #38bdf8;'>{active_label or '—'}</b> {pos_str} "
            f"<span style='color: {status_color}; font-weight: 700; margin-left: 6px;'>{status_tag}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )
    with nav_next:
        next_idx = min(len(distinct_windows) - 1, active_idx + 1)
        is_head = (next_idx == len(distinct_windows) - 1)
        st.button(
            "+5m ▶",
            key="btn_nav_next",
            help="Step forward 5 minutes",
            disabled=len(distinct_windows) <= 1 or (active_idx >= len(distinct_windows) - 1 and not is_playing),
            on_click=nav_step_to,
            args=(window_labels[next_idx], is_head, False),
        )
    with nav_live:
        st.button(
            "⏩ Live Head",
            key="btn_nav_live",
            help="Jump to latest streaming replay window",
            disabled=len(distinct_windows) <= 1 or (follow_stream and not is_playing),
            on_click=nav_step_to,
            args=(latest_label, True, False),
        )

    deck = get_cached_nyc_map(
        window_slice,
        str(active_window),
        tuple(active_zone_ids),
        metric=selected_metric,
        basemap=basemap,
        enable_3d=enable_3d,
    )
    st.pydeck_chart(deck, width="stretch", height=540)

    # Map Legend & Controls
    active_zones_count = int((window_slice["current_trip_count"] > 0).sum()) if not window_slice.empty else 0
    total_view_zones = len(active_zone_ids) if active_zone_ids else len(zone_ids)

    leg_c1, leg_c2 = st.columns([7.8, 2.2])
    with leg_c1:
        st.markdown(
            f"<div style='background: #0f172a; padding: 6px 12px; border-radius: 6px; border: 1px solid #1e293b; font-size: 11px; color: #94a3b8; margin-top: 2px;'>"
            f"<span style='color: #ef4444; font-weight: 700;'>■</span> Surge (>1.5×) &nbsp; "
            f"<span style='color: #f59e0b; font-weight: 700;'>■</span> Baseline (1.0–1.5×) &nbsp; "
            f"<span style='color: #10b981; font-weight: 700;'>■</span> Below Baseline (&lt;1.0×) &nbsp; "
            f"<span style='color: #475569; font-weight: 700;'>■</span> Inactive &nbsp;·&nbsp; "
            f"<b>{active_zones_count}</b> active / <b>{total_view_zones}</b> monitored"
            f"</div>",
            unsafe_allow_html=True,
        )
    with leg_c2:
        if st.button("🔄 Sync Map", key="btn_sync_map", help="Update map view"):
            st.rerun()

with col_insights:
    # 1. Fast Window Insights KPIs (synchronized with active_window)
    kpi_slice = window_slice
    total_trips = int(kpi_slice["current_trip_count"].sum()) if not kpi_slice.empty else 0
    active_kpi_count = int((kpi_slice["current_trip_count"] > 0).sum()) if not kpi_slice.empty else 0
    surge_count = int(kpi_slice["is_high_demand"].sum()) if not kpi_slice.empty else 0
    max_ratio = float(kpi_slice["demand_ratio"].max()) if not kpi_slice.empty and kpi_slice["demand_ratio"].notna().any() else None

    with st.container(border=True):
        st.markdown(
            f"<div style='font-size: 13px; font-weight: 700; letter-spacing: 0.05em; color: #94a3b8; text-transform: uppercase; margin-bottom: 6px;'>"
            f"WINDOW INSIGHTS <span style='font-size: 10px; color: #38bdf8; font-weight: 600;'>({active_label or '—'})</span></div>",
            unsafe_allow_html=True,
        )

        m1, m2 = st.columns(2)
        with m1:
            st.metric("Replay Trips", f"{total_trips:,}", help="Total pickups in this 5-minute replay window")
            st.metric("Active Zones", f"{active_kpi_count}", help="NYC taxi zones with at least 1 pickup in this window")
        with m2:
            st.metric("Surge Zones", f"{surge_count}", help=f"Zones with demand ratio > {HIGH_DEMAND_RATIO}")
            max_str = f"{max_ratio:.2f}×" if max_ratio is not None else "—"
            st.metric("Peak Surge", max_str, help="Maximum demand ratio recorded across all zones in this window")

        st.markdown("<hr style='margin: 8px 0; border: none; border-top: 1px solid #1e293b;' />", unsafe_allow_html=True)

        st.markdown("<div style='font-size: 12px; font-weight: 600; color: #e2e8f0; margin-bottom: 4px;'>Top Active Zones</div>", unsafe_allow_html=True)
        top_active = (
            kpi_slice[kpi_slice.current_trip_count > 0]
            .sort_values("current_trip_count", ascending=False)
            .head(5)
            if not kpi_slice.empty
            else pd.DataFrame()
        )

        if not top_active.empty:
            for idx, r in enumerate(top_active.itertuples(), start=1):
                ratio_str = f"{r.demand_ratio:.2f}×" if pd.notna(r.demand_ratio) else "—"
                badge_color = "#ef4444" if pd.notna(r.demand_ratio) and r.demand_ratio > HIGH_DEMAND_RATIO else "#38bdf8"
                st.markdown(
                    f"<div style='display: flex; justify-content: space-between; align-items: center; padding: 3px 0; font-size: 11px; border-bottom: 1px solid #1e293b;'>"
                    f"<span style='color: #cbd5e1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 140px;'>"
                    f"<b>#{idx}</b> {r.zone_name}</span>"
                    f"<span><b style='color: #f8fafc;'>{int(r.current_trip_count)} trips</b> "
                    f"<span style='color: {badge_color}; font-weight: 600;'>({ratio_str})</span></span>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
        else:
            st.caption("No pickup activity recorded in this window.")

        historical_expected = (
            float(kpi_slice["historical_mean_trip_count"].sum())
            if not kpi_slice.empty and kpi_slice["historical_mean_trip_count"].notna().any()
            else None
        )
        if historical_expected is not None and historical_expected > 0:
            diff_pct = ((total_trips - historical_expected) / historical_expected) * 100
            diff_color = "#ef4444" if diff_pct > 25 else ("#10b981" if diff_pct < -25 else "#f59e0b")
            st.caption(
                f"Baseline expected: **{historical_expected:.0f} trips** · "
                f"<span style='color: {diff_color}; font-weight: 600;'>{diff_pct:+.1f}% vs baseline</span>",
                unsafe_allow_html=True,
            )

    # 2. Fast 10-second Telemetry status (reads local JSON in 0.2ms, no S3!)
    @st.fragment(run_every=10 if auto_refresh_enabled else None)
    def render_telemetry_hud() -> None:
        pipe_curr = get_pipeline_telemetry(None)
        render_stream_status_panel(pipe_curr)

    render_telemetry_hud()

# 3. FULL-WIDTH REPLAY TIMELINE SCROLLER
with st.container(border=True):
    tl_c1, tl_c2, tl_c3 = st.columns([2.2, 6.8, 3.0])
    with tl_c1:
        st.markdown(
            "<div style='font-size: 12px; font-weight: 700; color: #94a3b8; text-transform: uppercase;'>"
            "⏱️ REPLAY TIMELINE</div>",
            unsafe_allow_html=True,
        )
        st.caption("Frozen TLC: Jan–Feb 2023")

    with tl_c2:
        if len(distinct_windows) > 1:
            def on_scrubber_change():
                val = st.session_state.get("replay_timeline_slider")
                if val in window_labels:
                    st.session_state["active_window_label"] = val
                    st.session_state["follow_stream"] = (val == latest_label)
                    st.session_state["replay_is_playing"] = False

            curr_slider_val = active_label if active_label in window_labels else window_labels[-1]
            st.select_slider(
                "Replay Window Scrubber",
                options=window_labels,
                value=curr_slider_val,
                key="replay_timeline_slider",
                on_change=on_scrubber_change,
                label_visibility="collapsed",
            )
            if is_playing:
                mode_desc = "▶ Chronological Playback (Older → Newer)"
                badge_col = "#38bdf8"
            elif follow_stream:
                mode_desc = "🟢 Live Replay Stream (Following Head)"
                badge_col = "#10b981"
            else:
                mode_desc = "⏸️ Historical Replay Window (Locked)"
                badge_col = "#f59e0b"

            st.caption(
                f"Viewing Window: **{active_label}** · 5-min tumbling window · Mode: "
                f"<span style='color: {badge_col}; font-weight: 600;'>{mode_desc}</span>",
                unsafe_allow_html=True,
            )
        else:
            st.select_slider(
                "Replay Window Scrubber",
                options=window_labels if window_labels else ["2023-02-01 00:00 NY"],
                value=window_labels[0] if window_labels else "2023-02-01 00:00 NY",
                disabled=True,
                key="replay_timeline_slider_single",
                label_visibility="collapsed",
            )
            win_label = distinct_windows[0].strftime("%Y-%m-%d %H:%M NY") if distinct_windows else "2023-02-01 00:00 NY"
            st.caption(f"Single replay window loaded (**{win_label}**) · Waiting for next 5-min tumbling window from streaming engine...")

    with tl_c3:
        st.markdown(
            f"<div style='text-align: right; font-size: 12px; color: #cbd5e1; margin-bottom: 2px;'>"
            f"<div><b>Virtual Replay:</b> <span style='color: #38bdf8; font-weight: 600;'>{speedup}× Speed</span></div>"
            f"</div>",
            unsafe_allow_html=True,
        )
        def on_speed_change():
            st.session_state["replay_play_speed"] = st.session_state["control_playback_speed"]

        st.select_slider(
            "Playback Pacing",
            options=[0.8, 1.5, 3.0],
            value=st.session_state.get("replay_play_speed", 1.5),
            format_func=lambda s: {0.8: "⚡ Fast (0.8s)", 1.5: "⏱️ Normal (1.5s)", 3.0: "🐢 Slow (3.0s)"}.get(s, f"{s}s"),
            key="control_playback_speed",
            on_change=on_speed_change,
            label_visibility="collapsed",
            help="Seconds per 5-minute window during chronological ▶ Play animation",
        )

# 4. EXPANDABLE DEEP-DIVE DRAWERS (Lazily evaluated when opened)
exp_charts = st.expander("📈 Trip Activity & Historical Baseline Comparison", expanded=False, on_change="rerun", key="exp_charts")
if st.session_state.get("exp_charts", False):
    with exp_charts:
        tab_vol, tab_surge, tab_trend = st.tabs(["Top Volume Zones", "Surge / Unusual Demand", "Time Series Trend"])
        with tab_vol:
            st.caption("Top 15 pickup zones by replay volume compared with historical baseline.")
            if chart_mode_for(filtered) == "grouped":
                st.altair_chart(grouped_comparison_chart(window_slice), width="stretch")
            else:
                st.altair_chart(current_activity_chart(window_slice), width="stretch")
        with tab_surge:
            st.caption("Zones with highest demand ratio (current trips / historical baseline).")
            surge_chart = surge_zones_chart(window_slice)
            if surge_chart is not None:
                st.altair_chart(surge_chart, width="stretch")
            else:
                st.info("No active surge zones detected in this window.")
        with tab_trend:
            st.caption("Demand over time for selected zones vs historical mean.")
            if len(active_zone_ids) > 5:
                st.caption(f"Aggregating total demand across all {len(active_zone_ids)} selected zones over time.")
                agg = (
                    filtered.groupby(["window_start_local"], as_index=False)
                    .agg({"current_trip_count": "sum", "historical_mean_trip_count": "sum"})
                )
                agg["zone_label"] = "Total Selected Zones"
                st.altair_chart(line_comparison_chart(agg, "zone_label"), width="stretch")
            else:
                st.altair_chart(line_comparison_chart(filtered, "zone_label"), width="stretch")

exp_zone = st.expander("🔍 Zone Explorer & Deep Dive", expanded=False, on_change="rerun", key="exp_zone")
if st.session_state.get("exp_zone", False):
    with exp_zone:
        explorer_pool = filtered if not filtered.empty else view
        if not explorer_pool.empty:
            available_labels = sorted(explorer_pool["zone_label"].dropna().unique().tolist())
            selected_zone_label = st.selectbox("Select Zone to Inspect", options=available_labels, key="explorer_zone_select")
            zone_rows = explorer_pool[explorer_pool.zone_label == selected_zone_label]
            if zone_rows.empty:
                zone_rows = view[view.zone_label == selected_zone_label]

            if zone_rows.empty:
                st.info("No activity recorded for this zone in current replay window.")
            else:
                latest_row = zone_rows.sort_values("window_start_local").iloc[-1]
                zk1, zk2, zk3, zk4 = st.columns(4)
                zk1.metric("Replay Trips", f"{int(latest_row.current_trip_count):,}")
                zk2.metric(
                    "Historical Baseline",
                    f"{latest_row.historical_mean_trip_count:.1f}" if pd.notna(latest_row.historical_mean_trip_count) else "—",
                )
                zk3.metric(
                    "Demand Ratio",
                    f"{latest_row.demand_ratio:.2f}×" if pd.notna(latest_row.demand_ratio) else "—",
                )
                zk4.metric("Z-Score", f"{latest_row.z_score:.2f}" if pd.notna(latest_row.z_score) else "—")

                if len(zone_rows) > 1:
                    st.altair_chart(line_comparison_chart(zone_rows, "zone_label"), width="stretch")
                show_table(zone_rows, f"History for {selected_zone_label}", key="tbl_explorer_zone")
        else:
            st.info("No zone data available to explore.")

show_table(window_slice, "Window Zone Metrics Table", key="tbl_window_slice")

exp_obs = st.expander("🛡️ Data Governance & Observability Drawer", expanded=False, on_change="rerun", key="exp_obs")
if st.session_state.get("exp_obs", False):
    with exp_obs:
        render_observability_drawer(get_pipeline_telemetry(None))
