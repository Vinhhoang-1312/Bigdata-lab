"""Mobility Analytics — map-first NYC taxi demand console."""
from __future__ import annotations

import os
import time

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st

from analytics_app.data_access import (
    HIGH_DEMAND_RATIO,
    LIVE_COLUMNS,
    GoldReader,
    chart_mode_for,
    prepare_live_view,
)
from analytics_app.zones import (
    attach_demand_to_geojson,
    load_taxi_zone_geojson,
    load_zone_lookup,
)
from analytics_app.session_cache import load_once_per_session
from analytics_app.hud import get_pipeline_telemetry, status_badge

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TABLE_COLS = [
    "pickup_zone_id", "zone_name", "borough", "window_label",
    "current_trip_count", "historical_mean_trip_count",
    "demand_ratio", "z_score", "is_high_demand",
]

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
historical_path = st.session_state.get("historical_path", "")
live_path = st.session_state.get("live_path", "")
lookup_path = st.session_state.get("lookup_path", "")

reader = GoldReader(historical_path, live_path)

try:
    lookup = load_once_per_session(
        st.session_state, lookup_path,
        lambda: load_zone_lookup(lookup_path),
    )
except Exception as exc:
    st.error(f"Unable to read zone lookup: {exc}")
    st.stop()

id_to_name = (
    lookup.drop_duplicates("pickup_zone_id")
    .set_index("pickup_zone_id")["zone_name"].to_dict()
    if not lookup.empty and "pickup_zone_id" in lookup.columns and "zone_name" in lookup.columns
    else {}
)
zone_ids = sorted(id_to_name)

try:
    live = reader.live()
except Exception as exc:
    st.warning(f"Temporary Gold read pause: {exc}")
    live = pd.DataFrame(columns=LIVE_COLUMNS)

# ---------------------------------------------------------------------------
# Waiting room (auto-refresh until first micro-batch materializes)
# ---------------------------------------------------------------------------
if live.empty:
    pipe_curr = get_pipeline_telemetry(None)
    state = pipe_curr["health"]["state"]

    st.markdown(
        f":material/local_taxi: **URBANFLOW** · :blue-background[HISTORICAL REPLAY] · "
        f"NYC TLC Yellow Taxi · Jan–Feb 2023 · {status_badge(state)}"
    )
    st.info(
        "Initializing streaming pipeline… "
        "Waiting for the first micro-batch to materialize in Live Gold.",
        icon=":material/hourglass_top:",
    )

    @st.fragment(run_every=3)
    def _poll_live():
        try:
            if not reader.live().empty:
                st.rerun()
        except Exception:
            pass

    _poll_live()
    st.stop()

# ---------------------------------------------------------------------------
# Prepare rolling view (trim to 120 most recent 5-min windows)
# ---------------------------------------------------------------------------
if "window_start" in live.columns:
    distinct_starts = live["window_start"].drop_duplicates().sort_values()
    if len(distinct_starts) > 120:
        live_recent = live[live.window_start >= distinct_starts.iloc[-120]]
    else:
        live_recent = live
else:
    live_recent = live

view = prepare_live_view(live_recent, lookup)
raw_windows = sorted(view["window_start_local"].drop_duplicates().tolist()) if not view.empty else []

if raw_windows:
    grid_start, grid_end = raw_windows[0], raw_windows[-1]
    uniform_grid = list(pd.date_range(start=grid_start, end=grid_end, freq="5min"))
    distinct_windows = uniform_grid[-120:] if len(uniform_grid) > 120 else uniform_grid
else:
    distinct_windows = []

latest_window_ts = distinct_windows[-1] if distinct_windows else None
speedup = os.getenv("SPEEDUP") or os.getenv("URBANFLOW_SPEEDUP", "60")
window_labels = [w.strftime("%Y-%m-%d %H:%M NY") for w in distinct_windows]
latest_label = window_labels[-1] if window_labels else None

# ---------------------------------------------------------------------------
# Replay state management
# ---------------------------------------------------------------------------
is_playing = st.session_state.setdefault("replay_is_playing", False)
play_speed = float(st.session_state.setdefault("replay_play_speed", 1.5))
follow_stream = st.session_state.setdefault("follow_stream", True)
current_selected = st.session_state.get("active_window_label")

if not is_playing and (follow_stream or not current_selected or current_selected not in window_labels):
    active_label = latest_label
    st.session_state["active_window_label"] = latest_label
else:
    active_label = current_selected if current_selected in window_labels else latest_label

active_idx = (
    window_labels.index(active_label)
    if active_label in window_labels
    else (len(window_labels) - 1 if window_labels else 0)
)
active_window = distinct_windows[active_idx] if distinct_windows else latest_window_ts

auto_refresh_enabled = st.session_state.get("control_auto_refresh", True)

# ---------------------------------------------------------------------------
# Sidebar: filters, controls, advanced replay
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("#### Filters")
    selected_metric = st.segmented_control(
        "Choropleth metric",
        options=["demand_ratio", "current_trip_count", "z_score"],
        default="demand_ratio",
        format_func=lambda m: {
            "demand_ratio": "Demand ratio",
            "current_trip_count": "Replay trips",
            "z_score": "Z-score",
        }.get(m, m),
        key="control_metric",
    )
    if not selected_metric:
        selected_metric = "demand_ratio"

    borough_options = ["All boroughs"]
    if "borough" in lookup.columns:
        borough_options.extend(sorted(b for b in lookup["borough"].dropna().unique() if b))
    selected_borough = st.selectbox("Borough", options=borough_options, key="control_borough")

    if selected_borough != "All boroughs" and "borough" in lookup.columns:
        borough_zone_ids = sorted(lookup[lookup.borough == selected_borough]["pickup_zone_id"].unique().tolist())
    else:
        borough_zone_ids = zone_ids

    specific_zones = st.multiselect(
        "Filter specific zones",
        options=borough_zone_ids,
        default=[],
        format_func=lambda z: f"{z} {id_to_name.get(z, '')}",
        key="control_specific_zones",
        help="Leave empty to include all zones in this borough",
    )

    active_zone_ids = [z for z in specific_zones if z in borough_zone_ids] if specific_zones else borough_zone_ids

    st.divider()
    auto_refresh = st.toggle(
        "Auto-refresh",
        value=True,
        key="control_auto_refresh",
        help="Refresh live data every 10 seconds",
    )

    with st.expander("Advanced replay controls", expanded=False):
        def on_speed_change():
            st.session_state["replay_play_speed"] = st.session_state["control_playback_speed"]

        st.select_slider(
            "Playback pacing",
            options=[0.8, 1.5, 3.0],
            value=st.session_state.get("replay_play_speed", 1.5),
            format_func=lambda s: {0.8: "Fast (0.8s)", 1.5: "Normal (1.5s)", 3.0: "Slow (3.0s)"}.get(s, f"{s}s"),
            key="control_playback_speed",
            on_change=on_speed_change,
            help="Seconds per 5-minute window during Play animation",
        )

        st.caption(f"Storage: `{live_path}`")

# ---------------------------------------------------------------------------
# Filter data
# ---------------------------------------------------------------------------
filtered = view[view.pickup_zone_id.isin(active_zone_ids)] if active_zone_ids else view
window_slice = (
    filtered[filtered.window_start_local == active_window]
    if not filtered.empty and active_window is not None
    else filtered
)

# ---------------------------------------------------------------------------
# Header banner (Auto-refreshes replay clock; triggers rerun on new Gold window)
# ---------------------------------------------------------------------------
@st.fragment(run_every=10 if auto_refresh_enabled else None)
def render_header_banner():
    pipe_curr = get_pipeline_telemetry(None)
    tel = pipe_curr["telemetry"]

    # Auto-advance page only when a new completed Gold window materializes
    if (auto_refresh_enabled
            and st.session_state.get("follow_stream", True)
            and not st.session_state.get("replay_is_playing", False)):
        try:
            curr_live = reader.live()
            if not curr_live.empty and "window_start" in curr_live.columns:
                curr_max_win = curr_live["window_start"].max()
                prev_max_win = st.session_state.get("last_seen_gold_window")
                if prev_max_win is not None and curr_max_win is not pd.NaT and curr_max_win > prev_max_win:
                    st.session_state["last_seen_gold_window"] = curr_max_win
                    st.rerun()
                if curr_max_win is not pd.NaT:
                    st.session_state["last_seen_gold_window"] = curr_max_win
        except Exception:
            pass

    # Derive clock strings
    max_ev = tel.get("max_event_time") if tel else None
    if max_ev:
        try:
            ev_dt = pd.Timestamp(max_ev).tz_convert("America/New_York")
            clock_str = ev_dt.strftime("%Y-%m-%d %H:%M NY")
        except Exception:
            clock_str = (latest_window_ts.strftime("%Y-%m-%d %H:%M NY") if latest_window_ts else "—")
    else:
        clock_str = (latest_window_ts.strftime("%Y-%m-%d %H:%M NY") if latest_window_ts else "—")

    tel_age = pipe_curr.get("tel_age")
    age_str = f"updated {tel_age}s ago" if tel_age is not None else ""

    st.markdown(
        f":material/local_taxi: **URBANFLOW** · :blue-background[HISTORICAL REPLAY] · "
        f"Replay clock **{clock_str}** · :material/bolt: {speedup}× virtual time"
        + (f" · {age_str}" if age_str else "")
    )


render_header_banner()

# ---------------------------------------------------------------------------
# Playback ticker (chronological older → newer)
# ---------------------------------------------------------------------------
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
            st.session_state["replay_is_playing"] = False
            st.session_state["follow_stream"] = True
            st.rerun(scope="app")


render_playback_ticker()

# ---------------------------------------------------------------------------
# KPI row: 4 cards
# ---------------------------------------------------------------------------
total_trips = int(window_slice["current_trip_count"].sum()) if not window_slice.empty else 0
active_count = int((window_slice["current_trip_count"] > 0).sum()) if not window_slice.empty else 0
surge_count = int(window_slice["is_high_demand"].sum()) if not window_slice.empty else 0

pipe = get_pipeline_telemetry(None)
pipe_state = pipe["health"]["state"]
pipe_color = {"HEALTHY": "green", "DEGRADED": "orange", "STALE": "orange"}.get(pipe_state, "red")

k1, k2, k3, k4 = st.columns(4)
k1.metric("Replay trips", f"{total_trips:,}", help="Pickups in this 5-minute replay window")
k2.metric("Active zones", f"{active_count}", help="Zones with at least 1 pickup")
k3.metric("Surge zones", f"{surge_count}", help=f"Zones with demand ratio > {HIGH_DEMAND_RATIO}")
k4.metric("Pipeline", f":{pipe_color}[{pipe_state}]", help=pipe["health"]["reason"])

# ---------------------------------------------------------------------------
# Map builder
# ---------------------------------------------------------------------------
def build_deck_map(
    frame: pd.DataFrame,
    metric: str = "demand_ratio",
    window_id: str | None = None,
) -> pdk.Deck:
    geo = attach_demand_to_geojson(load_taxi_zone_geojson(), frame, metric=metric, dark_mode=True)
    layer_suffix = f"{abs(hash(str(window_id) + metric)) % 100000}"
    return pdk.Deck(
        layers=[
            pdk.Layer(
                "GeoJsonLayer",
                id=f"taxi-zones-{layer_suffix}",
                data=geo,
                opacity=0.92,
                stroked=True,
                filled=True,
                extruded=False,
                get_fill_color="[properties.fill_r, properties.fill_g, properties.fill_b, properties.fill_a]",
                get_line_color="[properties.line_r, properties.line_g, properties.line_b, properties.line_a]",
                line_width_min_pixels=1.2,
                update_triggers={
                    "get_fill_color": [str(window_id), metric],
                    "get_line_color": [str(window_id)],
                },
                pickable=True,
                auto_highlight=True,
                highlight_color=[255, 255, 255, 75],
            )
        ],
        initial_view_state=pdk.ViewState(
            latitude=40.75, longitude=-73.97, zoom=10.8,
            pitch=0, bearing=0, max_zoom=16, min_zoom=9,
        ),
        tooltip={
            "html": """
                <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 6px 10px; min-width: 180px;">
                    <div style="font-size: 13px; font-weight: 700; color: #f8fafc;">{zone_name}</div>
                    <div style="font-size: 11px; color: #94a3b8; margin-bottom: 6px;">Zone #{location_id} · {borough}</div>
                    <div style="font-size: 11px; color: #cbd5e1;">Replay trips: <b style="color: #38bdf8;">{current_trip_count_str}</b></div>
                    <div style="font-size: 11px; color: #cbd5e1;">Historical mean: {historical_mean_str}</div>
                    <div style="font-size: 11px; color: #cbd5e1;">Demand ratio: <b style="color: #f59e0b;">{demand_ratio_str}</b></div>
                    <div style="font-size: 11px; color: #cbd5e1;">Z-score: {z_score_str}</div>
                </div>
            """,
            "style": {
                "backgroundColor": "rgba(15, 23, 42, 0.95)",
                "border": "1px solid #334155",
                "borderRadius": "6px",
            },
        },
        map_provider="carto",
        map_style="dark",
    )


def get_cached_map(ws: pd.DataFrame, wkey: str, zids: tuple[int, ...], metric: str) -> pdk.Deck:
    cache_key = f"map_{wkey}_{hash(zids)}_{metric}"
    if st.session_state.get("cached_map_key") != cache_key or "cached_map_deck" not in st.session_state:
        st.session_state["cached_map_deck"] = build_deck_map(ws, metric=metric, window_id=wkey)
        st.session_state["cached_map_key"] = cache_key
    return st.session_state["cached_map_deck"]

# ---------------------------------------------------------------------------
# Main layout: MAP (large) + Top surge zones (sidebar)
# ---------------------------------------------------------------------------

def nav_step_to(target_label: str, follow: bool = False, playing: bool = False):
    st.session_state["active_window_label"] = target_label
    st.session_state["follow_stream"] = follow
    st.session_state["replay_is_playing"] = playing
    if playing:
        st.session_state["last_playback_tick_time"] = time.time()


col_map, col_surge = st.columns([7.5, 2.5])

with col_map:
    deck = get_cached_map(
        window_slice,
        str(active_window),
        tuple(active_zone_ids),
        metric=selected_metric,
    )
    st.pydeck_chart(deck, width="stretch", height=520)

    # Dynamic Legend based on selected metric
    active_zones_str = f"{active_count}" if not window_slice.empty else "0"
    total_zones_str = f"{len(active_zone_ids)}"
    if selected_metric == "current_trip_count":
        legend_scale = ":red[■] ≥50 trips · :violet[■] 25–49 · :blue[■] 10–24 · :teal[■] 1–9 · :gray[■] 0 trips"
    elif selected_metric == "z_score":
        legend_scale = ":red[■] High surge (≥+2.0σ) · :orange[■] Elevated (+1.0 to +2.0σ) · :blue[■] Normal (±1.0σ) · :teal[■] Low (<-1.0σ)"
    else:
        legend_scale = f":red[■] Surge (>{HIGH_DEMAND_RATIO}×) · :orange[■] Baseline (1.0–{HIGH_DEMAND_RATIO}×) · :green[■] Below baseline · :gray[■] Inactive"
    st.caption(f"{legend_scale} · **{active_zones_str}** active / **{total_zones_str}** monitored")

with col_surge:
    st.markdown("##### Highest demand ratios")
    top_active = (
        window_slice[window_slice.current_trip_count > 0]
        .sort_values("demand_ratio", ascending=False, na_position="last")
        .head(8)
        if not window_slice.empty
        else pd.DataFrame()
    )

    if not top_active.empty:
        for idx, r in enumerate(top_active.itertuples(), start=1):
            ratio_str = f"{r.demand_ratio:.2f}×" if pd.notna(r.demand_ratio) else "—"
            is_surge = pd.notna(r.demand_ratio) and r.demand_ratio > HIGH_DEMAND_RATIO
            badge = f":red[{ratio_str}]" if is_surge else ratio_str
            st.markdown(
                f"**{idx}.** {r.zone_name} — **{int(r.current_trip_count)}** trips · {badge}"
            )
    else:
        st.caption("No pickup activity in this window.")

    # Baseline comparison insight
    hist_expected = (
        float(window_slice["historical_mean_trip_count"].sum())
        if not window_slice.empty and window_slice["historical_mean_trip_count"].notna().any()
        else None
    )
    if hist_expected is not None and hist_expected > 0:
        diff_pct = ((total_trips - hist_expected) / hist_expected) * 100
        if diff_pct > 25:
            st.caption(f":red[{diff_pct:+.1f}%] vs historical baseline ({hist_expected:.0f} expected)")
        elif diff_pct < -25:
            st.caption(f":green[{diff_pct:+.1f}%] vs historical baseline ({hist_expected:.0f} expected)")
        else:
            st.caption(f":orange[{diff_pct:+.1f}%] vs historical baseline ({hist_expected:.0f} expected)")

# ---------------------------------------------------------------------------
# Timeline bar
# ---------------------------------------------------------------------------
with st.container(border=True):
    tl_slider, tl_controls = st.columns([8, 2])

    with tl_slider:
        if len(distinct_windows) > 1:
            def on_scrubber_change():
                val = st.session_state.get("replay_timeline_slider")
                if val in window_labels:
                    st.session_state["active_window_label"] = val
                    st.session_state["follow_stream"] = (val == latest_label)
                    st.session_state["replay_is_playing"] = False

            curr_val = active_label if active_label in window_labels else window_labels[-1]
            st.select_slider(
                "Replay timeline",
                options=window_labels,
                value=curr_val,
                key="replay_timeline_slider",
                on_change=on_scrubber_change,
                label_visibility="collapsed",
            )
        else:
            st.select_slider(
                "Replay timeline",
                options=window_labels if window_labels else ["2023-02-01 00:00 NY"],
                value=window_labels[0] if window_labels else "2023-02-01 00:00 NY",
                disabled=True,
                key="replay_timeline_slider_single",
                label_visibility="collapsed",
            )

    with tl_controls:
        with st.container(horizontal=True):
            if is_playing:
                st.button(
                    ":material/pause: Pause",
                    key="btn_pause",
                    on_click=nav_step_to,
                    args=(active_label, False, False),
                )
            else:
                target_start = window_labels[0] if active_idx >= len(window_labels) - 1 else active_label
                st.button(
                    ":material/play_arrow: Play",
                    key="btn_play",
                    disabled=len(distinct_windows) <= 1,
                    on_click=nav_step_to,
                    args=(target_start, False, True),
                )
            st.button(
                "Live",
                key="btn_live",
                type="primary" if not follow_stream else "secondary",
                disabled=follow_stream and not is_playing,
                on_click=nav_step_to,
                args=(latest_label, True, False),
            )

    # Status line
    if is_playing:
        mode_str = ":blue[:material/play_arrow: Playing]"
    elif follow_stream:
        mode_str = ":green[:material/fiber_manual_record: Live head]"
    else:
        mode_str = ":orange[:material/lock: Locked]"

    pos_str = f"({active_idx + 1}/{len(distinct_windows)})" if distinct_windows else ""
    st.caption(
        f"Window: **{active_label or '—'}** {pos_str} · "
        f"Mode: {mode_str} · "
        f":material/bolt: {speedup}× virtual time"
    )

# ---------------------------------------------------------------------------
# Expandable deep-dive drawers (lazy evaluation)
# ---------------------------------------------------------------------------

def _ratio_color() -> alt.Color:
    return alt.Color(
        "demand_ratio:Q",
        scale=alt.Scale(
            domain=[0, 1, HIGH_DEMAND_RATIO, 3],
            range=["#10b981", "#f59e0b", "#ef4444", "#991b1b"],
        ),
        legend=alt.Legend(title="Demand ratio"),
    )


exp_charts = st.expander(
    "Trip activity and historical comparison",
    expanded=False,
    icon=":material/bar_chart:",
    on_change="rerun",
    key="exp_charts",
)
if st.session_state.get("exp_charts", False):
    with exp_charts:
        tab_vol, tab_surge, tab_trend = st.tabs(["Volume", "Surge zones", "Time series"])

        with tab_vol:
            ws = window_slice
            if ws["zone_label"].nunique() > 15:
                top_labels = ws.sort_values("current_trip_count", ascending=False).head(15)["zone_label"]
                ws = ws[ws.zone_label.isin(top_labels)]

            if chart_mode_for(filtered) == "grouped" and "historical_mean_trip_count" in ws.columns:
                long = ws.melt(
                    id_vars=["zone_label", "window_label"],
                    value_vars=["current_trip_count", "historical_mean_trip_count"],
                    var_name="series", value_name="trips",
                )
                long["series"] = long["series"].map({
                    "current_trip_count": "Replay trips",
                    "historical_mean_trip_count": "Historical mean",
                })
                chart = (
                    alt.Chart(long).mark_bar()
                    .encode(
                        x=alt.X("zone_label:N", sort="-y", title="Zone"),
                        y=alt.Y("trips:Q", title="Trips"),
                        color=alt.Color("series:N", title=""),
                        xOffset="series:N",
                        tooltip=["zone_label", "series", "trips"],
                    )
                    .properties(height=260)
                )
            else:
                chart = (
                    alt.Chart(ws).mark_bar()
                    .encode(
                        x=alt.X("zone_label:N", sort="-y", title="Zone"),
                        y=alt.Y("current_trip_count:Q", title="Replay trips"),
                        color=_ratio_color(),
                        tooltip=["zone_label", "current_trip_count", "demand_ratio"],
                    )
                    .properties(height=260)
                )
            st.altair_chart(chart, width="stretch")

        with tab_surge:
            valid = window_slice[window_slice["demand_ratio"].notna() & (window_slice["current_trip_count"] > 0)]
            if not valid.empty:
                top = valid.sort_values("demand_ratio", ascending=False).head(15)
                surge_chart = (
                    alt.Chart(top).mark_bar()
                    .encode(
                        x=alt.X("demand_ratio:Q", title="Demand ratio"),
                        y=alt.Y("zone_label:N", sort="-x", title="Zone"),
                        color=_ratio_color(),
                        tooltip=["zone_label", "demand_ratio", "current_trip_count", "z_score"],
                    )
                    .properties(height=260)
                )
                st.altair_chart(surge_chart, width="stretch")
            else:
                st.info("No surge zones in this window.")

        with tab_trend:
            if len(active_zone_ids) > 5:
                agg = (
                    filtered.groupby("window_start_local", as_index=False)
                    .agg({"current_trip_count": "sum", "historical_mean_trip_count": "sum"})
                )
                agg["zone_label"] = "All selected zones"
                plot_df = agg
            else:
                plot_df = filtered
            if "zone_label" in plot_df.columns and not plot_df.empty:
                long = plot_df.melt(
                    id_vars=["window_start_local", "zone_label"],
                    value_vars=["current_trip_count", "historical_mean_trip_count"],
                    var_name="series", value_name="trips",
                )
                long["series"] = long["series"].map({
                    "current_trip_count": "Replay trips",
                    "historical_mean_trip_count": "Historical mean",
                })
                trend_chart = (
                    alt.Chart(long).mark_line(point=True)
                    .encode(
                        x=alt.X("window_start_local:T", title="Window start (NY)"),
                        y=alt.Y("trips:Q", title="Trips"),
                        color="series:N",
                        strokeDash="zone_label:N",
                        tooltip=["window_start_local", "zone_label", "series", "trips"],
                    )
                    .properties(height=260)
                )
                st.altair_chart(trend_chart, width="stretch")
            else:
                st.info("Not enough data for trend chart.")

# Zone explorer
exp_zone = st.expander(
    "Zone explorer",
    expanded=False,
    icon=":material/search:",
    on_change="rerun",
    key="exp_zone",
)
if st.session_state.get("exp_zone", False):
    with exp_zone:
        pool = filtered if not filtered.empty else view
        if not pool.empty:
            labels = sorted(pool["zone_label"].dropna().unique().tolist())
            sel_label = st.selectbox("Select zone", options=labels, key="explorer_zone_select")
            zone_rows = pool[pool.zone_label == sel_label]
            if zone_rows.empty:
                zone_rows = view[view.zone_label == sel_label]

            if zone_rows.empty:
                st.info("No activity for this zone.")
            else:
                latest_row = zone_rows.sort_values("window_start_local").iloc[-1]
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Replay trips", f"{int(latest_row.current_trip_count):,}")
                c2.metric(
                    "Historical baseline",
                    f"{latest_row.historical_mean_trip_count:.1f}" if pd.notna(latest_row.historical_mean_trip_count) else "—",
                )
                c3.metric(
                    "Demand ratio",
                    f"{latest_row.demand_ratio:.2f}×" if pd.notna(latest_row.demand_ratio) else "—",
                )
                c4.metric("Z-score", f"{latest_row.z_score:.2f}" if pd.notna(latest_row.z_score) else "—")
        else:
            st.info("No zone data available.")

# Raw data table
exp_data = st.expander(
    "Raw data table",
    expanded=False,
    icon=":material/table:",
    on_change="rerun",
    key="exp_data",
)
if st.session_state.get("exp_data", False):
    with exp_data:
        pretty_cols = [c for c in TABLE_COLS if c in window_slice.columns]
        pretty = window_slice.loc[:, pretty_cols].rename(columns={
            "pickup_zone_id": "Zone ID",
            "zone_name": "Zone",
            "borough": "Borough",
            "window_label": "Window (NY)",
            "current_trip_count": "Replay trips",
            "historical_mean_trip_count": "Historical mean",
            "demand_ratio": "Demand ratio",
            "z_score": "Z-score",
            "is_high_demand": "Surge",
        })
        count = len(pretty)
        st.caption(f"{count:,} rows")
        st.dataframe(pretty.head(500) if count > 500 else pretty, hide_index=True)
