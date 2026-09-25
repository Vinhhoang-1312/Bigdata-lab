"""Glass Cockpit HUD & Stream Status components for Streamlit.

Production observability control plane answering three questions at a glance:
1. Are events arriving? (Input rate, Kafka source backlog, batch ID)
2. Is Spark keeping up? (Processing rate, batch duration, trigger load)
3. Is data trustworthy? (Watermark advancing, streaming Gold freshness, invalid events)
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from analytics_app.data_access import gold_freshness_age_seconds
from observability.governance import build_data_governance_report
from observability.health_state import classify_health
from observability.kafka_backlog import build_kafka_backlog_report
from observability.telemetry_reader import read_telemetry, telemetry_age_seconds
from observability.watermark import build_watermark_status


def status_badge(state: str) -> str:
    badges = {
        "HEALTHY": ":material/check_circle: :green[**HEALTHY**]",
        "DEGRADED": ":material/warning: :orange[**DEGRADED**]",
        "STALE": ":material/history: :orange[**STALE**]",
        "UNAVAILABLE": ":material/error: :red[**UNAVAILABLE**]",
    }
    return badges.get(state, f":material/help: **{state}**")


@st.cache_data(ttl=600)
def get_cached_governance_report(invalid_events_total: int = 0) -> dict:
    """Cached for 10 minutes to avoid S3 rejection scanning on fast ticks."""
    return build_data_governance_report({"invalid_events_total": invalid_events_total})


def get_pipeline_telemetry(live_df: pd.DataFrame | None = None) -> dict:
    """Read telemetry and compute pipeline health classification."""
    now = datetime.now(timezone.utc)
    telemetry = read_telemetry()

    live_age: int | None = None
    freshness_basis = "unavailable"
    if live_df is not None:
        live_age, freshness_basis = gold_freshness_age_seconds(live_df, pd.Timestamp(now))

    prev_wm = st.session_state.get("prev_watermark") if hasattr(st, "session_state") else None
    wm_report = build_watermark_status(telemetry, prev_watermark=prev_wm)
    if hasattr(st, "session_state") and telemetry and telemetry.get("watermark"):
        st.session_state["prev_watermark"] = telemetry["watermark"]

    health = classify_health(telemetry, live_age, now)
    kafka_report = build_kafka_backlog_report(telemetry)
    tel_age = telemetry_age_seconds(telemetry, now)

    return {
        "now": now,
        "telemetry": telemetry,
        "live_age": live_age,
        "freshness_basis": freshness_basis,
        "tel_age": tel_age,
        "health": health,
        "kafka_report": kafka_report,
        "gov_report": None,
        "wm_report": wm_report,
    }


def render_stream_status_panel(pipe_data: dict) -> None:
    """Render the compact Stream Status card for the right-hand console panel."""
    health = pipe_data["health"]
    telemetry = pipe_data["telemetry"]
    kafka_report = pipe_data["kafka_report"]
    wm_report = pipe_data["wm_report"]
    tel_age = pipe_data["tel_age"]

    state = health["state"]
    reason = health["reason"]

    with st.container(border=True):
        st.markdown(
            f"<div style='display: flex; justify-content: space-between; align-items: center; margin-bottom: 4px;'>"
            f"<span style='font-size: 13px; font-weight: 700; letter-spacing: 0.05em; color: #94a3b8; text-transform: uppercase;'>STREAM STATUS</span>"
            f"<span style='font-size: 13px;'>{status_badge(state)}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )
        st.caption(f":material/info: {reason}")

        # Metrics grid
        c1, c2 = st.columns(2)
        with c1:
            in_rate = telemetry.get("input_rows_per_second") if telemetry else None
            in_rate_str = f"{in_rate:.1f} r/s" if in_rate is not None else "—"
            st.metric("Kafka Ingest", in_rate_str, help="Kafka event arrival rate into Spark Structured Streaming")

            proc_rate = telemetry.get("processed_rows_per_second") if telemetry else None
            proc_rate_str = f"{proc_rate:.1f} r/s" if proc_rate is not None else "—"
            st.metric("Spark Process", proc_rate_str, help="Spark batch processing throughput")

        with c2:
            load = health.get("trigger_load")
            if load is not None:
                load_str = f"{load * 100:.1f}%"
                load_color = ":red" if load > 0.8 else ":green"
            else:
                load_str = "—"
                load_color = ""
            st.metric("Trigger Load", load_str if not load_color else f"{load_color}[{load_str}]", help="Trigger execution duration / 10s trigger interval (healthy < 80%)")

            batch_id = telemetry.get("batch_id") if telemetry else None
            batch_str = f"#{batch_id}" if batch_id is not None and batch_id >= 0 else "—"
            dur_ms = telemetry.get("batch_duration_ms") if telemetry else None
            dur_help = f"Batch duration: {dur_ms:,} ms" if dur_ms is not None else "No batch running"
            st.metric("Batch ID", batch_str, help=dur_help)

        # Health indicators row
        backlog = kafka_report.get("total_backlog_rows")
        backlog_str = f"{backlog:,} rows" if backlog is not None else "0 rows"
        wm_adv = wm_report.get("advancing")
        wm_icon = "🟢" if wm_adv else ("🔴" if wm_adv is False else "⚪")

        st.markdown(
            f"<div style='font-size: 11px; color: #cbd5e1; background: #0f172a; padding: 6px 10px; border-radius: 4px; border: 1px solid #1e293b; margin-top: 6px;'>"
            f"<div><b>Kafka Backlog:</b> {backlog_str} · <b>Topic:</b> urbanflow.trip-events</div>"
            f"<div><b>Watermark:</b> {wm_icon} {'Advancing' if wm_adv else 'Pending'} · <b>Lag:</b> 10m tumbling</div>"
            f"<div><b>Telemetry Heartbeat:</b> {tel_age}s ago</div>"
            f"</div>",
            unsafe_allow_html=True,
        )


def render_observability_drawer(pipe_data: dict) -> None:
    """Render the deep inspection expander for Spark execution topology and governance."""
    kafka_report = pipe_data["kafka_report"]
    telemetry = pipe_data.get("telemetry") or {}
    inv_total = telemetry.get("invalid_events_total", 0)
    gov_report = pipe_data.get("gov_report") or get_cached_governance_report(inv_total)

    with st.expander("🔬 Spark Execution Topology & Observability Inspection", expanded=False):
        st.info(
            "**Spark Execution Topology**: The streaming aggregation job executes in "
            "**`local[2]`** mode inside the driver container. Standalone Master (:8080) and Workers (:8081/:8082) "
            "serve as background standalone infrastructure daemons for batch scaling; the active streaming query, "
            "stages, and tasks are directly observed on the **Spark Driver UI (:4040)**."
        )

        diag_col1, diag_col2 = st.columns(2)

        with diag_col1:
            st.markdown("##### Kafka partition backlog")
            per_part = kafka_report.get("per_partition")
            if per_part:
                rows = []
                for p, info in sorted(per_part.items()):
                    rows.append({
                        "Partition": p,
                        "Latest offset": info.get("latest_offset"),
                        "Spark end offset": info.get("spark_end_offset"),
                        "Backlog rows": info.get("backlog"),
                    })
                st.dataframe(pd.DataFrame(rows), hide_index=True)
            else:
                st.caption("No partition-level backlog recorded yet.")
            st.caption(kafka_report.get("semantics_note", ""))

        with diag_col2:
            st.markdown("##### Data governance & quarantine")
            st.caption(gov_report.get("note", ""))
            st.markdown(
                f"**Streaming invalid events:** {gov_report.get('streaming_invalid_events_total', 0)}"
            )
            st.markdown(
                f"**Batch quarantine total rejected:** {gov_report.get('batch_quarantine_total_rejected', 0):,}"
            )
            reasons = gov_report.get("batch_rejection_reasons", {})
            if reasons:
                st.markdown("**Batch rejection reasons:**")
                r_df = pd.DataFrame(
                    [{"Reason": k, "Count": v} for k, v in reasons.items()]
                )
                st.dataframe(r_df, hide_index=True)
                st.caption(
                    "*Note: Rejection reason counts are non-mutually-exclusive; "
                    "a single rejected record may contribute to multiple validation reason counters.*"
                )


def render_glass_cockpit(live_df: pd.DataFrame) -> None:
    """Legacy backward-compatible HUD renderer."""
    pipe_data = get_pipeline_telemetry(live_df)
    render_stream_status_panel(pipe_data)
    render_observability_drawer(pipe_data)
