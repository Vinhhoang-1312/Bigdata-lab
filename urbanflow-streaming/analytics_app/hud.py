"""Glass Cockpit HUD component for Streamlit.

Production observability control plane answering three questions at a glance:
1. Are events arriving? (Input rate, Kafka source backlog, batch ID)
2. Is Spark keeping up? (Processing rate, batch duration, trigger load)
3. Is the data trustworthy? (Watermark advancing, live gold age, invalid events)
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from observability.governance import build_data_governance_report
from observability.health_state import classify_health
from observability.kafka_backlog import build_kafka_backlog_report
from observability.telemetry_reader import read_telemetry, telemetry_age_seconds
from observability.watermark import build_watermark_status


def _status_badge(state: str) -> str:
    badges = {
        "HEALTHY": "🟢 :green[**HEALTHY**]",
        "DEGRADED": "🟡 :orange[**DEGRADED**]",
        "STALE": "🟠 :orange[**STALE**]",
        "UNAVAILABLE": "🔴 :red[**UNAVAILABLE**]",
    }
    return badges.get(state, f"⚪ **{state}**")


def render_glass_cockpit(live_df: pd.DataFrame) -> None:
    """Render the compact Glass Cockpit operational control plane."""
    now = datetime.now(timezone.utc)
    telemetry = read_telemetry()

    # Calculate live Gold age
    live_age: int | None = None
    if not live_df.empty and "window_end" in live_df.columns:
        latest_end = live_df["window_end"].max()
        live_age = max(0, int((pd.Timestamp(now).tz_convert("UTC") - latest_end).total_seconds()))

    health = classify_health(telemetry, live_age, now)
    state = health["state"]
    reason = health["reason"]

    kafka_report = build_kafka_backlog_report(telemetry)
    gov_report = build_data_governance_report(telemetry)
    wm_report = build_watermark_status(telemetry)

    # 1. Main Cockpit HUD Container
    with st.container(border=True):
        hdr_col1, hdr_col2 = st.columns([3, 1])
        with hdr_col1:
            st.markdown(
                f"### Operational control plane · {_status_badge(state)}"
            )
            st.caption(f"Status rationale: {reason}")
        with hdr_col2:
            tel_age = telemetry_age_seconds(telemetry, now)
            if tel_age is not None:
                st.caption(f":material/schedule: Heartbeat {tel_age}s ago")
            else:
                st.caption(":material/schedule: Heartbeat unavailable")

        # 2. The Three Questions Scorecard
        col_arriving, col_keeping_up, col_trustworthy = st.columns(3)

        # --- Q1: Are events arriving? ---
        with col_arriving:
            st.markdown("**:material/input: Are events arriving?**")
            in_rate = telemetry.get("input_rows_per_second") if telemetry else None
            in_rate_str = f"{in_rate:.1f} rows/s" if in_rate is not None else "—"
            batch_id = telemetry.get("batch_id") if telemetry else None
            batch_str = f"Batch #{batch_id}" if batch_id is not None and batch_id >= 0 else "—"
            st.metric("Kafka input rate", in_rate_str, help=batch_str)

            backlog = kafka_report.get("total_backlog_rows")
            trend = kafka_report.get("trend")
            trend_str = f" ({trend})" if trend else ""
            backlog_str = f"{backlog:,} rows{trend_str}" if backlog is not None else "—"
            st.caption(f"Kafka source backlog: **{backlog_str}**")

        # --- Q2: Is Spark keeping up? ---
        with col_keeping_up:
            st.markdown("**:material/speed: Is Spark keeping up?**")
            proc_rate = telemetry.get("processed_rows_per_second") if telemetry else None
            proc_rate_str = f"{proc_rate:.1f} rows/s" if proc_rate is not None else "—"
            duration_ms = telemetry.get("batch_duration_ms") if telemetry else None
            dur_str = f"{duration_ms} ms" if duration_ms is not None else "—"
            st.metric("Processing rate", proc_rate_str, help=f"Batch duration: {dur_str}")

            load = health.get("trigger_load")
            load_str = f"{load:.3f}" if load is not None else "—"
            load_label = (
                f":red[{load_str}] (>0.80)"
                if load and load > 0.8
                else f"**{load_str}**"
                if load is not None
                else "—"
            )
            st.caption(f"Trigger load (exec/trigger): {load_label}")
            tp_ratio = health.get("throughput_ratio")
            if tp_ratio is not None:
                st.caption(f"Throughput ratio (in/proc): **{tp_ratio:.2f}**")

        # --- Q3: Is the data trustworthy? ---
        with col_trustworthy:
            st.markdown("**:material/verified: Is data trustworthy?**")
            wm_adv = wm_report.get("advancing")
            wm_adv_str = "Yes" if wm_adv else ("No" if wm_adv is False else "—")
            wm_val = wm_report.get("current_watermark") or "—"
            st.metric("Watermark advancing", wm_adv_str, help=f"Watermark: {wm_val}")

            invalid_total = gov_report.get("streaming_invalid_events_total", 0)
            st.caption(
                f"Invalid stream events: **{invalid_total}** · Live gold age: **{live_age}s**"
                if live_age is not None
                else f"Invalid stream events: **{invalid_total}**"
            )

        # 3. Observability details expander
        with st.expander("Spark Execution Topology & Observability Inspection", expanded=False):
            st.info(
                "**Spark Execution Topology**: The live Structured Streaming application executes in "
                "**`local[2]`** mode inside the driver container. Standalone Master (:8080) and Workers (:8081/:8082) "
                "serve as standalone infrastructure cluster daemons for batch scaling; the active streaming query, "
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
