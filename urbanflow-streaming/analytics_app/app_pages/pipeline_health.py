"""Pipeline Health — Spark, Kafka, and governance observability."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from analytics_app.hud import (
    get_cached_governance_report,
    get_pipeline_telemetry,
    status_badge,
)

# ---------------------------------------------------------------------------
# Auto-refreshing health overview (10-second fragment)
# ---------------------------------------------------------------------------
auto_refresh = st.session_state.get("control_auto_refresh", True)


@st.fragment(run_every=10 if auto_refresh else None)
def render_health_overview():
    pipe = get_pipeline_telemetry(None)
    health = pipe["health"]
    tel = pipe.get("telemetry") or {}
    kafka = pipe["kafka_report"]
    wm = pipe["wm_report"]
    tel_age = pipe.get("tel_age")

    state = health["state"]
    reason = health["reason"]

    # Status header
    st.markdown(f"### {status_badge(state)}")
    st.caption(f":material/info: {reason}")
    if tel_age is not None:
        st.caption(f"Telemetry heartbeat: updated {tel_age}s ago")

    st.space("small")

    # Metrics grid
    with st.container(border=True):
        c1, c2, c3, c4 = st.columns(4)

        in_rate = tel.get("input_rows_per_second")
        c1.metric(
            "Kafka ingest rate",
            f"{in_rate:.1f} rows/s" if in_rate is not None else "—",
            help="Kafka event arrival rate into Spark Structured Streaming",
        )

        proc_rate = tel.get("processed_rows_per_second")
        c2.metric(
            "Spark throughput",
            f"{proc_rate:.1f} rows/s" if proc_rate is not None else "—",
            help="Spark batch processing throughput",
        )

        load = health.get("trigger_load")
        if load is not None:
            load_str = f"{load * 100:.1f}%"
            load_display = f":red[{load_str}]" if load > 0.8 else f":green[{load_str}]"
        else:
            load_display = "—"
        c3.metric(
            "Trigger load",
            load_display,
            help="Execution duration / 10s trigger interval (healthy < 80%)",
        )

        batch_id = tel.get("batch_id")
        dur_ms = tel.get("batch_duration_ms")
        c4.metric(
            "Batch ID",
            f"#{batch_id}" if batch_id is not None and batch_id >= 0 else "—",
            help=f"Duration: {dur_ms:,} ms" if dur_ms is not None else "No batch running",
        )

    st.space("small")

    # Watermark + Kafka backlog
    wm_c1, wm_c2 = st.columns(2)
    with wm_c1:
        st.markdown("##### Watermark")
        wm_adv = wm.get("advancing")
        wm_val = wm.get("current_watermark") or "—"
        if wm_val != "—":
            wm_status = "Advancing" if wm_adv else "Steady"
            wm_color = "green" if wm_adv else "blue"
            icon = ":material/check_circle:" if wm_adv else ":material/schedule:"
            st.markdown(f"{icon} :{wm_color}[**{wm_status}**] · `{wm_val}`")
        else:
            st.markdown(":material/pending: :orange[**Pending**] · `—`")

        inv = tel.get("invalid_events_total", 0)
        st.metric("Invalid stream events", f"{inv:,}", help="Total malformed events rejected by Spark")

    with wm_c2:
        st.markdown("##### Kafka backlog")
        backlog = kafka.get("total_backlog_rows")
        st.metric("Backlog", f"{backlog:,} rows" if backlog is not None else "Unavailable")
        st.caption("Topic: `urbanflow.trip-events` · 4 partitions")

        per_part = kafka.get("per_partition")
        if per_part:
            rows = []
            for p, info in sorted(per_part.items()):
                rows.append({
                    "Partition": p,
                    "Latest offset": info.get("latest_offset"),
                    "Spark end offset": info.get("spark_end_offset"),
                    "Backlog": info.get("backlog"),
                })
            st.dataframe(pd.DataFrame(rows), hide_index=True)

    st.caption(kafka.get("semantics_note", ""))


render_health_overview()

# ---------------------------------------------------------------------------
# Governance (lazy, cached 10 minutes)
# ---------------------------------------------------------------------------
exp_gov = st.expander(
    "Data governance and quarantine",
    expanded=False,
    icon=":material/shield:",
    on_change="rerun",
    key="exp_governance",
)
if st.session_state.get("exp_governance", False):
    with exp_gov:
        tel = get_pipeline_telemetry(None).get("telemetry") or {}
        inv = tel.get("invalid_events_total", 0)
        gov = get_cached_governance_report(inv)

        st.caption(gov.get("note", ""))
        st.metric("Streaming invalid events", gov.get("streaming_invalid_events_total", 0))
        st.metric("Batch quarantine rejected", f"{gov.get('batch_quarantine_total_rejected', 0):,}")

        reasons = gov.get("batch_rejection_reasons", {})
        if reasons:
            st.markdown("**Batch rejection reasons:**")
            st.dataframe(
                pd.DataFrame([{"Reason": k, "Count": v} for k, v in reasons.items()]),
                hide_index=True,
            )
            st.caption(
                "Rejection reason counts are non-mutually-exclusive; "
                "a single rejected record may contribute to multiple counters."
            )

# ---------------------------------------------------------------------------
# Spark topology (lazy)
# ---------------------------------------------------------------------------
exp_spark = st.expander(
    "Spark execution topology",
    expanded=False,
    icon=":material/memory:",
    on_change="rerun",
    key="exp_spark_topo",
)
if st.session_state.get("exp_spark_topo", False):
    with exp_spark:
        st.info(
            "The streaming aggregation job executes in **`local[2]`** mode inside the driver container. "
            "Standalone Master (:8080) and Workers (:8081/:8082) serve as background daemons. "
            "The active streaming query is observed on **Spark Driver UI (:4040)**."
        )

# ---------------------------------------------------------------------------
# Infrastructure links (lazy)
# ---------------------------------------------------------------------------
exp_links = st.expander(
    "Infrastructure links",
    expanded=False,
    icon=":material/link:",
    on_change="rerun",
    key="exp_infra_links",
)
if st.session_state.get("exp_infra_links", False):
    with exp_links:
        st.markdown(
            """
**Primary endpoints:**
- [Streamlit Dashboard :8501](http://localhost:8501)
- [Spark Driver UI :4040](http://localhost:4040) *(active streaming)*
- [FastAPI Health API :8000](http://localhost:8000/v1/streaming/health)
- [FastAPI OpenAPI docs :8000](http://localhost:8000/docs)
- [SeaweedFS Filer :8888](http://localhost:8888)

**Background daemons:**
- [SeaweedFS S3 API :8333](http://localhost:8333) *(S3 API, not console)*
- [Spark Master :8080](http://localhost:8080)
- [Spark Worker 1 :8081](http://localhost:8081)
- [Spark Worker 2 :8082](http://localhost:8082)
- [SeaweedFS Master :9333](http://localhost:9333)
            """
        )
