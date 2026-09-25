"""UrbanFlow — NYC Taxi Demand Analytics (Historical Replay Console).

Multipage Streamlit entry point: Mobility Analytics (default) and Pipeline Health.
"""
from __future__ import annotations

import os
from pathlib import Path

import streamlit as st
from pydeck.bindings import json_tools as _pdk_json_tools

# ---------------------------------------------------------------------------
# Compact Deck serialization (eliminate default indent=2 bloat)
# ---------------------------------------------------------------------------
def _compact_deck_serialize(serializable: object) -> str:
    import json
    return json.dumps(
        serializable,
        sort_keys=True,
        default=_pdk_json_tools.default_serialize,
        separators=(",", ":"),
    )


_pdk_json_tools.serialize = _compact_deck_serialize

# ---------------------------------------------------------------------------
# Data source path resolution (shared across pages via session state)
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HISTORICAL = ROOT / "urbanflow-foundation/fixtures/gold/historical_zone_baseline.jsonl"
DEFAULT_LIVE = ROOT / "urbanflow-foundation/fixtures/gold/expected_live_zone_metrics.jsonl"
DEFAULT_LOOKUP = ROOT / "urbanflow-foundation/fixtures/bronze/zone_lookup.csv"
INTEGRATED_LOOKUP = "s3a://urbanflow/bronze/zone_lookup/taxi_zone_lookup.csv"

historical_default = os.getenv("URBANFLOW_HISTORICAL_PATH", str(DEFAULT_HISTORICAL))
live_default = os.getenv("URBANFLOW_LIVE_PATH", str(DEFAULT_LIVE))
lookup_default = INTEGRATED_LOOKUP if live_default.startswith(("s3://", "s3a://")) else str(DEFAULT_LOOKUP)
lookup_path = os.getenv("URBANFLOW_ZONE_LOOKUP_PATH", lookup_default)

# Store paths in session state for pages to access
st.session_state.setdefault("historical_path", historical_default)
st.session_state.setdefault("live_path", live_default)
st.session_state.setdefault("lookup_path", lookup_path)

# ---------------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="UrbanFlow — Historical Replay Console",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Minimal layout adjustment: comfortable padding
st.html(
    """
    <style>
    .block-container {
        padding-top: 1.5rem !important;
        padding-bottom: 1rem !important;
        max-width: 100% !important;
    }
    </style>
    """
)

# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------
page = st.navigation(
    [
        st.Page(
            "app_pages/mobility.py",
            title="Mobility analytics",
            icon=":material/map:",
            default=True,
        ),
        st.Page(
            "app_pages/pipeline_health.py",
            title="Pipeline health",
            icon=":material/monitor_heart:",
        ),
    ],
    position="top",
)

page.run()
