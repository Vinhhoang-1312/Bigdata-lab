# Streaming dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the Streamlit Gold viewer so fixture and live Gold look correct in New York time, with named zones, a NYC map, and charts that work for a single window.

**Architecture:** Keep Gold I/O in `GoldReader`. Add pure view helpers (timezone, labels, high-demand flag, chart-ready frames) tested with pytest. `app.py` only renders those frames.

**Tech Stack:** Streamlit, pandas, Altair, pydeck, pytest.

## Global Constraints

- Foundation timezone America/New_York
- App consumes Gold only
- demand_ratio = current / historical mean; highlight > 1.5
- No credentials in source

## Tasks

- [ ] Failing tests for NY display, zone labels, high-demand, single-window chart frame
- [ ] Implement helpers in data_access.py / zones.py
- [ ] Rebuild app.py tabs + map + refresh
- [ ] pytest + Playwright MCP screenshots of all tabs
- [ ] Commit and push branch urbanflow-streaming
