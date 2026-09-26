# Archived Streamlit UI

**Status:** archived on 2026-09-26.

The Streamlit interface on port `8501` was the first analytics prototype. It
proved that live Gold and historical Gold could be read and compared, but it is
not the product interface. React now owns the Streaming and ML user experience:

- Streaming: `http://127.0.0.1:5173/streaming`
- ML: `http://127.0.0.1:5173/ml`

Run both workspaces with `start-urbanflow.bat` at the repository root.

The old entry point remains at `analytics_app/app.py` for source history only.
Do not start it in normal development or demonstrations. Other files under
`analytics_app/`, especially data-access code, are still used by the read-only
FastAPI service and therefore are not legacy as a whole.

The local Docker container is renamed to `urbanflow-streamlit-archived` when it
is present. This preserves its local history without exposing port `8501` as an
active UrbanFlow interface.
