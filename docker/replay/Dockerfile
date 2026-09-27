FROM python:3.13.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /workspace

COPY urbanflow-replay/pyproject.toml /tmp/replay/pyproject.toml
COPY urbanflow-replay/producer /tmp/replay/producer
RUN python -m pip install --no-cache-dir /tmp/replay && rm -rf /tmp/replay
