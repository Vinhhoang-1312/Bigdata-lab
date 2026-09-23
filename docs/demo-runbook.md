# UrbanFlow One-Command Demo & Glass Cockpit Runbook

This runbook documents the automated **One-Command Demo** lifecycle (`make demo`), the hybrid architecture, system navigation, runtime configuration, and operational troubleshooting for UrbanFlow.

---

## 1. Overview & Architecture

The One-Command Demo provides a completely automated, reproducible environment to demonstrate the end-to-end continuous ingestion, streaming aggregations, virtual time replay, and the Glass Cockpit observability control plane.

```
                              ┌──────────────────────────────────────────────┐
                              │            SeaweedFS Object Storage          │
                              │  S3 API: :8333 | Filer: :8888 | Master: :9333 │
                              │      s3a://urbanflow/gold/live_zone_metrics/ │
                              └──────────────▲────────────────▲──────────────┘
                                             │                │
                                       (writes Parquet) (reads Parquet)
                                             │                │
┌───────────────────────────┐         ┌──────┴─────────┐      │     ┌────────────────────────────┐
│   Virtual Time Replay     │         │ Spark Streaming│      │     │    Streamlit Dashboard     │
│   (urbanflow-demo-replay) │         │ (local[2],:4040│      │     │       (Host Port 8501)     │
│   Rate-controlled 60x     │         │ 5-min sliding  │      │     │  Interactive maps, metrics,│
│   looping February 2023   │         │ 1-min slide)   │      │     │  HUD Glass Cockpit panel   │
└─────────────┬─────────────┘         └──────▲─────────┘      │     └─────────────▲──────────────┘
              │                              │                │                   │
         (publishes)                    (consumes)            │                   │
              │                              │                │                   │
              ▼                              │                │                   │
┌───────────────────────────┐                │                │                   │
│    Apache Kafka Broker    │────────────────┘                │                   │
│    Port 9092 (Docker)     │                                 │                   │
│    Port 9094 (Host)       │                                 │                   │
│ Topic:                    │                                 │                   │
│  urbanflow.trip-events    │                                 │                   │
└───────────────────────────┘                                 │                   │
              ▲                                               │                   │
              │ (reads offsets for backlog)                   │                   │
              │                                               │                   │
┌─────────────┴─────────────┐                                 │     ┌─────────────┴──────────────┐
│  Spark Telemetry Heartbeat│─────────────────────────────────┼────▶│  FastAPI Health Service    │
│  .telemetry/              │  (atomic JSON writes / reads)   │     │       (Host Port 8000)     │
│  streaming_health.json    │                                 │     │  /v1/streaming/health      │
└───────────────────────────┘                                 │     │  /docs (OpenAPI specs)     │
                                                              └─────┴────────────────────────────┘
```

### Hybrid Execution Model

1. **Containerized Engine & Infrastructure (Docker Compose)**:
   - **SeaweedFS S3**: Multi-protocol storage engine (`s3a://urbanflow`).
   - **Apache Kafka**: 4-partition event streaming broker.
   - **Apache Spark Master & Worker**: Standalone cluster infrastructure (`spark://spark-master:7077`).
   - **Spark Streaming Driver (`urbanflow-demo-streaming`)**: Runs PySpark Structured Streaming with `--master 'local[2]'` and Web UI on port `4040`.
   - **Virtual Time Replay Producer (`urbanflow-demo-replay`)**: Reads Silver Parquet directly via Arrow batches and streams rate-controlled events with monotonic virtual clock rebasing.

2. **Detached Host Control & Presentation Plane**:
   - **FastAPI Observability Service**: Runs via `./urbanflow-streaming/.venv` on port `8000`. Exposes `/v1/streaming/health` and OpenAPI documentation at `/docs`.
   - **Streamlit Analytics Dashboard**: Runs via `./urbanflow-streaming/.venv` on port `8501`. Features live zone choropleth maps, historical demand comparisons, and the operational Glass Cockpit HUD.

3. **Telemetry & Communication Plane**:
   - The Spark driver thread periodically dumps query progress snapshots to `.telemetry/streaming_health.json`.
   - FastAPI and Streamlit poll this atomic JSON heartbeat to evaluate duty cycle, throughput, watermark progression, and Kafka consumer backlog with zero extra infrastructure (no external Prometheus or Datadog required).

---

## 2. System Navigation & Ports

The table below lists all network endpoints with accurate port descriptors and role distinctions:

| Service / Interface | URL | Environment / Network | Description & Role |
| :--- | :--- | :--- | :--- |
| **Streamlit Dashboard** | `http://localhost:8501` | Host Process | User-facing analytics dashboard with live maps and Glass Cockpit HUD |
| **FastAPI Health API** | `http://localhost:8000/v1/streaming/health` | Host Process | Production telemetry contract returning JSON health classification |
| **FastAPI OpenAPI Docs** | `http://localhost:8000/docs` | Host Process | Interactive Swagger UI for the observability and streaming service API |
| **Spark Driver Web UI** | `http://localhost:4040` | Container (`urbanflow-demo-streaming`) | **Active streaming execution UI** showing running micro-batches and stages |
| **Spark Master Web UI** | `http://localhost:8080` | Container (`urbanflow-spark-master-1`) | Standalone Spark Master UI (idle background infrastructure) |
| **Spark Worker 1 Web UI**| `http://localhost:8081` | Container (`urbanflow-spark-worker-1-1`)| Standalone Spark Worker 1 UI (idle background infrastructure) |
| **Spark Worker 2 Web UI**| `http://localhost:8082` | Container (`urbanflow-spark-worker-2-1`)| Standalone Spark Worker 2 UI (optional worker daemon) |
| **SeaweedFS S3 API** | `http://localhost:8333` | Container (`urbanflow-seaweedfs-1`) | **S3 API endpoint** (not a web console; returns XML root for AWS SDK/s3a) |
| **SeaweedFS Filer** | `http://localhost:8888` | Container (`urbanflow-seaweedfs-1`) | SeaweedFS filer web browsing interface |
| **SeaweedFS Master UI**| `http://localhost:9333` | Container (`urbanflow-seaweedfs-1`) | SeaweedFS storage cluster status and volume management console |
| **Kafka External Broker**| `localhost:9094` | Container (`urbanflow-kafka-1`) | External PLAINTEXT listener for host clients and diagnostic inspection |

> [!NOTE]
> `http://localhost:8333` is an **S3 API endpoint** designed for S3A / AWS CLI requests, not an interactive web dashboard.
> The active Spark streaming query executes locally on the driver container (`urbanflow-demo-streaming`) and exposes its live DAG and micro-batch metrics on port `4040`. The standalone cluster daemons on ports `8080`–`8082` provide batch infrastructure.

---

## 3. Quick Start Lifecycle

### Prerequisites

1. Ensure Docker and Docker Compose v2 are installed and running.
2. Initialize project credentials:
   ```bash
   cp .env.example .env
   ```
3. Initialize the streaming virtual environment:
   ```bash
   cd urbanflow-streaming
   uv venv
   uv pip install -e '.[analytics,serving,offline]'
   cd ..
   ```

### Launching the Demo

To launch all infrastructure, pipelines, replay, and dashboards with a single command:

```bash
make demo
```

This single command will:
1. Validate prerequisites (`.env`, Docker Compose, and virtual environment).
2. Cleanly stop any existing demo processes or port listeners.
3. Bring up SeaweedFS, Kafka, and Spark cluster containers.
4. Run `bucket-init` to create the `urbanflow` S3 bucket.
5. Run `kafka-topic-init` to create `urbanflow.trip-events` (4 partitions).
6. Launch Spark Structured Streaming (`urbanflow-demo-streaming`) with sliding-window aggregations.
7. Launch the Virtual Time Replay Producer (`urbanflow-demo-replay`) at 60x acceleration with looping enabled.
8. Start the FastAPI Observability Service on port `8000` as a detached daemon.
9. Start the Streamlit Analytics Dashboard on port `8501` as a detached daemon.

### Checking Component Status

Inspect all running containers, FastAPI health response, Streamlit dashboard status, Spark telemetry heartbeat, Kafka partition offsets, and Replay clock timestamps:

```bash
make demo-status
```

### Stopping the Demo

Cleanly stop all demo containers and kill host background daemons:

```bash
make demo-stop
```

This removes containers `urbanflow-demo-replay` and `urbanflow-demo-streaming`, reclaims host ports `8000` and `8501` via `fuser`, and cleans up `.telemetry/streaming_health.json`.

---

## 4. Configuration & Parameter Tuning

All major runtime parameters can be passed directly as command-line variables to `make`:

```bash
make demo [VARIABLE=VALUE ...]
```

| Variable | Default | Description | Example Usage |
| :--- | :--- | :--- | :--- |
| `SPEEDUP` | `60` | Virtual time acceleration factor (1 second wall clock = N seconds virtual event time). | `make demo SPEEDUP=300` |
| `MAX_RATE` | _(computed)_ | Burst limit in events/sec. Defaults in Python to `max(500.0, 4.0 * SPEEDUP)`. | `make demo MAX_RATE=1200` |
| `DEMO_MODE` | `integrated` | Data source mode: `integrated` (full February 2023 dataset) or `fixture`. | `make demo DEMO_MODE=fixture` |
| `STREAMLIT_PORT`| `8501` | Host port for the Streamlit Analytics Dashboard. | `make demo STREAMLIT_PORT=8502` |
| `FASTAPI_PORT` | `8000` | Host port for the FastAPI Observability Service. | `make demo FASTAPI_PORT=8001` |

### Fast-Forward Demo (300x Speedup)

To demonstrate hours of taxi pickup data in a few seconds:

```bash
make demo SPEEDUP=300
```

---

## 5. Observability & Glass Cockpit Semantics

The Glass Cockpit control plane evaluates streaming health across four discrete states:

- **`HEALTHY`**: Heartbeat is fresh (< 60s), micro-batches are actively advancing, trigger load duty cycle is < 0.80, and backlog is stable.
- **`DEGRADED`**: Spark is experiencing backpressure or execution duty cycle exceeds safe capacity (`trigger_load > 0.80` or `throughput_ratio > 1.0`).
- **`STALE`**: Query is reporting activity but batch progress or watermark has ceased advancing for >= 3 consecutive intervals.
- **`UNAVAILABLE`**: Heartbeat file is missing, engine is uninitialized (`batch_id = -1`), or heartbeat age exceeds 60 seconds.

### Key Metric Definitions

1. **Trigger Load (Execution Duty Cycle)**:
   $$\text{trigger\_load} = \frac{\text{trigger\_execution\_ms}}{\text{trigger\_interval\_ms}}$$
   Measures what proportion of the micro-batch trigger interval was spent executing the batch.
   - Example: Execution of 1,980 ms on a 5,000 ms trigger gives $\text{trigger\_load} = 0.396$ (39.6% load).
   - Values $\ge 0.80$ trigger a `DEGRADED` health state.

2. **Throughput Ratio (Ingest Pressure)**:
   $$\text{throughput\_ratio} = \frac{\text{input\_rows\_per\_second}}{\text{processed\_rows\_per\_second}}$$
   Measures whether ingestion arrival rate exceeds processing drain rate. Values $> 1.0$ indicate backpressure accumulating in the Kafka source.

3. **Kafka Source Backlog**:
   $$\text{backlog\_rows} = \sum_{p} (\text{latest\_offset}_p - \text{spark\_end\_offset}_p)$$
   Calculated directly by querying Kafka partition high-water marks against Spark's committed end offsets from the micro-batch progress report.
   *Note: Spark Structured Streaming manages its own offsets in checkpoint metadata rather than Kafka consumer groups.*

4. **Data Governance & Rejections**:
   - **Streaming Ingest Errors**: Malformed JSON or unparseable event payloads at Kafka ingestion (DLQ/quarantine visibility).
   - **Batch Quarantine**: Bronze $\to$ Silver ETL rejections. Rejection reason counts are **non-mutually-exclusive** (a single trip record with negative fare and invalid dropoff timestamp will increment both counters).

---

## 6. Testing & Verification

UrbanFlow provides a single command to execute the entire verification suite:

```bash
make test
```

This target runs:
1. **`foundation-check`**: Validates schema contracts, fixture hashes, and acceptance fixtures.
2. **`replay-check`**: Validates replay event mappers, rate controllers, and Kafka serializers.
3. **`streaming-check`**: Runs an end-to-end smoke test reading Live Gold Parquet from SeaweedFS S3.
4. **`observability-check`**: Runs pytest unit tests for the Glass Cockpit health state machine and FastAPI endpoints.
5. **`verify_monotonic_virtual_time.py`**: Verifies virtual time monotonicity across multi-cycle replays, proportional time scaling, and burst limiting.
6. **`verify_feb_silver_monotonicity.py`**: Scans 100,000 rows of February 2023 Silver Parquet from S3 to verify strict global chronological ordering.

---

## 7. Troubleshooting & Operational Guide

### Ports Already in Use

If starting the demo fails due to address already in use:
```bash
# Cleanly terminate any hanging host processes
make demo-stop
# Or manually inspect ports
fuser 8000/tcp 8501/tcp 4040/tcp
```

### Inspecting Container Logs

```bash
# Streaming driver query logs
docker logs -f urbanflow-demo-streaming

# Replay producer virtual clock progression
docker logs -f urbanflow-demo-replay

# SeaweedFS storage logs
docker logs -f urbanflow-seaweedfs-1

# Kafka broker logs
docker logs -f urbanflow-kafka-1
```

### Inspecting Host Daemon Logs

```bash
# FastAPI service logs
cat /tmp/urbanflow-fastapi.log

# Streamlit dashboard logs
cat /tmp/urbanflow-streamlit.log
```

### Resetting Streaming State & Checkpoints

If you modify aggregation window definitions and require a fresh checkpoint:
```bash
make demo-stop
docker compose run --rm aws s3 rm --recursive s3://urbanflow/gold/live_zone_metrics/
docker compose run --rm aws s3 rm --recursive s3://urbanflow/checkpoints/
make demo
```
