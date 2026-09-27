# UrbanFlow: Current Streaming Reality and Use Cases

## 1. What the project can process

UrbanFlow is designed for an unbounded event stream:

```text
NYC TLC Silver or replay producer
        -> Apache Kafka: urbanflow.trip-events
        -> Spark Structured Streaming readStream
        -> event validation
        -> 5-minute event-time window + 10-minute watermark
        -> Historical Gold join
        -> Live Gold Parquet in SeaweedFS
        -> FastAPI read API
        -> React Streaming workspace
```

The stream code is not a batch-only DataFrame. The production-shaped path uses
`readStream`, event time, a watermark, a checkpoint, and a continuously running
query. The replay producer is a finite historical source that makes the stream
look real-time for a local experiment.

### Important current-state boundary

`start-urbanflow.bat` starts Docker infrastructure, the two read APIs, React,
and Spark Master UI. It does **not** start the Kafka replay producer or the
long-running `UrbanFlowLiveMetrics` Spark application. Therefore:

- Kafka can be healthy while no event is being produced.
- Spark workers can be healthy while no streaming query is active.
- A Gold snapshot can be valid while still being a historical replay.
- The Reliability view must report the stream as stopped when the Spark app is
  not running.

To prove a live-style run, start the replay producer and the continuous Spark
query separately, then verify new Kafka offsets, query progress, checkpoint
updates, and new Live Gold windows.

## 2. Kafka partitions and ordering

The topic has four partitions. The Kafka key is the decimal
`pickup_zone_id`, so events for one pickup zone normally go to one stable
partition while the topic configuration remains unchanged.

Kafka guarantees order **inside one partition**, not one global order across all
four partitions. The application must therefore use `event_timestamp` and
Spark event-time windows, not arrival order. The ten-minute watermark bounds how
long Spark waits for late events. Events later than the watermark can be
dropped from the finalized aggregation.

The current design already addresses several issues:

- explicit RFC 3339 timestamps with a New York offset;
- strict event validation before aggregation;
- a left join so a current window is not silently lost when its baseline is
  missing;
- separate Live Gold and checkpoint paths;
- checkpoint-based restart of the Spark query.

Still open for a production-grade system:

- current Kafka consumer lag is not exposed by the API;
- invalid-record counts are not persisted as a durable metric;
- the topic is one broker with replication factor one, so it is not highly
  available;
- duplicate-event policy is not a complete business-level exactly-once
  guarantee;
- a replay is finite and cannot prove an always-on service.

## 3. Where Kafka data is stored

Kafka stores its log segments in the Docker named volume `kafka-data`, mounted
inside the broker at `/var/lib/kafka/data`. This is Kafka's transport retention,
not the permanent analytical archive.

The durable UrbanFlow data products are separate:

| Data | Storage |
| --- | --- |
| Silver trips | `s3a://urbanflow/silver/trips/` |
| Historical Gold | `s3a://urbanflow/gold/historical_zone_baseline/` |
| Live Gold | `s3a://urbanflow/gold/live_zone_metrics/` |
| Spark streaming checkpoint | `s3a://urbanflow/checkpoints/live_trip_metrics/` |
| Kafka broker log | Docker volume `kafka-data` |

Kafka retention is controlled by broker/topic retention settings. Do not treat
Kafka as the long-term source of analytical truth; replay or archive the
contracted Parquet data when long retention is required.

## 4. Current real-time analytics use cases

### Supported now

1. **Zone demand monitoring**: count pickups in five-minute windows and compare
   them with the historical Gold baseline using demand ratio and z-score.
2. **Operational review support**: show the busiest or unusual zones on a map
   so a human can inspect the current replay window.
3. **Data-pipeline reliability**: distinguish a valid historical Gold snapshot
   from an active Spark streaming job, and show when telemetry is unavailable.
4. **Historical playback**: replay completed five-minute Gold windows by month
   in React, including the pickup-by-hour chart.
5. **Trip-pattern screening**: Spark MLlib K-means can score a trip using
   distance, duration, fare, and derived speed. The current UI exposes this as
   a human-review signal, not as a live fraud decision.

### Not supported honestly yet

- fraud detection: there are no verified fraud labels or a fraud classifier;
- recommendation systems: there is no user/item feedback or recommender model;
- safety verdicts: an anomaly score is not proof of unsafe driving;
- live passenger dispatch automation: the current stream is a research replay
  and the forecast model is research-only;
- a claimed accuracy percentage for K-means: K-means is unsupervised, so use
  review rate, stability, cluster diagnostics, and human validation instead.

The practical next use case is **human-in-the-loop operational triage**:
streaming demand identifies zones that deserve attention, while K-means ranks
trip records for review. It should not automatically accuse a driver,
cancel a trip, or make a safety decision.

## 5. Canonical project location

The canonical Git worktree is:

```text
C:\Users\DELL\Documents\Big Data
```

The former `.tmp/team-merge-20260926` directory was a temporary Git worktree
used to isolate the integration branch. It has been removed after merging into
the canonical `urbanflow-streaming` worktree. The local integration branch is
kept as Git history, but it is not another project directory.
