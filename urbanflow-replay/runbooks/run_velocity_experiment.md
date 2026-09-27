# Run the Replay Velocity Experiment

```bash
docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/tests/run_velocity_experiment.py
```

The experiment uses canonical February Silver and publishes for two seconds at
each target:

```text
100, 500, 1,000, 2,500, 5,000 events/sec
```

Each run must have zero rejected rows and send errors, and achieved throughput
must stay within 90–110% of requested throughput. Raw local JSON is written to
ignored `urbanflow-replay/output/velocity-results.json`; committed evidence is
under `docs/results/`.

## Remaining cross-subproject acceptance: Streaming active during Replay

The producer-only result above has already passed. The September 14 and 20
10,000-event runs separately passed bounded Replay CLI → Kafka → Spark
Structured Streaming → Live Gold equality, but Replay completed before each
`availableNow` consumption. Do not repeat either result as an open gate.

For the final velocity gate, use canonical February Silver and the canonical
Kafka topic with a unique Replay `--run-id` for each requested rate. Start a
Spark Structured Streaming query **before** publishing and keep it active while
Replay publishes at each rate (100, 500, 1,000, 2,500, 5,000 events/sec).
A continuous trigger with an isolated output and checkpoint is appropriate;
allow it to drain and finalize eligible windows before comparing Gold. Record
the producer start/end interval and Spark query progress showing input rows
consumed during that interval. A query started only after Replay exits does
not meet this condition.

For each rate, retain the requested and achieved rate, acknowledged count,
rejections and send errors, run ID, query progress/offsets and invalid count.
Require the existing 90–110% producer-rate band, zero rejected/send-error
events, and Spark progress that demonstrates concurrent ingestion without
unresolved backlog at drain. Compare the run-scoped Live Gold keys and metrics
against an independent reference for the same run ID and finalized windows;
require equal row counts and zero mismatches. Use fresh run-scoped paths or
an equivalent isolated checkpoint/offset boundary so earlier topic data cannot
satisfy the check. Record failures as failures, including timeouts or missing
progress. The existing `urbanflow-streaming/tests/verify_integrated_run.py`
can inform the equality comparison, but its current `availableNow` flow
does not itself prove concurrent consumption.

This is a bounded final experiment. It does not establish an unattended,
continuously running production service.
