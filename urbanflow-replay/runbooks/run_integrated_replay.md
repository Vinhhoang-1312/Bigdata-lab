# Run Canonical February Replay

Canonical Batch Silver must already exist at
`s3a://urbanflow/silver/trips/`.

Verify the direct Batch boundary and publish 100 sampled real events:

```bash
docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/tests/verify_batch_boundary.py
```

Expected source count:

```text
batch_replay_rows=2888729
```

Run a bounded canonical replay:

```bash
docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/producer/replay_trips.py \
  --mode integrated --rate 1000 --max-events 10000 --print-events 1 \
  --metrics-output /workspace/urbanflow-replay/output/integrated-replay.json
```

Omit `--max-events` only when deliberately replaying all 2,888,729 February
rows. Kafka retains events between runs; CLI-generated run suffixes keep event
IDs unique across intentional repeated runs.
