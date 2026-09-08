# Run Fixture Replay

From the repository root:

```bash
docker compose up -d --build
docker compose run --rm foundation-check
docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/tests/run_contract_tests.py
```

Preview mapped events without Kafka sends:

```bash
docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/producer/replay_trips.py \
  --mode fixture --dry-run --print-events 7
```

Publish the seven events at 100 events/sec:

```bash
docker compose run --rm replay-python \
  python /workspace/urbanflow-replay/producer/replay_trips.py \
  --mode fixture --rate 100 --max-events 7 --print-events 1
```

Expected markers include `records_serialized=7`, `records_sent=7`, zero errors,
and `REPLAY = PASS` in the final JSON/text output.
