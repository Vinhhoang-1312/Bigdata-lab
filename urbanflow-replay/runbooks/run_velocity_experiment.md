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
