# ML Demand Frame Smoke

Date: 2026-09-15
Status: `PASS`

## Command

```powershell
docker-compose --profile integration run --rm spark-submit `
  --conf spark.ui.showConsoleProgress=false `
  --master local[2] `
  /workspace/urbanflow-ml/spark/build_demand_frame.py `
  --run-id task9-smoke-20260915-001 `
  --start-date 2023-01-02 `
  --end-date 2023-01-02 `
  --zone-lookup s3a://urbanflow/bronze/zone_lookup/
```

## Result

```json
{
  "end_date": "2023-01-02",
  "output_path": "s3a://urbanflow/dev/ml/demand_frame/task9-smoke-20260915-001/",
  "rows": 12720,
  "run_id": "task9-smoke-20260915-001",
  "start_date": "2023-01-02",
  "status": "PASS",
  "zones": 265
}
```

Final marker:

```text
ML DEMAND-FRAME = PASS
```

## Interpretation

This is a writer smoke, not the final full training frame. It proves the Task 9
builder can read canonical Silver plus the integrated S3A zone lookup, create a
zero-inclusive 30-minute zone frame, validate invariants, and write a
run-scoped development Parquet output without overwriting another run.
