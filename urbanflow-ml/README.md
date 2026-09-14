# UrbanFlow ML Experiment

Contract-first Spark ML experiment for zone demand forecasting.

This project is experimental. It reads canonical Silver trips and writes only
development artifacts until Foundation change-control approves any official
forecast contract.

Current gate:

```text
ML DATA-GO: not passed until canonical Silver covers the required periods.
```

Run the Silver audit from the repository root:

```powershell
docker-compose --profile integration run --rm spark-submit `
  --conf spark.ui.showConsoleProgress=false `
  --master local[2] `
  /workspace/urbanflow-ml/spark/audit_silver.py
```
