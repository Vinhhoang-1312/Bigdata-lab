# End-to-End Streaming Result

Date: 2026-09-20T06:33:08Z
Status: `PASS`
Run ID: `stream-health-20260920-001`

## Summary

- Replay events: `10000`
- Invalid rows: `0`
- Reference rows: `2863`
- Live rows: `2863`
- Mismatched rows: `0`
- Output path: `s3a://urbanflow/gold/live_zone_metrics/task6/stream_health_20260920_001/`
- Checkpoint path: `s3a://urbanflow/checkpoints/live_trip_metrics/task6/stream_health_20260920_001/`

## Mismatch Examples

```json
[]
```

## Streaming Progress

```json
{
  "batchDuration": 25925,
  "batchId": 1,
  "durationMs": {
    "addBatch": 25458,
    "commitOffsets": 147,
    "getBatch": 0,
    "latestOffset": 1,
    "queryPlanning": 144,
    "triggerExecution": 25925,
    "walCommit": 164
  },
  "eventTime": {
    "watermark": "2023-02-19T23:06:41.000Z"
  },
  "id": "d94e47c4-3066-4c88-bcb1-8e29e3af4d03",
  "inputRowsPerSecond": 0.0,
  "name": null,
  "numInputRows": 0,
  "observedMetrics": {},
  "processedRowsPerSecond": 0.0,
  "runId": "c3239daa-f30c-470f-869a-203f587042a0",
  "sink": {
    "description": "FileSink[s3a://urbanflow/gold/live_zone_metrics/task6/stream_health_20260920_001]",
    "metrics": {},
    "numOutputRows": -1
  },
  "sources": [
    {
      "description": "KafkaV2[Subscribe[urbanflow.trip-events]]",
      "endOffset": "{\"urbanflow.trip-events\": {\"0\": 7823, \"1\": 12176, \"2\": 11003, \"3\": 9019}}",
      "inputRowsPerSecond": 0.0,
      "latestOffset": "{\"urbanflow.trip-events\": {\"0\": 7823, \"1\": 12176, \"2\": 11003, \"3\": 9019}}",
      "metrics": {
        "avgOffsetsBehindLatest": "0.0",
        "maxOffsetsBehindLatest": "0",
        "minOffsetsBehindLatest": "0"
      },
      "numInputRows": 0,
      "processedRowsPerSecond": 0.0,
      "startOffset": "{\"urbanflow.trip-events\": {\"0\": 7823, \"1\": 12176, \"2\": 11003, \"3\": 9019}}"
    }
  ],
  "stateOperators": [
    {
      "allRemovalsTimeMs": 7457,
      "allUpdatesTimeMs": 402,
      "commitTimeMs": 17313,
      "customMetrics": {
        "SnapshotLastUploaded.partition_139_default": -1,
        "SnapshotLastUploaded.partition_157_default": -1,
        "SnapshotLastUploaded.partition_172_default": -1,
        "SnapshotLastUploaded.partition_178_default": -1,
        "SnapshotLastUploaded.partition_33_default": -1,
        "forceSnapshotCount": 0,
        "loadedMapCacheHitCount": 400,
        "loadedMapCacheMissCount": 0,
        "numSnapshotsAutoRepaired": 0,
        "stateOnCurrentVersionSizeBytes": 76096
      },
      "memoryUsedBytes": 806304,
      "numRowsDroppedByWatermark": 0,
      "numRowsRemoved": 2863,
      "numRowsTotal": 2,
      "numRowsUpdated": 0,
      "numShufflePartitions": 200,
      "numStateStoreInstances": 200,
      "operatorName": "stateStoreSave"
    }
  ],
  "timestamp": "2026-09-20T06:32:25.977Z"
}
```

## Limitations

- Run-scoped Live Gold path used for safe verification.
- Replay was run before availableNow consumption in this bounded verifier.
- Streamlit visual smoke is recorded separately when the app is launched.
