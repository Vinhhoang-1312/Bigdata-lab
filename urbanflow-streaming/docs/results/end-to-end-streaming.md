# End-to-End Streaming Result

Date: 2026-09-14T12:32:36Z
Status: `PASS`
Run ID: `task6-20260914-003`

## Summary

- Replay events: `10000`
- Invalid rows: `0`
- Reference rows: `4394`
- Live rows: `4394`
- Mismatched rows: `0`
- Output path: `s3a://urbanflow/gold/live_zone_metrics/task6/task6_20260914_003/`
- Checkpoint path: `s3a://urbanflow/checkpoints/live_trip_metrics/task6/task6_20260914_003/`

## Mismatch Examples

```json
[]
```

## Streaming Progress

```json
{
  "batchDuration": 18949,
  "batchId": 1,
  "durationMs": {
    "addBatch": 18674,
    "commitOffsets": 59,
    "getBatch": 0,
    "latestOffset": 1,
    "queryPlanning": 111,
    "triggerExecution": 18949,
    "walCommit": 95
  },
  "eventTime": {
    "watermark": "2023-02-14T23:36:43.000Z"
  },
  "id": "62d4adaa-9c11-4953-8fbb-29a21a24f298",
  "inputRowsPerSecond": 0.0,
  "name": null,
  "numInputRows": 0,
  "observedMetrics": {},
  "processedRowsPerSecond": 0.0,
  "runId": "fdb89963-761e-4e82-b2e0-f6b373423ee2",
  "sink": {
    "description": "FileSink[s3a://urbanflow/gold/live_zone_metrics/task6/task6_20260914_003]",
    "metrics": {},
    "numOutputRows": -1
  },
  "sources": [
    {
      "description": "KafkaV2[Subscribe[urbanflow.trip-events]]",
      "endOffset": "{\"urbanflow.trip-events\": {\"0\": 5814, \"1\": 9306, \"2\": 8100, \"3\": 6801}}",
      "inputRowsPerSecond": 0.0,
      "latestOffset": "{\"urbanflow.trip-events\": {\"0\": 5814, \"1\": 9306, \"2\": 8100, \"3\": 6801}}",
      "metrics": {
        "avgOffsetsBehindLatest": "0.0",
        "maxOffsetsBehindLatest": "0",
        "minOffsetsBehindLatest": "0"
      },
      "numInputRows": 0,
      "processedRowsPerSecond": 0.0,
      "startOffset": "{\"urbanflow.trip-events\": {\"0\": 5814, \"1\": 9306, \"2\": 8100, \"3\": 6801}}"
    }
  ],
  "stateOperators": [
    {
      "allRemovalsTimeMs": 5451,
      "allUpdatesTimeMs": 218,
      "commitTimeMs": 12579,
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
        "stateOnCurrentVersionSizeBytes": 105632
      },
      "memoryUsedBytes": 1191424,
      "numRowsDroppedByWatermark": 0,
      "numRowsRemoved": 4394,
      "numRowsTotal": 1,
      "numRowsUpdated": 0,
      "numShufflePartitions": 200,
      "numStateStoreInstances": 200,
      "operatorName": "stateStoreSave"
    }
  ],
  "timestamp": "2026-09-14T12:32:11.120Z"
}
```

## Limitations

- Run-scoped Live Gold path used for safe verification.
- Replay was run before availableNow consumption in this bounded verifier.
- Streamlit visual smoke is recorded separately when the app is launched.
