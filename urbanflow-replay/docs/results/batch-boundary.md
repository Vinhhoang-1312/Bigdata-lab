# Batch Silver → Replay Boundary Result

Replay read Batch's canonical Parquet directly through the normal integrated
source adapter; no boundary-only rename or cast was introduced.

```text
batch_replay_rows=2888729
DIRECT SILVER PARQUET = PASS
INTEGRATED EVENT CONTRACT = PASS
published_batch_rows=100
BATCH SILVER TO KAFKA = PASS
BATCH TO REPLAY INTEGRATION = PASS
```

The sampled events contained exactly seven fields, February pickup timestamps
serialized with `-05:00`, unique IDs, valid numeric values, and decimal pickup
zone Kafka keys. A generic consumer received the 100 acknowledged events.
