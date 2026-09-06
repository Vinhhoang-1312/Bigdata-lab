# Replay Velocity Experiment Result

## Environment

- Source: canonical February 2023 Silver over PyArrow/S3
- Source population: 2,888,729 rows
- Kafka: Apache Kafka 3.9.1, one KRaft broker
- Topic: `urbanflow.trip-events`, 4 partitions, replication factor 1
- Producer: confluent-kafka/librdkafka 2.15.0
- Python: 3.13.11
- Measurement duration: approximately two seconds per target

## Results

| Requested events/sec | Events sent | Duration (sec) | Achieved events/sec |
|---:|---:|---:|---:|
| 100 | 200 | 2.002 | 99.92 |
| 500 | 1,000 | 2.004 | 498.93 |
| 1,000 | 2,000 | 2.003 | 998.69 |
| 2,500 | 5,000 | 2.067 | 2,419.28 |
| 5,000 | 10,000 | 2.003 | 4,992.41 |

All 18,200 events were acknowledged. Rejected records and send errors were
zero at every rate. Every achieved rate was within 90–110% of its target.

```text
REQUESTED VS ACHIEVED RATE = PASS
REPLAY VELOCITY EXPERIMENT = PASS
```
