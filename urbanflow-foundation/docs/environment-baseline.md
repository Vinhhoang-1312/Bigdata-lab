# Environment Baseline

**Foundation version:** 1.0.0

## Purpose

This is a template for the environment lock record. After the relevant smoke tests pass, fill in the **actual working combination** — not merely intended versions.

The completed version should be stored as `docs/environment-lock.md` in the project or integration documentation.

## Template

```text
URBANFLOW ENVIRONMENT LOCK — v1

Foundation:
  urbanflow-foundation 1.0.0

Primary host:
  Windows 11 <exact build>

Docker Desktop:
  <exact version>

Java Windows:
  21.0.x

Java WSL:
  21.0.x

Python Windows:
  3.13.x

Python WSL:
  3.13.x

Spark:
  4.2.0

PySpark:
  4.2.0

Scala:
  2.13.x

Hadoop:
  <resolved version>

hadoop-aws:
  <resolved version>

AWS SDK:
  <resolved version>

Kafka distribution:
  Confluent Platform 8.2.1

Kafka broker/runtime:
  <exact version reported by installed environment>

Kafka deployment:
  WSL Ubuntu

Kafka mode/configuration:
  <resolved project setting>

Kafka topic:
  urbanflow.trip-events

Kafka partitions:
  4

Kafka replication factor:
  1

SeaweedFS:
  <validated exact version>

SeaweedFS S3 endpoint:
  <resolved integration endpoint>

Storage bucket:
  urbanflow

Analytics timezone:
  America/New_York
```

## Rules

After this record is frozen:

- Do not upgrade dependencies during implementation simply because a newer release appears.
- Change a locked dependency only for a concrete blocker, required fix, or compatibility reason.
- Any change that affects a cross-codebase interface also requires a Foundation version/change-control decision.
- The three independent machines do not have to be byte-for-byte identical, but any runtime difference that changes a Foundation contract, timestamp interpretation, serialization, or Spark/Kafka behavior is not allowed without explicit validation.
