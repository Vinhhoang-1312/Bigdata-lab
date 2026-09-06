# UrbanFlow — Big Data Analytics Demo

An educational Big Data engineering demonstration: a large historical mobility
dataset at rest plus a high-rate event stream in motion, moving through one
pipeline.

```text
Data Sources -> Ingestion -> Storage -> Processing -> Analytics
```

**Foundation version:** 1.0.0

## Codebases

| Codebase | Role |
|---|---|
| [urbanflow-foundation/](urbanflow-foundation/) | Contract-first governance: schemas, time/path semantics, fixtures, acceptance tests. Not an importable library. |
| [urbanflow-batch/](urbanflow-batch/) | A — Bronze -> Silver -> historical Gold baseline. |
| [urbanflow-replay/](urbanflow-replay/) | B — Silver -> rate-controlled Kafka trip events. |
| [urbanflow-streaming/](urbanflow-streaming/) | C — Kafka + historical Gold -> live Gold -> analytics app. |

A, B and C are independently buildable and share contracts, not implementation
code. During independent development they use Foundation fixtures in place of
unfinished upstream subprojects.

Full detail lives in
[docs/UrbanFlow_Proposal_and_Development_Guide_2026-09-06.md](docs/UrbanFlow_Proposal_and_Development_Guide_2026-09-06.md);
cross-codebase contracts live in
[urbanflow-foundation/README.md](urbanflow-foundation/README.md). This file
does not restate either — it is the entry point only.

## Prerequisites

### 0. Foundation check (no infrastructure required)

Run this first. It needs only Python 3.13 and the standard library.

```bash
cd urbanflow-foundation
python checks/validate_contract_fixtures.py
```

Expected:

```text
FOUNDATION FIXTURES = PASS
```

### 1. SeaweedFS — S3-compatible object storage (Docker on Windows)

The S3 API must be enabled explicitly; the image does not start it by default.
Port 8333 is the S3 endpoint, 9333 the master, 8888 the filer.

```powershell
docker run -d --name urbanflow-seaweedfs `
  -p 9333:9333 -p 8888:8888 -p 8333:8333 `
  -v weed-data:/data `
  chrislusf/seaweedfs:4.45 `
  server -dir=/data -s3
```

Create the frozen bucket (`urbanflow`, per
`urbanflow-foundation/contracts/storage-paths.yaml`):

```bash
aws s3api create-bucket --bucket urbanflow --endpoint-url http://localhost:8333
```

The image tag above is the **candidate** from the development guide, not a
validated version. Pin the exact tag that passes the Spark <-> SeaweedFS
Parquet round-trip (acceptance test E) in `docs/environment-lock.md`, together
with the resolved Hadoop / `hadoop-aws` / AWS SDK versions. Credentials belong
in environment variables, per
`urbanflow-foundation/config/config-reference.yaml` — never in source.

### 2. Kafka (Confluent Platform, WSL Ubuntu)

Start the broker in KRaft mode, then create the frozen topic:

```bash
kafka-topics --create \
  --topic urbanflow.trip-events \
  --partitions 4 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

Topic name, partition count and replication factor are frozen in
`urbanflow-foundation/contracts/kafka-topic.yaml` — do not vary them per
machine. Record the broker start command your Confluent Platform version
actually uses in `docs/environment-lock.md`.

### 3. Record the environment lock

After the smoke tests in
`urbanflow-foundation/docs/acceptance-tests.md` pass, fill in
`docs/environment-lock.md` from the template in
`urbanflow-foundation/docs/environment-baseline.md`. Record the combination
that actually worked, not the intended versions.

## Acceptance tests and GO gates

| Gate | Meaning |
|---|---|
| FOUNDATION-GO | Test 0 passes, contracts frozen |
| GO-A / GO-B / GO-C | That subproject's local prerequisites and fixtures work |
| INTEGRATION-GO | Tests H, I, J, K pass and the environment lock is complete |

Definitions: `urbanflow-foundation/docs/acceptance-tests.md`.

## Change control

Foundation contracts are frozen. A change affecting more than one subproject is
made in `urbanflow-foundation` first, versioned there, then adopted by the
affected producer and consumer. See
`urbanflow-foundation/docs/change-control.md`.
