# UrbanFlow — Project Introduction and Conceptual Overview

## 1. Project Overview

**UrbanFlow** is an educational **Big Data engineering and analytics demonstration** built around urban mobility data.

The project demonstrates how a modern data pipeline can work with both:

- a **large historical dataset at rest**, and
- a **high-rate stream of events in motion**.

Public urban mobility data, primarily NYC taxi-trip data, is used as the example domain.

UrbanFlow is not intended to be a production smart-city platform or a machine-learning project. Its purpose is to provide a compact, understandable demonstration of fundamental Big Data concepts and technologies working together in one end-to-end system.

---

## 2. Main Purpose

The main purpose of UrbanFlow is to demonstrate the lifecycle:

```text
Data Sources
    ↓
Ingestion
    ↓
Storage
    ↓
Processing
    ↓
Analytics
```

Rather than demonstrating individual technologies in isolation, the project shows **why different Big Data technologies exist and how they work together**.

The primary Big Data characteristics demonstrated are:

```text
Volume   → large historical datasets
Velocity → high-rate event streams
Variety  → several useful data formats
```

The main emphasis is on **Volume and Velocity**, while Variety is included in a practical and controlled way.

---

## 3. The Problem Being Demonstrated

Urban mobility produces large amounts of trip data over time.

Historical data can tell us:

> **What normally happens in a particular city zone at a particular time?**

A live or simulated real-time stream can tell us:

> **What is happening there right now?**

Combining them allows UrbanFlow to answer a more interesting analytical question:

> **Is what is happening right now unusual compared with historical behavior?**

For example:

```text
Zone: 161
Time: Monday, 08:00–08:05

Historical expected trips: 103
Current streamed trips:    214

Demand ratio: 2.08
```

This means current trip activity is approximately twice the historical expectation.

The analytical formula is intentionally simple. The main learning value comes from the **data architecture, distributed processing, batch/stream integration, and analytics pipeline** surrounding it.

---

## 4. What the Project Demonstrates

UrbanFlow demonstrates several core Big Data concepts in one system.

### Big Data processing

- Apache Spark distributed processing
- Spark RDD
- Spark DataFrame
- Spark SQL
- Batch processing
- Spark Structured Streaming

### Streaming

- Apache Kafka
- Producers and consumers
- Topics and partitions
- Rate-controlled event replay
- Streaming throughput and processing behavior

### Data storage and organization

- SeaweedFS as self-hosted S3-compatible object storage
- Hadoop S3A access
- Parquet
- Bronze / Silver / Gold organization
- Partitioned analytical datasets

### Analytics

- Historical mobility baselines
- Real-time event aggregation
- Current-vs-historical comparison
- Demand-ratio analysis
- Spark SQL analytical queries
- A lightweight Streamlit/web analytics application

### Big Data characteristics

```text
Volume
    Multi-month datasets containing millions of records

Velocity
    Kafka event streams at configurable events-per-second rates

Variety
    Parquet + JSON + CSV + GeoJSON/geographic data
```

### Source data

The historical dataset is the public **NYC TLC Trip Record Data**
(<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>) — Yellow-taxi
monthly Parquet files — together with the taxi-zone lookup table (CSV) and
taxi-zone boundaries (shapefile / GeoJSON) as reference data. Concrete download
links are listed in `UrbanFlow_urbanflow-batch_Subproject_Overview_Architecture_and_Development_Guide_2026-09-06.md`
(§1.3 and §3.1) and in Gate 7 of `UrbanFlow_Proposal_and_Development_Guide_2026-09-06.md`.

---

## 5. Key Ideas

### 5.1 One continuous data story

The most important idea is that UrbanFlow is not simply a collection of Spark, Kafka, and storage examples.

They form one continuous pipeline:

```text
Urban mobility data
        ↓
     Ingestion
        ↓
      Storage
        ↓
 Spark Processing
        ↓
 Gold data products
        ↓
     Analytics
```

Each technology has a specific responsibility within that story.

---

### 5.2 Historical and streaming processing complement each other

UrbanFlow contains two related processing paths.

#### Historical path

```text
Historical source data
        ↓
Bronze
        ↓
Silver
        ↓
Spark batch processing
        ↓
Historical Gold baseline
```

This establishes what activity normally looks like.

#### Streaming path

```text
Held-out trip records
        ↓
Replay Producer
        ↓
Kafka
        ↓
Spark Structured Streaming
        ↓
Current activity
        ↓
Historical-baseline comparison
        ↓
Live Gold metrics
```

The two paths meet during analytics.

---

### 5.3 Bronze, Silver, and Gold represent increasing analytical readiness

```text
Bronze
  Raw/source-oriented data
        ↓
Silver
  Cleaned and standardized reusable data
        ↓
Gold
  Analytics-ready data products
```

This gives the project a clear separation between raw data, reusable processed data, and analytical results.

---

### 5.4 Use the right abstraction for the right purpose

Spark is intentionally demonstrated at several levels:

```text
RDD
    ↓
lower-level distributed processing concepts

DataFrame
    ↓
main ETL and transformation interface

Spark SQL
    ↓
analytical querying

Structured Streaming
    ↓
continuous event processing
```

One simple analytical problem can also be implemented using RDD, DataFrame, and SQL to show how these abstractions relate to each other.

---

### 5.5 Keep analytics separate from data engineering

The final Streamlit/web application consumes **Gold outputs**.

It does not reproduce the ETL or streaming logic.

Conceptually:

```text
Historical Gold
       +
Live Gold
       ↓
Analytics Application
       ↓
Dashboard
Charts / tables
Filters
Comparisons
Exploration
```

This keeps the analytics-serving layer simple and focused.

---

## 6. Conceptual Architecture

At the highest level:

```text
┌─────────────────────────────────────┐
│            DATA SOURCES             │
│                                     │
│ NYC taxi Parquet                    │
│ Zone CSV                            │
│ Zone GeoJSON                        │
└──────────────────┬──────────────────┘
                   ↓
┌─────────────────────────────────────┐
│             INGESTION               │
│                                     │
│ Batch acquisition                   │
│ Kafka replay producer               │
└──────────────────┬──────────────────┘
                   ↓
┌─────────────────────────────────────┐
│              STORAGE                │
│                                     │
│ SeaweedFS / S3A                     │
│ Parquet                             │
│ Bronze → Silver → Gold              │
│ Kafka event transport               │
└──────────────────┬──────────────────┘
                   ↓
┌─────────────────────────────────────┐
│             PROCESSING              │
│                                     │
│ Spark batch processing              │
│ RDD / DataFrame / SQL               │
│ Structured Streaming                │
│ Event-time windows                  │
│ Historical comparison               │
└──────────────────┬──────────────────┘
                   ↓
┌─────────────────────────────────────┐
│              ANALYTICS              │
│                                     │
│ Historical Gold                     │
│ Live Gold                           │
│ Demand ratio                        │
│ Spark SQL                           │
│ Streamlit / web analytics           │
└─────────────────────────────────────┘
```

---

## 7. Implementation Design

The conceptual system is implemented through three application subprojects governed by a shared contract foundation:

```text
                 urbanflow-foundation
                /         |          \
               /          |           \
              ↓           ↓            ↓

     urbanflow-batch  urbanflow-replay  urbanflow-streaming
```

### A — `urbanflow-batch`

```text
Source data
    ↓
Bronze
    ↓
Silver
    ↓
Historical Gold
```

Responsible mainly for historical ingestion, cleaning, Spark batch processing, and historical analytics.

### B — `urbanflow-replay`

```text
Silver
   ↓
Replay Producer
   ↓
Kafka
```

Responsible for turning held-out trip records into configurable real-time-style Kafka traffic.

### C — `urbanflow-streaming`

```text
Kafka + Historical Gold
            ↓
Structured Streaming
            ↓
Live Gold
            ↓
Analytics Application
```

Responsible for continuous processing, historical comparison, live analytical metrics, and analytics serving.

The **Shared Foundation** defines common contracts, schemas, time semantics, storage paths, fixtures, and acceptance rules so the three application codebases can be developed and tested independently.

---

## 8. Important Design Principles

UrbanFlow deliberately follows several simple principles:

**Every technology must have a reason to exist.**

Spark performs distributed processing. Kafka transports streaming events. SeaweedFS provides object storage. Parquet stores analytical datasets. Streamlit exposes analytics.

**Share contracts, not application implementation.**

The three application subprojects communicate through defined data contracts rather than importing each other's application code.

**Keep the system educationally complete but technically manageable.**

The project demonstrates the important Big Data concepts without expanding into unnecessary infrastructure such as Kubernetes, Airflow, a full lakehouse platform, complex microservices, or machine learning.

**Prefer a clear end-to-end story over infrastructure complexity.**

The main project story remains:

```text
Data Sources
    ↓
Ingestion
    ↓
Storage
    ↓
Processing
    ↓
Analytics
```

---

## 9. In One Sentence

> **UrbanFlow demonstrates how large historical data and high-rate event streams can be ingested, stored, processed with Spark and Kafka, transformed into analytical data products, and finally served through an interactive analytics layer.**

---

## 10. Project at a Glance

```text
                    URBANFLOW

 Real historical data                Simulated live data
         │                                   │
         ▼                                   ▼
      Volume                              Velocity
         │                                   │
         └──────────────┬────────────────────┘
                        ▼
                 Big Data Pipeline
                        │
     Data Sources → Ingestion → Storage
                        ↓
                    Processing
                        ↓
                     Analytics
                        │
                        ▼
      "What is happening now compared
         with what normally happens?"
```