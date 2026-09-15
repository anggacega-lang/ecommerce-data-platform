# E-commerce Data Platform — Architecture

## 1. Architecture Overview

The E-commerce Data Platform is a local data engineering platform designed to demonstrate an end-to-end modern data pipeline.

The architecture uses **Change Data Capture (CDC)** to capture transactional changes from PostgreSQL, **Apache Kafka** to stream those changes, **Apache Spark Structured Streaming** to process the events, and **MinIO** as an S3-compatible object storage layer.

Processed data is organized using the **Medallion Architecture**:

```text
Bronze → Silver → Gold
```

The high-level architecture is:

```text
┌──────────────────────┐
│     PostgreSQL       │
│   Transactional DB   │
└──────────┬───────────┘
           │
           │ WAL / Logical Replication
           ▼
┌──────────────────────┐
│      Debezium        │
│   CDC Connector      │
└──────────┬───────────┘
           │
           │ CDC Events
           ▼
┌──────────────────────┐
│        Kafka         │
│   Event Streaming    │
└──────────┬───────────┘
           │
           │ Kafka Topics
           ▼
┌──────────────────────┐
│        Spark         │
│ Structured Streaming │
└──────────┬───────────┘
           │
           │ Data Processing
           ▼
┌─────────────────────────────────┐
│             MinIO               │
│        S3-Compatible Storage     │
│                                 │
│   ┌────────┐ ┌────────┐ ┌─────┐│
│   │ Bronze │ │ Silver │ │Gold ││
│   └────────┘ └────────┘ └─────┘│
└────────────────┬────────────────┘
                 │
                 ▼
        ┌─────────────────┐
        │ Analytics / BI  │
        │ DuckDB/Metabase │
        └─────────────────┘
```

---

## 2. End-to-End Data Flow

The complete data flow can be summarized as:

```text
PostgreSQL
    │
    │ INSERT / UPDATE / DELETE
    ▼
Debezium
    │
    │ CDC Event
    ▼
Kafka
    │
    │ Topic
    ▼
Spark Structured Streaming
    │
    │ Transform / Process
    ▼
MinIO
    │
    ├── Bronze
    │
    ├── Silver
    │
    └── Gold
    │
    ▼
Analytics
```

Each component has a specific responsibility.

| Layer      | Component          | Responsibility                 |
| ---------- | ------------------ | ------------------------------ |
| Source     | PostgreSQL         | Transactional source system    |
| CDC        | Debezium           | Capture database changes       |
| Streaming  | Kafka              | Transport CDC events           |
| Processing | Spark              | Transform and process data     |
| Storage    | MinIO              | Store data lake datasets       |
| Bronze     | Spark + MinIO      | Raw CDC data                   |
| Silver     | Spark + MinIO      | Clean and conformed data       |
| Gold       | Spark + Delta Lake | Business-ready analytical data |
| Analytics  | DuckDB / Metabase  | Query and visualization        |

---

# 3. Source Layer — PostgreSQL

PostgreSQL acts as the transactional source database.

The database contains e-commerce entities such as:

```text
customers
products
orders
order_items
payments
inventory
```

The source database is responsible for transactional operations.

For example, when a customer record changes:

```sql
UPDATE customers
SET city = 'Bandung'
WHERE customer_id = 4;
```

the change is committed to PostgreSQL first.

The downstream pipeline does not directly query PostgreSQL repeatedly to detect the change. Instead, the change is captured through CDC.

---

# 4. Change Data Capture — Debezium

Debezium is responsible for capturing changes from PostgreSQL.

The CDC mechanism uses PostgreSQL logical replication to identify database changes.

The flow is:

```text
PostgreSQL
     │
     │ Logical Replication
     ▼
 Debezium
     │
     ▼
 CDC Event
```

The CDC event contains information about the state of the record before and after the change.

Conceptually:

```json
{
  "before": {},
  "after": {},
  "op": "u"
}
```

The `op` field identifies the type of operation.

| Operation | Meaning         |
| --------- | --------------- |
| `c`       | Create / Insert |
| `u`       | Update          |
| `d`       | Delete          |
| `r`       | Read / Snapshot |

For an update:

```text
before → previous record
after  → updated record
op     → u
```

This information allows downstream processing to understand the actual database change.

---

# 5. Kafka — Event Streaming Layer

Apache Kafka acts as the event streaming layer.

Debezium publishes CDC events into Kafka topics.

The topic naming convention follows the e-commerce source structure.

Example:

```text
ecommerce.public.customers
ecommerce.public.products
ecommerce.public.orders
```

The general structure is:

```text
<topic-prefix>.<schema>.<table>
```

For this project:

```text
ecommerce.public.<table>
```

Kafka provides several important capabilities:

* Decoupling source and consumers
* Durable event storage
* Partitioned event streams
* Consumer offsets
* Scalable event processing
* Ability to replay events

---

## 5.1 Kafka Partitions

Kafka topics are divided into partitions.

For example:

```text
ecommerce.public.customers

Partition 0
Partition 1
Partition 2
```

Events within a partition maintain their ordering.

Kafka offsets allow consumers such as Spark to track their progress.

Conceptually:

```text
Partition 0
  offset 0
  offset 1
  offset 2
  ...

Partition 1
  offset 0
  offset 1
  offset 2
  ...

Partition 2
  offset 0
  offset 1
  ...
```

This provides the foundation for reliable stream processing.

---

# 6. Spark Structured Streaming

Apache Spark is responsible for consuming Kafka events and processing the CDC data.

The processing flow is:

```text
Kafka
  │
  ▼
Spark Structured Streaming
  │
  ├── Read Kafka event
  ├── Parse Debezium payload
  ├── Extract before / after
  ├── Identify operation
  ├── Transform data
  └── Write to data lake
```

The Spark processing layer is designed to separate event ingestion from analytical transformations.

Important event metadata can include:

```text
topic
partition
offset
kafka_timestamp
event_timestamp
processed_at
operation
```

This metadata is useful for debugging, traceability, and monitoring.

---

# 7. Data Lake — MinIO

MinIO provides S3-compatible object storage for the local data platform.

The data lake is logically organized into:

```text
s3://bronze/
s3://silver/
s3://gold/
```

The project uses S3A connectivity from Spark to communicate with MinIO.

Conceptually:

```text
Spark
  │
  │ S3A
  ▼
MinIO
```

This allows the local environment to simulate an object-storage-based data lake architecture similar to cloud environments.

---

# 8. Medallion Architecture

The data lake follows the Medallion Architecture.

```text
              ┌──────────────┐
              │    Bronze    │
              │ Raw CDC Data │
              └──────┬───────┘
                     │
                     ▼
              ┌──────────────┐
              │    Silver    │
              │ Clean Data   │
              └──────┬───────┘
                     │
                     ▼
              ┌──────────────┐
              │     Gold     │
              │ Analytics    │
              └──────────────┘
```

Each layer has a different purpose.

---

# 9. Bronze Layer

The Bronze layer is the first persistent layer after streaming ingestion.

Its primary purpose is to preserve incoming data with minimal transformation.

```text
Kafka
  │
  ▼
Spark
  │
  ▼
Bronze
```

Bronze data can retain information such as:

```text
before
after
operation
event_timestamp
topic
partition
offset
kafka_timestamp
processed_at
```

The Bronze layer is useful for:

* Reprocessing
* Debugging
* Auditing
* CDC investigation
* Recovering downstream processing

For example, a CDC event can be traced from Kafka into Bronze using its topic, partition, and offset.

---

# 10. Silver Layer

The Silver layer contains cleaned and transformed data.

Typical Silver processing includes:

```text
Bronze
  │
  ├── Parse CDC payload
  ├── Normalize schema
  ├── Convert data types
  ├── Handle CDC operations
  ├── Deduplicate records
  └── Apply business rules
  │
  ▼
Silver
```

The goal is to provide a reliable and consistent dataset for downstream analytical processing.

Silver should contain data that is easier to consume than raw Bronze events.

---

# 11. Gold Layer

The Gold layer contains business-oriented analytical datasets.

Gold datasets are designed for analytics, reporting, and BI workloads.

Example:

```text
gold/order_summary
```

The `order_summary` dataset can combine information from multiple entities such as:

```text
orders
    +
order_items
    +
customers
```

resulting in an analytical dataset suitable for reporting.

Conceptually:

```text
Orders
   │
   ├──────────────┐
   │              │
Order Items    Customers
   │              │
   └──────┬───────┘
          ▼
    Order Summary
          │
          ▼
        Gold
```

---

# 12. Delta Lake

The Gold layer uses Delta Lake format for analytical datasets.

Example:

```scala
spark.read
  .format("delta")
  .load("s3a://gold/order_summary")
```

Delta Lake provides capabilities such as:

* Transactional table management
* Schema enforcement
* Reliable reads and writes
* Data versioning
* Support for analytical workloads

This makes the Gold layer more robust than storing analytical datasets as unmanaged files alone.

---

# 13. Idempotent Processing

An important design principle in this project is idempotent processing.

A pipeline should be safe to rerun without unintentionally creating duplicate records.

Conceptually:

```text
Input Event
    │
    ▼
Processing
    │
    ▼
Target Dataset
```

If the same processing operation is executed again:

```text
Input Event
    │
    ▼
Processing
    │
    ▼
Same Logical Result
```

This is particularly important for CDC pipelines because events may need to be replayed during development, recovery, or troubleshooting.

---

# 14. Data Lineage

The architecture provides a clear lineage path:

```text
PostgreSQL
    │
    ▼
Debezium
    │
    ▼
Kafka Topic
    │
    ▼
Kafka Partition + Offset
    │
    ▼
Spark
    │
    ▼
Bronze
    │
    ▼
Silver
    │
    ▼
Gold
```

This allows a downstream record to be traced back toward its source event.

For example:

```text
Gold order_summary
       │
       ▼
Silver order data
       │
       ▼
Bronze CDC record
       │
       ▼
Kafka topic / partition / offset
       │
       ▼
PostgreSQL source record
```

---

# 15. Reliability Considerations

The architecture is designed around several data engineering principles.

## 15.1 Decoupling

PostgreSQL does not need to communicate directly with every downstream processing component.

Kafka provides an intermediary event streaming layer.

```text
PostgreSQL
    │
    ▼
 Kafka
    │
    ├── Spark
    ├── Other Consumers
    └── Future Consumers
```

---

## 15.2 Replayability

Kafka retains events according to its configured retention policy.

This allows consumers to replay historical events when necessary.

For example:

```text
Kafka
  │
  ├── Current processing
  │
  └── Replay from offset
```

This is useful when debugging or rebuilding downstream datasets.

---

## 15.3 Observability

Kafka topic, partition, and offset metadata can be retained during processing.

This makes it possible to investigate:

```text
Where did the event come from?
Which partition contained it?
Which offset was processed?
When was it processed?
```

---

# 16. Architecture Decisions

### Why PostgreSQL?

PostgreSQL represents a realistic transactional database and provides native logical replication capabilities required for CDC.

### Why Debezium?

Debezium provides a standard CDC solution that can capture database changes without requiring application-level changes to every transaction.

### Why Kafka?

Kafka decouples data producers from consumers and provides durable, partitioned event streams.

### Why Spark?

Spark provides a unified processing engine capable of handling both streaming and batch workloads.

### Why MinIO?

MinIO provides S3-compatible object storage locally, allowing the project to simulate cloud data lake patterns.

### Why Medallion Architecture?

Bronze, Silver, and Gold layers provide clear separation between raw ingestion, data transformation, and business-ready analytical data.

### Why Delta Lake?

Delta Lake provides reliable table management and transactional capabilities for analytical datasets.

---

# 17. Local Deployment

All major infrastructure components are deployed locally using Docker Compose.

The primary configuration is:

```text
docker-compose.yml
```

The platform can be started with:

```bash
docker compose up -d
```

The running containers can be inspected using:

```bash
docker ps
```

The project uses a dedicated Docker network:

```text
ecommerce-data-platform
```

This allows the containers to communicate using their Docker service/container names.

---

# 18. Current Architecture Status

The current implementation has successfully established the core pipeline:

```text
PostgreSQL
     ↓
Debezium
     ↓
Kafka
     ↓
Spark
     ↓
MinIO
     ↓
Bronze
     ↓
Silver / Gold
```

The Gold layer includes a Delta Lake dataset:

```text
s3a://gold/order_summary
```

The architecture will continue to evolve as orchestration, data quality, analytics, and monitoring components are implemented.

---

# 19. Future Architecture

The planned final architecture will extend the current pipeline with orchestration, quality checks, analytics, and observability.

```text
                       PostgreSQL
                           │
                           ▼
                       Debezium
                           │
                           ▼
                         Kafka
                           │
                           ▼
                    Spark Streaming
                           │
                           ▼
                         MinIO
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
           Bronze        Silver        Gold
                                        │
                         ┌──────────────┼──────────────┐
                         ▼              ▼              ▼
                      DuckDB        Metabase       Analytics
                         
             ┌─────────────────────────────────┐
             │          Airflow                 │
             │     Workflow Orchestration      │
             └─────────────────────────────────┘

             ┌─────────────────────────────────┐
             │       Data Quality              │
             │ Validation / Completeness       │
             └─────────────────────────────────┘

             ┌─────────────────────────────────┐
             │       Monitoring                │
             │ Metrics / Logs / Alerts         │
             └─────────────────────────────────┘
```

This architecture provides a foundation for a complete local modern data platform while keeping individual components replaceable and independently scalable.
