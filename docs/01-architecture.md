# E-commerce Data Platform — Architecture

## 1. Overview

This project is a local e-commerce data platform demonstrating an end-to-end pipeline using Change Data Capture (CDC), event streaming, Spark processing, and S3-compatible object storage.

The validated core flow is:

```text
PostgreSQL → Debezium → Kafka → Spark → MinIO Bronze → Silver → Gold
```

The current environment runs locally with Docker Compose.

## 2. Technology Stack

| Component | Purpose | Status |
|---|---|---|
| PostgreSQL 16 | Transactional source database | Implemented and validated |
| Debezium | Captures PostgreSQL changes | Implemented and validated |
| Apache Kafka | CDC event streaming | Implemented and validated |
| Apache Spark 3.5.7 | Data processing | Implemented and validated |
| MinIO | S3-compatible object storage | Implemented and validated |
| Parquet | Bronze storage format | Implemented and validated |
| Delta Lake | Silver and Gold storage format | Implemented and validated |
| DuckDB | Analytical query layer | Planned |
| Metabase | BI and dashboards | Planned |

## 3. Architecture

```text
PostgreSQL
    |
    | Logical replication / CDC
    v
Debezium
    |
    | CDC events
    v
Apache Kafka
    |
    | CDC events
    v
Apache Spark
    |
    v
Bronze
Parquet
    |
    v
Silver
Delta Lake
    |
    v
Gold
Delta Lake
order_summary
```

DuckDB, Metabase, Airflow, dbt/Dataform, and automated data quality components are documented as future work unless separately implemented and validated.

## Architecture Decisions

### Why PostgreSQL?

PostgreSQL provides a realistic transactional source database and supports logical replication for Change Data Capture (CDC).

### Why Debezium?

Debezium captures database changes through CDC without requiring application-level changes to every transaction. It publishes structured change events to Kafka.

### Why Kafka?

Kafka provides a decoupled event streaming layer between the source database and downstream processing. Its topic, partition, and offset model supports event tracking and potential replay while events remain available.

### Why Spark?

Apache Spark provides a processing engine for consuming CDC events and creating Bronze, Silver, and Gold datasets. It also supports both streaming and batch processing patterns.

### Why MinIO?

MinIO provides S3-compatible object storage in the local environment. It allows the project to demonstrate data lake patterns without requiring a cloud storage service.

### Why Medallion Architecture?

The Bronze, Silver, and Gold layers separate raw ingestion, processed data, and analytical datasets. This separation improves clarity and supports independent validation of each processing stage.

### Why Delta Lake?

Delta Lake is used for the Silver and Gold layers to provide a structured table format for analytical datasets. Advanced Delta Lake capabilities are not considered implemented unless separately configured and validated.

## 4. End-to-End Data Flow

The end-to-end processing flow is:

```text
PostgreSQL
    │
    │ Logical Replication / CDC
    ▼
Debezium
    │
    │ CDC Events
    ▼
Kafka
    │
    │ Topic / Partition / Offset
    ▼
Spark Structured Streaming
    │
    ▼
Bronze
Parquet
    │
    ▼
Silver
Delta Lake
    │
    ▼
Gold
Delta Lake
order_summary
```
The current schema contains:

- `customers`
- `products`
- `orders`
- `order_items`
- `payments`
- `inventory`

PostgreSQL logical replication is used to support CDC through Debezium. PostgreSQL remains the source of truth, while Bronze, Silver, and Gold are downstream representations.

## 6. CDC Layer — Debezium

Debezium captures changes from PostgreSQL and publishes them to Kafka.

The configured connector is:

```text
postgres-ecommerce-connector
```

The connector and its task were validated as running.

The CDC operation field includes:

| Operation | Meaning |
|---|---|
| `c` | Insert / create |
| `u` | Update |
| `d` | Delete |
| `r` | Snapshot / read event |

Depending on the event, the payload can contain `before`, `after`, operation information, timestamps, and source metadata.

## 7. Kafka Layer

Kafka transports CDC events from Debezium to Spark.

The topic naming convention is:

```text
ecommerce.public.<table>
```

Examples:

```text
ecommerce.public.customers
ecommerce.public.products
ecommerce.public.orders
ecommerce.public.order_items
ecommerce.public.payments
ecommerce.public.inventory
```

Kafka metadata such as topic, partition, offset, and timestamp can be retained by downstream processing jobs for operational investigation.

The customer topic was validated with three partitions.

### Topics, Partitions, and Offsets

Kafka topics are partitioned logs. Each event is associated with a topic, partition, and offset.

The customer CDC topic was validated with three partitions during local testing.

Conceptually:

```text
ecommerce.public.customers

Partition 0 → offset 0, 1, 2, ...
Partition 1 → offset 0, 1, 2, ...
Partition 2 → offset 0, 1, 2, ...
```
Partition and offset metadata can be retained by downstream processing to support troubleshooting and investigation of individual CDC events.

Kafka retention and consumer configuration determine how long previously published events remain available for replay.

## 8. Spark Processing Layer

Apache Spark processes CDC events and writes datasets to MinIO.

The Spark environment uses:

- Spark 3.5.7
- Hadoop AWS dependencies for S3A access
- Delta Lake extensions
- MinIO as the S3-compatible endpoint

Processing jobs are located under:

```text
spark/jobs/
```

The project contains Bronze ingestion, Silver processing, and Gold aggregation jobs. The execution mode and behavior should be evaluated per job rather than assumed to be identical across the entire project.

## 9. MinIO Storage Layer

MinIO provides S3-compatible object storage.

The logical storage areas are:

```text
s3a://bronze/
s3a://silver/
s3a://gold/
```

Spark connects to MinIO through the Docker network using:

```text
http://minio:9000
```

Credentials are supplied through environment variables and are not intended to be hardcoded in source files.

## 10. Medallion Architecture

The platform separates data into three layers:

```text
Bronze → Silver → Gold
```

### 10.1 Bronze

Bronze stores captured source data in Parquet format.

The Bronze layer is implemented for the following source datasets:

- `customers`
- `products`
- `orders`
- `order_items`
- `payments`
- `inventory`

All six datasets have produced Parquet output in the local MinIO Bronze storage area.

Where implemented, event metadata includes:

- CDC payload
- Operation
- Topic
- Partition
- Offset
- Kafka timestamp
- Event timestamp
- Processing timestamp

A customer CDC test validated the following operation sequence:

```text
c → u → d
```

This confirms the tested insert, update, and delete flow for that record. It does not prove that every edge case across every table has been tested.

### 10.2 Silver

Silver stores processed datasets in Delta Lake format.

The Silver layer is implemented for the following datasets:

- `customers`
- `products`
- `orders`
- `order_items`
- `payments`
- `inventory`

The datasets have produced Delta Lake output in the local MinIO Silver storage area.

The tested customer CDC flow resulted in customer record `4` being removed from the Silver output after the delete event.

Claims about comprehensive deduplication, schema evolution, business rules, or complete data quality coverage should only be added after those behaviors are explicitly implemented and tested.

### 10.3 Gold

Gold contains datasets prepared for analytical use cases.

The current validated Gold dataset is:

```text
order_summary
```

It is stored in Delta Lake under the Gold storage area.

The Gold job completed successfully and produced:

```text
Gold records: 5
```

The Gold output reflects the aggregation logic implemented in:

```text
spark/jobs/gold_order_summary.py
```

## 11. Delta Lake

Delta Lake is used for the Silver and Gold layers.

The Spark session is configured with:

```text
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

Advanced capabilities such as time travel, schema evolution, optimization, and vacuum should not be considered implemented unless they are separately configured and validated.

## 12. Data Lineage

The intended lineage path is:

```text
PostgreSQL table
    → Debezium connector
    → Kafka topic
    → Spark job
    → Bronze
    → Silver
    → Gold
```

Available event metadata can support troubleshooting and lineage investigation.

A complete, independently verified record-level lineage from every Gold record to its original source events has not been established and remains future work.

## 13. Validation Summary

The following record counts were observed during local validation:

| Dataset | Record count |
|---|---:|
| `customers` | 6 |
| `products` | 5 |
| `orders` | 5 |
| `order_items` | 8 |
| `payments` | 5 |
| `inventory` | 5 |
| Gold `order_summary` | 5 |

These counts represent the validated local state at the time of testing and are not fixed production-volume guarantees.

Validation activities included:

- Checking Docker container status and health
- Checking Debezium connector status
- Verifying Bronze and Silver outputs
- Checking record counts
- Testing CDC insert, update, and delete operations
- Running the Gold processing job
- Verifying Spark access to MinIO with environment-based credentials

## 14. Reliability Considerations

### Idempotent Processing

The pipeline is designed to reduce the risk of duplicate effects when processing CDC events.

For the validated Silver processing flow, CDC records are interpreted using the Debezium operation type and the resulting state is written to the corresponding dataset.

The customer CDC flow was tested with an insert, update, and delete sequence. The resulting Silver dataset reflected the delete operation.

This validation demonstrates the expected behavior for the tested scenario, but it does not establish an idempotency guarantee across every processing job or failure scenario.

### Decoupling

Kafka provides a buffer between CDC capture and downstream processing.

Debezium publishes database changes to Kafka topics, while Spark consumes those events independently. This separates the transactional source system from the processing layer and allows the components to operate at different processing rates.

This architecture also provides a clear boundary between event capture and data processing, making individual components easier to troubleshoot and evolve.

### Replayability

Kafka retains CDC events according to its retention configuration. Consumers can use topic, partition, and offset information to identify and process events from a specific position in the stream.

This provides an architectural mechanism for replaying previously published events when they are still available in Kafka.

Automated replay and backfill workflows are not yet implemented as part of the project and require additional testing and operational handling.

### Observability

The pipeline retains operational metadata such as Kafka topic, partition, offset, Kafka timestamp, event timestamp, and processing timestamp in the Bronze layer.

These fields provide context for tracing CDC events through the processing pipeline and investigating processing behavior.

The current implementation provides metadata that can support manual troubleshooting. Dedicated monitoring dashboards, automated alerting, and centralized operational metrics are not yet implemented.

The following areas require additional implementation or testing before the platform can be described as production-ready:

- Automated retry policies
- Dead-letter handling
- Comprehensive data quality checks
- Schema evolution
- Automated replay and backfill workflows
- Monitoring and alerting
- Automated integration tests
- Failure recovery across all pipeline stages
- Broader idempotency testing across processing jobs and failure scenarios

The current validation demonstrates that the tested local pipeline works for the documented scenarios. It does not represent a complete production-readiness assessment.

## 15. Local Deployment

The project should be executed from the project root directory:

```text
ecommerce-data-platform
```

The Docker network is:

```text
ecommerce-data-platform
```

Main services include:

- PostgreSQL
- Kafka
- Debezium
- MinIO
- Spark

Useful commands:

```bash
docker ps
```

```bash
curl -s http://localhost:8083/connectors/postgres-ecommerce-connector/status
```

Example Spark execution:

```bash
docker exec ecommerce-spark sh -c '
/opt/spark/bin/spark-submit   --master "local[*]"   --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog   /opt/spark/jobs/gold_order_summary.py
'
```

The exact S3A and MinIO configuration should match the configuration used by the current Spark image.

## 16. Security Practices

Sensitive values should be managed through environment variables or a local `.env` file.

Recommended practices:

- Do not hardcode database passwords
- Do not hardcode MinIO credentials
- Exclude `.env` from Git
- Keep placeholders in `.env.example`
- Rotate credentials if exposed
- Review repository contents before pushing
- Remove secrets from Git history if they were committed

Credentials that have been exposed should be treated as compromised and rotated.

## 17. Implementation Status

### Implemented and validated

- PostgreSQL source schema
- Debezium PostgreSQL connector
- Kafka CDC topics
- MinIO storage
- Spark processing environment
- Bronze Parquet datasets
- Silver Delta datasets
- Gold `order_summary`
- CDC insert, update, and delete test for a customer record
- Environment-based MinIO credentials

### Implemented but requiring broader validation

- CDC behavior across all tables
- Silver processing behavior beyond tested cases
- Reprocessing and idempotency across all jobs
- Metadata retention and end-to-end lineage
- Failure recovery

### Planned

- Automated data quality checks
- Monitoring and alerting
- Automated replay and backfill workflows
- Schema evolution management
- DuckDB analytical access
- Metabase dashboards
- dbt/Dataform models
- Airflow orchestration
- Broader integration testing
- Production deployment patterns

## 18. Future Direction

The planned direction is:

```text
CDC ingestion
    ↓
Spark processing
    ↓
Bronze / Silver / Gold
    ↓
Data quality validation
    ↓
Orchestration and monitoring
    ↓
Analytical query layer
    ↓
BI dashboards
```

Potential future components include Airflow, dbt/Dataform, DuckDB, Metabase, automated quality checks, and monitoring.

These components remain planned until their implementation and operation are validated.

## 19. Design Principles

1. Separate transactional and analytical workloads.
2. Capture source changes through CDC.
3. Preserve event metadata where possible.
4. Separate raw, processed, and analytical layers.
5. Use environment-based configuration for credentials.
6. Validate implementation claims through actual tests.
7. Distinguish implemented functionality from planned functionality.
8. Avoid presenting a local proof of concept as production-ready without additional evidence.
