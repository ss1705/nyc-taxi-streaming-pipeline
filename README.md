# NYC Taxi Demand Pipeline

A production-style real-time data pipeline built on NYC TLC taxi trip data, demonstrating distributed stream processing from ingestion to interactive dashboard.

**Live demo:** [NYC Taxi Demand Dashboard](https://nyc-taxi-streaming-pipeline-production.up.railway.app)

## Architecture

NYC TLC API → Kafka (KRaft) → PySpark Structured Streaming → DuckDB → dbt → Streamlit


| Layer | Technology | Role |
|---|---|---|
| Ingestion | Apache Kafka 3.7 (KRaft) | Message broker, decouples producer from consumer |
| Producer | Python / Socrata API | Streams trip records into Kafka topic |
| Processing | PySpark 3.5.1 Structured Streaming | Micro-batch transforms, type casting, feature engineering |
| Storage | DuckDB | Local columnar data warehouse |
| Transformation | dbt Core 1.11 | 3-layer SQL models (staging → intermediate → marts) |
| Dashboard | Streamlit + Altair | Interactive demand and fare analytics |

## Pipeline Details

**Producer** (`producer/producer.py`): Pulls NYC TLC trip records from the Socrata open data API and publishes them to a Kafka topic (`taxi-trips`) as JSON-serialized messages, simulating a real-time dispatch event stream.

**Consumer** (`spark/consumer.py`): PySpark Structured Streaming job reads from Kafka every 10 seconds, applies schema validation, filters invalid records (zero coordinates, zero distance), casts all fields to correct types, and engineers three features:
- `trip_duration_min` — derived from dropoff minus pickup timestamp
- `pickup_hour` — hour of day (0–23) for demand pattern analysis
- `pickup_dayofweek` — day of week for weekly seasonality

Processed batches are written to DuckDB via `foreachBatch` using a Pandas bridge.

**dbt models** (`dbt_taxi/models/`):
- `stg_taxi_trips` — cleans and renames raw fields, casts coordinates from VARCHAR to DOUBLE
- `int_trips_enriched` — adds time-of-day buckets, tip rate, fare per mile, weekend flag
- `mart_hourly_demand` — aggregates trip count, revenue, and duration metrics by hour and day of week

## Dashboard

Three Altair charts served by Streamlit:
- **Trip Demand by Hour of Day** — reveals the natural demand rhythm with 3–5am trough and evening peak
- **Trip Demand by Time of Day** — bucketed into overnight / morning rush / midday / evening rush / night
- **Average Fare by Time of Day** — overnight fares highest, consistent with longer airport and cross-borough trips

## Key Design Decisions

**Kafka KRaft mode** — eliminated Zookeeper dependency, reducing the local stack to a single container. Modern standard since Kafka 3.3.

**DuckDB over Redshift locally** — identical SQL semantics to Redshift/Snowflake, zero infrastructure cost. dbt models are warehouse-agnostic and would run unchanged against a cloud warehouse by swapping the connection profile.

**PySpark 3.5.1 pinned** — PySpark 4.x released during development but the Kafka connector ecosystem hadn't stabilized. Pinned to 3.5.1 for connector compatibility, matching production patterns where stability is preferred over bleeding-edge versions.

**foreachBatch sink pattern** — standard approach for writing Spark Structured Streaming output to systems without a native Spark connector. Converts each micro-batch to Pandas and uses DuckDB's native DataFrame query API.

## Setup

```bash
# Start Kafka
docker compose up -d

# Activate environment
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Stream data
python producer/producer.py   # Terminal 1
python spark/consumer.py      # Terminal 2

# Build dbt models
cd dbt_taxi && dbt run && dbt test

# Launch dashboard
cd .. && streamlit run dashboard.py
```

## Stack

Python 3.11 · Apache Kafka 3.7 (KRaft) · PySpark 3.5.1 · DuckDB 1.5 · dbt Core 1.11 · Streamlit 1.57 · Altair 6.1 · Docker