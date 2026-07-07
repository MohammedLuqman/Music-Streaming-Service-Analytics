# 🎵 Music Analytics Streaming Service

> **An end-to-end enterprise-grade Data Engineering project that simulates a modern music streaming analytics platform using Apache Kafka, Apache Spark, Apache Airflow, MinIO, ClickHouse, Grafana, and Docker Compose.**


![Kafka](https://img.shields.io/badge/Apache-Kafka-black?logo=apachekafka)
![Spark](https://img.shields.io/badge/Apache-Spark-orange?logo=apachespark)
![Airflow](https://img.shields.io/badge/Apache-Airflow-blue?logo=apacheairflow)
![ClickHouse](https://img.shields.io/badge/ClickHouse-yellow?logo=clickhouse)
![Grafana](https://img.shields.io/badge/Grafana-orange?logo=grafana)
![MinIO](https://img.shields.io/badge/MinIO-red?logo=minio)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)

---

# 📖 Table of Contents

- Overview
- Business Problem
- Solution Overview
- Architecture
- Data Pipeline
- Technology Stack
- Project Structure
- Installation
- Configuration
- Running the Project
- Airflow Workflows
- Streaming Pipeline
- Analytics Layer
- Dashboard
- Future Improvements
- License

---

# 📌 Overview

Modern music streaming platforms generate millions of user interactions every day, including song plays, likes, skips, searches, playlists, and listening sessions.

Traditional databases are not designed to efficiently process high-volume streaming events while simultaneously supporting analytical workloads.

This project demonstrates how a modern **Lakehouse Architecture** can ingest, process, transform, and analyze streaming music events using scalable open-source technologies.

The pipeline combines both **real-time** and **batch processing** to build a complete analytics platform capable of generating business insights from streaming events.

---

# 🎯 Business Problem

Music streaming companies need to answer questions such as:

- What songs are currently trending?
- Which artists are growing in popularity?
- Which genres are increasing or decreasing over time?
- How long do users listen before skipping songs?
- What listening patterns exist across different users?
- Which recommendations generate the highest engagement?

Answering these questions requires processing millions of streaming events efficiently while maintaining a scalable analytics platform.

---

# 💡 Solution

This project implements an end-to-end Data Lakehouse that performs:

- Real-time event ingestion using Apache Kafka
- Distributed stream processing using Apache Spark
- Bronze / Silver / Gold Medallion Architecture
- Object storage using MinIO
- Analytical storage using ClickHouse
- Workflow orchestration using Apache Airflow
- Interactive dashboards using Grafana

The architecture separates ingestion, processing, storage, orchestration, and analytics into independent components, making the platform modular and scalable.

---

# 🏗 Architecture

```
                 Music Streaming Events
                           │
                           ▼
                 Apache Kafka (Streaming)
                           │
                           ▼
                Spark Structured Streaming
                           │
                           ▼
               Bronze Layer (Raw JSON, CSV)
                           │
                           ▼
                  Spark Batch Cleaning
                           │
                           ▼
             Silver Layer (Clean Parquet)
                           │
                           ▼
                Spark Aggregation Jobs
                           │
                           ▼
             Gold Layer (Business Metrics)
                    │                 │
                    ▼                 ▼
              ClickHouse         Grafana
                    │
                    ▼
              Business Analytics

              Apache Airflow
        (Pipeline Orchestration)
```

---

# 🚀 Key Features

✔ Real-time data ingestion

✔ Distributed Spark processing

✔ Medallion Architecture

✔ Automated Airflow workflows

✔ High-performance ClickHouse analytics

✔ Interactive Grafana dashboards

✔ Docker Compose deployment

✔ Modular project architecture

✔ Enterprise-ready design

---

# 🛠 Technology Stack

| Layer | Technology |
|---------|------------|
| Streaming | Apache Kafka |
| Processing | Apache Spark |
| Workflow Orchestration | Apache Airflow |
| Object Storage | MinIO |
| Analytics Database | ClickHouse |
| Visualization | Grafana |
| Containerization | Docker Compose |

---

# ⚙ End-to-End Data Flow

The pipeline processes streaming events through multiple stages.

## Step 1 — Event Generation

Music listening events are continuously generated from the simulated music streaming application.

Each event contains information such as:

- User ID
- Song ID
- Artist
- Genre
- Listening duration
- Timestamp
- Device
- Country

↓

## Step 2 — Kafka Streaming

Events are published into Kafka topics where they become available for downstream consumers.

↓

## Step 3 — Spark Structured Streaming

Spark continuously consumes Kafka topics and performs:

- Schema validation
- Timestamp conversion
- Data enrichment
- Invalid record filtering

↓

## Step 4 — Bronze Layer

Raw events are stored inside MinIO as immutable Parquet files.

↓

## Step 5 — Silver Layer

Spark cleans, standardizes, and enriches the data before writing optimized Parquet datasets.

↓

## Step 6 — Gold Layer

Business aggregations are generated including:

- Top Songs
- Top Artists
- Listening Time
- Genre Statistics
- User Engagement Metrics

↓

## Step 7 — ClickHouse

Aggregated datasets are loaded into ClickHouse for ultra-fast analytical queries.

↓

## Step 8 — Grafana

Grafana connects directly to ClickHouse to visualize business KPIs and trends in real time.

---

---

# 📂 Project Structure

```text
music-analytics-data-lakehouse/
│
├── dags/                         # Apache Airflow DAGs
│   ├── kafka_stream_ingestion.py
│   ├── lastfm_to_minio.py
│   ├── music_recommendations_ai.py
│   └── ...
│
├── clickhouse/
│   ├── music_events.sql
│   ├── user_recommendations.sql
│
├── grafana/
│   ├── dashboards/

├── notebooks/ scripts/ spark_processor.ipynb
│
├── jars/ Requirements.txt
│
├── docker-compose.yml
│
├── logs/
│
└── README.md
```

# ⚙ Infrastructure Components

The project is deployed entirely using **Docker Compose**, allowing every component to communicate over a shared network while remaining independently scalable.

| Service | Description |
|----------|-------------|
| Apache Kafka | Streaming platform responsible for ingesting music events. |
| Apache Spark | Executes both streaming and batch ETL workloads. |
| Apache Airflow | Schedules and orchestrates all data pipelines. |
| MinIO | Stores Bronze, Silver, and Gold datasets in Parquet format. |
| ClickHouse | Provides ultra-fast analytical queries over aggregated datasets. |
| Grafana | Visualizes business metrics and operational dashboards. |

---

# 🚀 Getting Started

## Prerequisites

Before running the project, ensure the following software is installed:

- Docker Desktop
- Docker Compose


# 📥 Clone Repository

```bash
git clone https://github.com/<your-username>/Music-Streaming-Service-Analytics.git
```


# ▶ Start the Infrastructure

Launch every service using Docker Compose.

```bash
docker compose up -d
```

Verify that all containers are healthy.

```bash
docker compose ps
```

---

# 🌐 Available Services

| Service | Default Port |
|----------|--------------|
| Kafka UI | 9092 |
| pyspark-notebook | 8888 |
| Airflow | 8081 |
| MinIO | 9000 |
| ClickHouse HTTP | 8123 |
| Grafana | 3000 |

---

# 📊 Airflow Workflows

Apache Airflow orchestrates the complete analytics pipeline.

Each DAG is responsible for a specific stage of the platform.

Example responsibilities include:

- Starting Kafka ingestion
- Loading external music datasets
- Running scheduled ETL jobs
- Triggering recommendation workflows
- Performing health checks

This orchestration layer guarantees that every processing stage executes in the correct order while providing monitoring, retry mechanisms, scheduling, and execution history.

---

# 🎧 Streaming Pipeline

The streaming pipeline is responsible for continuously processing music listening events.

## Event Producer

The producer simulates real-time music activity by publishing events into Kafka topics.

Each event may include:

- User ID
- Song ID
- Artist
- Album
- Genre
- Device
- Country
- Timestamp
- Listening Duration

---

## Kafka

Kafka serves as the central event streaming platform.

Benefits include:

- High throughput
- Fault tolerance
- Horizontal scalability
- Decoupled architecture

Instead of sending data directly to Spark, producers publish events into Kafka where consumers can process them independently.

---

## Spark Structured Streaming

Spark continuously consumes Kafka topics.

Streaming transformations include:

- Parsing JSON messages
- Schema validation
- Null filtering
- Timestamp normalization
- Data enrichment
- Error handling

After processing, validated events are written into the Bronze layer.

---

# 🥉 Bronze Layer

The Bronze layer stores raw immutable data exactly as it was received.

Characteristics:

- Raw events
- Append-only
- Historical archive
- Parquet format
- Stored inside MinIO

No business transformations are applied at this stage.

---

# 🥈 Silver Layer

The Silver layer contains cleaned and standardized datasets.

Transformations include:

- Removing invalid records
- Handling missing values
- Standardizing data types
- Removing duplicates
- Applying business rules

These datasets become the trusted source for analytics.

---

# 🥇 Gold Layer

The Gold layer contains business-ready datasets optimized for reporting.

Typical aggregations include:

- Most streamed songs
- Top artists
- Listening trends
- Genre popularity
- Daily active users
- User engagement statistics

These datasets are optimized for BI workloads.

---

# ⚡ ClickHouse Analytics

Gold datasets are loaded into ClickHouse.

ClickHouse provides:

- Columnar storage
- High compression
- Extremely fast analytical queries
- Real-time dashboard support

Example analytical questions include:

- Top songs today
- Most active listeners
- Trending genres
- Average listening duration
- Daily streaming volume

---

# 📈 Analytics Dashboard

The project includes Grafana dashboards that provide real-time insights into music streaming activity.

Example visualizations include:

- 🎵 Top Streamed Songs
- 🎤 Most Popular Artists
- 🎼 Genre Distribution
- 👤 User Listening Activity
- 🌍 Listening Activity by Country
- ⏱ Average Listening Duration
- 📅 Daily Streaming Volume
- 📊 Platform Growth Trends

Grafana connects directly to ClickHouse, enabling low-latency visualization of aggregated business metrics.

---

# 🤖 Recommendation Pipeline

The project includes an AI recommendation workflow designed to generate personalized music recommendations.

---

# 📊 Data Quality

Data quality is maintained throughout the pipeline using Spark transformations.

Validation steps include:

- Schema validation
- Missing value handling
- Duplicate removal
- Timestamp normalization
- Invalid record filtering
- Data type standardization

These checks ensure downstream analytical datasets remain reliable and consistent.

---

## Grafana cannot connect to ClickHouse

Verify that:

- ClickHouse container is healthy.
- Network connectivity exists between containers.
- Grafana datasource configuration is correct.

---

# 📄 License

This project is intended for educational and portfolio purposes.

Feel free to fork, learn from, and extend the project.

---

# 👨‍💻 Author

**Mohammed Luqman**

Data Engineering | Big Data | Apache Spark | Apache Kafka | Airflow | ClickHouse | Docker | Python

If you found this project useful, consider giving it a ⭐ on GitHub.

---

# ⭐ Support

If this project helped you learn something new, don't forget to star the repository.

Your support helps improve future open-source Data Engineering projects.
