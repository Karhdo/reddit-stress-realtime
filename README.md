## 🚀 Overview

**Reddit Stress Streaming** is a real-time data pipeline that continuously collects Reddit posts, processes them through a **Medallion Architecture** (Bronze → Silver → Gold), and applies **deep learning-based stress classification** for analytics and visualization.

It enables near real-time monitoring of mental health–related trends using scalable, fault-tolerant streaming and AI inference on distributed systems.

## 🧱 Architecture

Reddit API ─▶ Kafka ─▶ Bronze ─▶ Silver ─▶ Gold ─▶ Analytics (Updating Diagram)

| Layer         | Purpose                                     | Technology                              |
| ------------- | ------------------------------------------- | --------------------------------------- |
| **Bronze**    | Raw ingestion from Kafka                    | Spark Structured Streaming + Delta Lake |
| **Silver**    | Clean & deduplicate                         | Spark SQL + Delta MERGE                 |
| **Gold**      | Stress classification + feature engineering | Spark + Pandas UDF + HuggingFace        |
| **Analytics** | Interactive data exploration                | PySpark + Jupyter Notebook              |

## Project Structure

```
reddit-stress-streaming/
├── Makefile
├── README.md
├── airflow
│   └── dags
│       └── retrain_model_dag.py
├── configs
│   └── config.yaml
├── docker-compose.yml
├── requirements.txt
├── spark-warehouse
├── src
│   ├── common
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── config_types
│   │   │   ├── __init__.py
│   │   │   ├── app_config.py
│   │   │   ├── kafka_config.py
│   │   │   ├── minio_config.py
│   │   │   ├── model_config.py
│   │   │   ├── reddit_config.py
│   │   │   ├── sink_config.py
│   │   │   └── spark_config.py
│   │   ├── logging_utils.py
│   │   └── schema.py
│   ├── model
│   │   ├── __init__.py
│   │   ├── artifacts
│   │   ├── infer.py
│   │   └── train.py
│   ├── producer
│   │   ├── __init__.py
│   │   └── reddit_producer.py
│   └── streaming
│       ├── __init__.py
│       ├── app.py
│       ├── app_backup.py
│       ├── stream_bronze.py
│       ├── stream_gold.py
│       └── stream_silver.py
└── tests
    ├── __init__.py
    └── test_infer.py
```

## 🛠️ Tech Stack

| Component         | Technology                                  |
| ----------------- | ------------------------------------------- |
| Ingestion         | Reddit API (PRAW) → Kafka                   |
| Stream Processing | Apache Spark Structured Streaming 3.5.1     |
| Storage           | Delta Lake on MinIO (S3A connector)         |
| Model Inference   | Hugging Face Transformers (DistilBERT)      |
| Serving           | InferenceService Singleton per Spark Worker |
| Analytics         | Spark SQL + Jupyter Notebooks               |
| Containerization  | Docker Compose                              |
| Orchestration     | Makefile (one-command runs)                 |

## 🧩 Data Flow

### 🟤 Bronze Layer — Raw Data

- Source: Kafka topic `reddit_posts`
- Sink: `s3a://datalake/bronze/reddit_posts`
- Adds `event_time`, `dt`, `ingest_ts`
- File: `src/streaming/stream_bronze.py`

### ⚪ Silver Layer — Clean & Deduplicate

- Source: Bronze Delta
- Deduplicate by `post_id` (latest `ingest_ts`)
- Fill missing fields, remove empty posts
- Delta `MERGE INTO` for incremental upserts
- File: `src/streaming/stream_silver.py`

### 🟡 Gold Layer — Classified & Enriched

- Source: Silver Delta
- Builds `text = title + selftext`
- Computes `interaction_rate`
- Classifies `score_stress` using DL model
- Writes to partitioned Delta table (`dt`)
- File: `src/streaming/stream_gold.py`

## 🧠 Machine Learning Inference & Jupyter Notebooks

File: `src/model/infer.py` & Folder: `notebooks/`

Implements a **singleton InferenceService** per Spark executor:

- Lazy-loaded Hugging Face models
- Batched inference via **Pandas UDFs**
- Supports both `embed()` and `classify()` methods
- Optimized for local CPU or GPU usage

## 📚 References

- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/streaming/index.html)
- [Delta Lake](https://docs.delta.io/)
- [Hugging Face Transformers](https://huggingface.co/docs/transformers/index)
- [MinIO Documentation](https://www.min.io/)
- [Medallion Architecture (Databricks)](https://www.databricks.com/glossary/medallion-architecture)

## 🧑‍💻 Author

Reddit Stress Streaming Developed by **[Karhdo](https://github.com/karhdo) & [Ziduck](https://github.com/ziduck)** - October 2025
