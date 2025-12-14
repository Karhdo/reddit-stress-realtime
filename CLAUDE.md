# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Reddit Stress Streaming** is a real-time data pipeline that collects Reddit posts, processes them through a Medallion Architecture (Bronze → Silver → Gold), and applies deep learning-based stress classification using Apache Spark Structured Streaming, Delta Lake, and HuggingFace transformers.

## Tech Stack

- **Stream Processing**: Apache Spark 3.5.1 (Structured Streaming)
- **Storage**: Delta Lake on MinIO (S3-compatible object storage)
- **Messaging**: Apache Kafka 3.7.1 (KRaft mode, no ZooKeeper)
- **ML Inference**: HuggingFace Transformers (DistilBERT fine-tuned model)
- **Database**: PostgreSQL 16 with Flyway migrations
- **Visualization**: Streamlit dashboard
- **Containerization**: Docker Compose

## Common Development Commands

### Environment Setup
```bash
# Copy environment template and fill in Reddit API credentials
cp .env.example .env

# Start infrastructure (Kafka, MinIO, Postgres, Streamlit)
docker compose up -d

# Create Kafka topics
make topics

# Run database migrations
make migrate
```

### Running the Pipeline

The pipeline consists of three independent streaming layers that should run in separate terminals:

```bash
# Terminal 1: Start Reddit → Kafka producer
make producer

# Terminal 2: Bronze layer (Kafka → Delta Lake)
make stream_bronze

# Terminal 3: Silver layer (Clean & Deduplicate)
make stream_silver

# Terminal 4: Gold layer (ML Inference & Classification)
make stream_gold
```

Each layer is an independent Spark Structured Streaming job with its own checkpoint directory.

### Development Tools
```bash
# Run tests
make test
# or
pytest -q

# Lint code
make lint
# or
ruff check .

# Format code
make format
# or
black .

# Train ML model
make train
```

### Database Migrations
```bash
# Apply pending migrations
make migrate

# View migration status
make migrate_info

# Repair migration checksums (use sparingly)
make migrate_repair
```

## Architecture

### Medallion Architecture (Bronze → Silver → Gold)

The pipeline follows the Medallion Architecture pattern with three layers:

**Bronze Layer** (`src/streaming/stream_bronze.py`)
- Reads from Kafka topic `reddit_posts`
- Parses JSON payload using `reddit_post_schema`
- Adds temporal columns: `event_time`, `dt`, `ingest_ts`
- Writes raw data to Delta Lake at `s3a://datalake/bronze/reddit_posts`
- Checkpoint: `s3a://datalake/_checkpoints/bronze`

**Silver Layer** (`src/streaming/stream_silver.py`)
- Reads from Bronze Delta table
- Performs cleaning: trims text fields, fills numeric nulls, drops empty posts
- Deduplicates by `post_id` (keeps latest by `ingest_ts`, then `event_time`)
- Uses Delta MERGE operation for incremental upserts
- Writes to `s3a://datalake/silver/reddit_posts`
- Checkpoint: `s3a://datalake/_checkpoints/silver`

**Gold Layer** (`src/streaming/stream_gold.py`)
- Reads from Silver Delta table
- Builds combined text: `text = title + selftext`
- Computes `interaction_rate = (upvotes + 0.5*num_comments) / age_hours`
- Applies stress classification using Pandas UDF with HuggingFace model
- Outputs `score_stress` (probability) and `label_stress` (binary threshold)
- Writes to either Delta Lake or PostgreSQL (configurable via `sink.gold_backend`)
- Checkpoint: `s3a://datalake/_checkpoints/gold`

### Configuration System

Configuration is centralized in `configs/config.yaml` with environment variable expansion:

- **Pattern**: `${ENV_VAR:-default_value}`
- **Config types**: Defined in `src/common/config_types/`
- **Loading**: `load_config()` in `src/common/config.py` loads YAML and expands env vars
- **Main config object**: `Config` dataclass with typed sub-configs (KafkaCfg, MinioCfg, etc.)

Key configuration sections:
- `kafka`: Bootstrap servers, topics, starting offsets
- `minio`: S3 endpoint, credentials, bucket name
- `sink`: Data layer prefixes, backend selection (delta/postgres)
- `spark`: Master URL, shuffle partitions, S3A/Delta configs
- `model`: ML model URI, batch size, threshold, label index
- `postgres`: JDBC connection details
- `reddit`: API credentials, subreddits, polling interval

### Spark Application Entry Point

`src/streaming/app.py` is the unified entry point for all three layers:

1. Loads config from `configs/config.yaml`
2. Builds SparkSession with S3A and Delta Lake configurations
3. Routes to correct layer based on `APP_MODE` environment variable:
   - `APP_MODE=bronze` → `stream_bronze()`
   - `APP_MODE=silver` → `stream_silver()`
   - `APP_MODE=gold` → `stream_gold()`
4. Starts the streaming query and awaits termination

### Machine Learning Inference

**Model Loading** (`src/model/infer.py`)
- Models are stored as `.zip` files containing HuggingFace checkpoint
- Supports both local and S3 URIs (`s3a://artifacts/models/.../model.zip`)
- Content-addressed caching: extracts to `/tmp/model_cache/{sha1_hash}/`
- Singleton pattern: one `HFZipService` instance per executor per (model_uri, pos_idx, max_len)

**Inference Pattern**
- Uses Pandas UDF for distributed batch inference
- Iterator-based UDF processes batches of text across partitions
- Each executor lazily loads the model on first invocation
- Batch size configurable via `model.classifier_batch_size` (default: 16)
- Returns probability of positive class (stress) at index `classifier_label_index`

**Preflight Check**
- Gold layer calls `_preflight_model()` on driver before starting stream
- Fails fast if model URI is invalid or unreachable
- Validates model loads correctly and can perform inference

### Data Schemas

Schemas are defined in `src/common/schema.py`:

- **RedditPost**: Dataclass for Kafka messages (post_id, author, subreddit, title, selftext, etc.)
- **reddit_post_schema**: Spark StructType for parsing Kafka JSON
- **SILVER_COLS**: Canonical column list for Silver layer
- **GOLD_COLS**: Canonical column list for Gold layer with ML features
- **get_silver_schema()**, **get_gold_schema()**: Return StructTypes for table initialization

### Checkpointing Strategy

Each streaming layer maintains its own checkpoint directory:
- Location: `s3a://datalake/_checkpoints/{layer_name}/`
- Contains offsets, state, and metadata for exactly-once processing
- To reset a layer: delete its checkpoint directory (WARNING: may cause reprocessing)

### Spark Packages

The Makefile configures these Spark packages (via `--packages`):
- `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` - Kafka connector
- `io.delta:delta-spark_2.12:3.2.0` - Delta Lake
- `org.apache.hadoop:hadoop-aws:3.3.4` - S3A filesystem
- `com.amazonaws:aws-java-sdk-bundle:1.12.767` - AWS SDK
- `org.postgresql:postgresql:42.7.4` - PostgreSQL JDBC driver

### MinIO (S3-Compatible Storage)

- **Endpoint**: http://localhost:9000 (API), http://localhost:9001 (Console)
- **Credentials**: minio/minio123 (default)
- **Bucket**: `datalake` (auto-created by minio-mc service)
- **Path Style Access**: Required for MinIO (configured in Spark)
- **Delta Log Store**: Uses `S3SingleDriverLogStore` for single-writer safety

### Reddit Producer

`src/producer/reddit_producer.py`:
- Uses PRAW library to fetch posts from configured subreddits
- Deduplicates posts using in-memory set (bounded to 10,000 IDs)
- Sends JSON messages to Kafka topic `reddit_posts`
- Polling interval configurable via `reddit.poll_interval` (default: 30s)
- Supports both keyword search and new posts from subreddit

Default subreddits: `['Professors', 'PhD', 'GradSchool', 'csMajors', 'EngineeringStudents']`

## Important Implementation Details

### Delta MERGE for Silver Layer

The Silver layer uses Delta's MERGE operation for upserts:
```python
merge(src, "t.post_id = s.post_id")
  .whenMatchedUpdate(set=set_map)
  .whenNotMatchedInsert(values=set_map)
  .execute()
```
This ensures idempotent processing: late-arriving duplicates update existing rows.

### Watermarking

Bronze stream sets a 15-minute watermark on `event_time` to handle late data:
```python
.withWatermark("event_time", "15 minutes")
```

### foreachBatch Pattern

Silver and Gold layers use `foreachBatch` for batch-oriented processing:
- Allows Delta MERGE operations (not available in append mode)
- Enables per-batch logging and metrics
- Provides transactional writes with exactly-once semantics

### S3A Configuration

Critical S3A settings for MinIO compatibility:
- `spark.hadoop.fs.s3a.endpoint` - MinIO endpoint URL
- `spark.hadoop.fs.s3a.path.style.access=true` - Use path-style URLs (not virtual-hosted)
- `spark.hadoop.fs.s3a.connection.ssl.enabled=false` - Disable SSL for http://
- `spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem` - S3A filesystem
- `spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore` - Delta log store

### Zipping Python Code

The Makefile zips `src/` into `src.zip` and passes it via `--py-files`:
```bash
make zip  # Creates src.zip from src/ directory
```
This ensures all modules in `src/` are available to Spark executors.

### PostgreSQL Sink Option

Gold layer supports writing to PostgreSQL instead of Delta:
- Set `sink.gold_backend: 'postgres'` in config.yaml
- Target table: `public.gold_reddit_posts`
- Schema created via Flyway migration: `migrations/V1__init_gold_schema.sql`
- Uses JDBC driver: `org.postgresql.Driver`

## Testing

Test files in `tests/`:
- `tests/test_infer.py` - ML inference service tests
- Run with: `pytest -q`

## File Structure Highlights

```
configs/config.yaml           # Central configuration (YAML + env vars)
src/streaming/app.py          # Unified Spark entry point
src/streaming/stream_*.py     # Layer-specific streaming logic
src/producer/reddit_producer.py  # Kafka producer (Reddit → Kafka)
src/model/infer.py            # ML inference service (singleton per executor)
src/model/train.py            # Model training script
src/common/config.py          # Config loader and dataclass
src/common/schema.py          # Spark schemas and dataclasses
src/common/logging_utils.py   # Loguru logger setup
migrations/                   # Flyway SQL migrations
apps/streamlit/               # Streamlit dashboard app
```

## Environment Variables

Required in `.env`:
- `REDDIT_CLIENT_ID` - Reddit API client ID
- `REDDIT_CLIENT_SECRET` - Reddit API secret
- `REDDIT_USER_AGENT` - User agent string

Optional (have defaults):
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka bootstrap (default: localhost:19092)
- `S3_ENDPOINT_URL` - MinIO endpoint (default: http://localhost:9000)
- `AWS_ACCESS_KEY_ID` - MinIO access key (default: minio)
- `AWS_SECRET_ACCESS_KEY` - MinIO secret key (default: minio123)
- `MINIO_BUCKET` - MinIO bucket name (default: datalake)
- `APP_MODE` - Streaming layer to run: bronze|silver|gold
- `CHECKPOINT_BASE` - Base path for checkpoints

## Debugging Tips

**View Kafka messages:**
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic reddit_posts --from-beginning
```

**Access MinIO Console:**
Navigate to http://localhost:9001 (login: minio/minio123)

**View Spark UI:**
When streaming jobs are running, access Spark UI at http://localhost:4040

**Check Delta table history:**
```python
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "s3a://datalake/silver/reddit_posts")
dt.history().show()
```

**Reset checkpoint (use with caution):**
```bash
# WARNING: This will cause reprocessing from beginning
aws s3 rm --recursive --endpoint-url=http://localhost:9000 \
  s3://datalake/_checkpoints/silver/
```
