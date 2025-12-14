# Reddit Stress Streaming

**Real-time mental health trend monitoring using Apache Spark, Delta Lake, and Deep Learning**

## 🚀 Overview

**Reddit Stress Streaming** is a production-ready, real-time data pipeline that continuously collects Reddit posts from mental health-related subreddits, processes them through a **Medallion Architecture** (Bronze → Silver → Gold), and applies **deep learning-based stress classification** for analytics and visualization.

The system enables near real-time monitoring of mental health trends using scalable, fault-tolerant streaming with exactly-once semantics and distributed AI inference.

### Key Features

- **Real-time ingestion**: Continuous data collection from Reddit API via PRAW
- **Medallion Architecture**: Progressive data refinement (Bronze → Silver → Gold)
- **Exactly-once processing**: Kafka + Spark Structured Streaming with checkpointing
- **Distributed ML inference**: HuggingFace DistilBERT with Pandas UDFs
- **Scalable storage**: Delta Lake on S3-compatible MinIO
- **Interactive analytics**: Streamlit dashboard with real-time updates
- **Production-ready**: Docker Compose orchestration, database migrations, monitoring

## 🧱 Architecture

```
Reddit API ─▶ Kafka ─▶ Bronze ─▶ Silver ─▶ Gold ─▶ PostgreSQL/Delta ─▶ Streamlit Dashboard
                        │         │         │
                        └─────────┴─────────┴────── Delta Lake (MinIO S3)
```

| Layer         | Purpose                                     | Technology                              |
| ------------- | ------------------------------------------- | --------------------------------------- |
| **Ingestion** | Fetch posts from Reddit subreddits          | PRAW + Kafka Producer                   |
| **Bronze**    | Raw ingestion from Kafka                    | Spark Structured Streaming + Delta Lake |
| **Silver**    | Clean & deduplicate                         | Spark SQL + Delta MERGE                 |
| **Gold**      | Stress classification + feature engineering | Spark + Pandas UDF + HuggingFace        |
| **Analytics** | Interactive data exploration & visualization| Streamlit + PostgreSQL                  |

## 📁 Project Structure

```
stressvn-realtime/
├── airflow/                          # Airflow DAGs (orchestration)
│   └── dags/
│       └── retrain_model_dag.py      # Model retraining workflow
│
├── apps/                             # Application interfaces
│   └── streamlit/
│       └── app_streamlit.py          # Real-time analytics dashboard
│
├── configs/                          # Configuration files
│   └── config.yaml                   # Central YAML config (env var support)
│
├── migrations/                       # Flyway database migrations
│   └── V1__init_gold_schema.sql      # Initial Gold table schema
│
├── src/                              # Main source code
│   ├── common/                       # Shared utilities
│   │   ├── config_types/             # Typed configuration classes
│   │   │   ├── app_config.py         # Application settings
│   │   │   ├── kafka_config.py       # Kafka connection config
│   │   │   ├── minio_config.py       # MinIO/S3 config
│   │   │   ├── model_config.py       # ML model settings
│   │   │   ├── postgres_config.py    # PostgreSQL config
│   │   │   ├── reddit_config.py      # Reddit API config
│   │   │   ├── sink_config.py        # Data sink settings
│   │   │   └── spark_config.py       # Spark session config
│   │   ├── config.py                 # Config loader (YAML + env vars)
│   │   ├── logging_utils.py          # Loguru logger setup
│   │   └── schema.py                 # Spark schemas & data classes
│   │
│   ├── model/                        # Machine learning
│   │   └── infer.py                  # ML inference service (singleton)
│   │
│   ├── producer/                     # Data ingestion
│   │   └── reddit_producer.py        # Reddit → Kafka producer
│   │
│   └── streaming/                    # Spark streaming layers
│       ├── app.py                    # Unified entry point (Bronze/Silver/Gold)
│       ├── stream_bronze.py          # Kafka → Delta (raw ingestion)
│       ├── stream_silver.py          # Clean & deduplicate
│       └── stream_gold.py            # ML inference & classification
│
├── tests/                            # Unit tests
│   └── test_infer.py                 # ML inference tests
│
├── .env.example                      # Environment variable template
├── CLAUDE.md                         # Claude Code guidance
├── Dockerfile                        # Container image (Streamlit app)
├── Makefile                          # Development commands
├── README.md                         # Project documentation
├── docker-compose.yml                # Infrastructure orchestration
└── requirements.txt                  # Python dependencies
```

### Key Directories

| Directory | Purpose |
|-----------|---------|
| `airflow/` | Contains Airflow DAGs for model retraining and batch jobs |
| `apps/streamlit/` | Streamlit dashboard for real-time analytics and visualization |
| `configs/` | YAML configuration with environment variable expansion |
| `migrations/` | Flyway SQL migrations for PostgreSQL schema management |
| `src/common/` | Shared utilities: config loader, schemas, logging |
| `src/model/` | ML inference service with HuggingFace DistilBERT |
| `src/producer/` | Reddit API → Kafka data ingestion |
| `src/streaming/` | Spark Structured Streaming (Bronze/Silver/Gold layers) |
| `tests/` | Unit tests for core functionality |

### Key Files

| File | Purpose |
|------|---------|
| `src/streaming/app.py` | Unified Spark entry point (routes to Bronze/Silver/Gold based on `APP_MODE`) |
| `src/common/config.py` | Loads `configs/config.yaml` and expands environment variables |
| `src/common/schema.py` | Defines Spark schemas and Reddit post data classes |
| `src/model/infer.py` | HuggingFace model inference with singleton pattern per executor |
| `Makefile` | Development commands: `make producer`, `make stream_bronze`, etc. |
| `docker-compose.yml` | Orchestrates Kafka, MinIO, PostgreSQL, Streamlit services |
| `.env.example` | Template for required environment variables (Reddit API creds) |

## 🛠️ Tech Stack

| Component         | Technology                                  |
| ----------------- | ------------------------------------------- |
| Ingestion         | Reddit API (PRAW) → Kafka 3.7.1             |
| Stream Processing | Apache Spark Structured Streaming 3.5.1     |
| Storage           | Delta Lake 3.2.0 on MinIO (S3A connector)   |
| Model Inference   | HuggingFace Transformers (DistilBERT)       |
| Database          | PostgreSQL 16 + Flyway migrations           |
| Analytics         | Streamlit + SQLAlchemy                      |
| Containerization  | Docker Compose                              |
| Orchestration     | Makefile (one-command runs)                 |

## 📋 Prerequisites

- **Docker** and **Docker Compose** (for infrastructure)
- **Python 3.11+** (for local development)
- **Java 17** (for Spark - included in Dockerfile)
- **Reddit API credentials** (free from [Reddit Apps](https://www.reddit.com/prefs/apps))
- **8GB+ RAM** recommended for running all services

## 🚀 Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/karhdo/stressvn-realtime.git
cd stressvn-realtime
```

### 2. Set up environment variables

```bash
# Copy the environment template
cp .env.example .env

# Edit .env and add your Reddit API credentials
# Get credentials from: https://www.reddit.com/prefs/apps
nano .env  # or use your preferred editor
```

Required variables in `.env`:
```env
REDDIT_CLIENT_ID=your_client_id_here
REDDIT_CLIENT_SECRET=your_client_secret_here
REDDIT_USER_AGENT=stress-stream/0.1

# Optional - these have defaults
KAFKA_BOOTSTRAP_SERVERS=localhost:19092
```

### 3. Start infrastructure services

```bash
# Start Kafka, MinIO, PostgreSQL, and Streamlit
docker compose up -d

# Wait for services to be healthy (30-60 seconds)
docker compose ps

# Create Kafka topics
make topics

# Run database migrations
make migrate
```

### 4. Install Python dependencies (for local development)

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 5. Run the data pipeline

Open **4 separate terminals** and run each layer:

**Terminal 1: Reddit Producer** (fetches posts and sends to Kafka)
```bash
source .venv/bin/activate
make producer
```

**Terminal 2: Bronze Layer** (Kafka → Delta Lake)
```bash
source .venv/bin/activate
make stream_bronze
```

**Terminal 3: Silver Layer** (Clean & Deduplicate)
```bash
source .venv/bin/activate
make stream_silver
```

**Terminal 4: Gold Layer** (ML Inference & Classification)
```bash
source .venv/bin/activate
make stream_gold
```

### 6. Access the Streamlit dashboard

Open your browser and navigate to:
- **Streamlit Dashboard**: http://localhost:8501
- **MinIO Console**: http://localhost:9001 (login: minio/minio123)

## 📊 Service Endpoints

| Service           | URL                      | Credentials       |
| ----------------- | ------------------------ | ----------------- |
| Streamlit         | http://localhost:8501    | -                 |
| MinIO Console     | http://localhost:9001    | minio/minio123    |
| MinIO API         | http://localhost:9000    | minio/minio123    |
| Kafka (External)  | localhost:19092          | -                 |
| Kafka (Internal)  | kafka:9092               | -                 |
| PostgreSQL        | localhost:5432           | postgres/postgres |
| Spark UI          | http://localhost:4040    | (when job running)|

## 🧩 Data Flow

### 🔵 Ingestion — Reddit Producer

**File**: `src/producer/reddit_producer.py`

- Connects to Reddit API using PRAW library
- Monitors configured subreddits: `['Professors', 'PhD', 'GradSchool', 'csMajors', 'EngineeringStudents']`
- Fetches new posts every 30 seconds (configurable)
- Deduplicates using in-memory set (max 10,000 post IDs)
- Sends JSON messages to Kafka topic `reddit_posts`
- Compression: gzip, Acks: all (durability)

### 🟤 Bronze Layer — Raw Data Ingestion

**File**: `src/streaming/stream_bronze.py`

- **Source**: Kafka topic `reddit_posts`
- **Processing**:
  - Parses JSON payload using predefined schema
  - Adds temporal columns: `event_time`, `dt`, `ingest_ts`
  - No filtering or transformation (raw data preservation)
- **Sink**: `s3a://datalake/bronze/reddit_posts` (Delta Lake)
- **Trigger**: Every 10 seconds
- **Checkpoint**: `s3a://datalake/_checkpoints/bronze`

### ⚪ Silver Layer — Clean & Deduplicate

**File**: `src/streaming/stream_silver.py`

- **Source**: Bronze Delta table
- **Processing**:
  - Trim text fields (title: 5000 chars, selftext: 20000 chars)
  - Fill missing numeric values (upvotes, num_comments → 0)
  - Drop empty posts (no title AND no selftext)
  - Deduplicate by `post_id` (keep latest by `ingest_ts`)
  - Watermark: 15 minutes for late-arriving data
- **Sink**: `s3a://datalake/silver/reddit_posts` (Delta Lake)
- **Merge Strategy**: Delta MERGE (upsert on `post_id`)
- **Trigger**: Every 10 seconds
- **Checkpoint**: `s3a://datalake/_checkpoints/silver`

### 🟡 Gold Layer — ML Inference & Feature Engineering

**File**: `src/streaming/stream_gold.py`

- **Source**: Silver Delta table
- **Processing**:
  - Build combined text: `text = title + " " + selftext`
  - Compute engagement metric: `interaction_rate = (upvotes + 0.5*comments) / age_hours`
  - Run stress classification using DistilBERT model
  - Apply threshold (default: 0.6) to generate binary label
  - Add metadata: `feature_version`, `model_version`
- **Model Inference**:
  - Pandas UDF for distributed batch processing
  - Singleton pattern: one model instance per executor
  - Batch size: 16 (configurable)
  - Output: `score_stress` (probability), `label_stress` (0/1)
- **Sink**: PostgreSQL `gold_reddit_posts` table OR Delta Lake (configurable)
- **Trigger**: Every 10 seconds
- **Checkpoint**: `s3a://datalake/_checkpoints/gold`

## 🧠 Machine Learning

### Model Architecture

**File**: `src/model/infer.py`

- **Base Model**: DistilBERT (distilbert-base-uncased fine-tuned)
- **Task**: Binary sequence classification (stress vs. non-stress)
- **Storage**: Model artifacts stored as `.zip` on S3/MinIO
- **Inference Service**:
  - Singleton per Spark executor (thread-safe caching)
  - Content-addressed caching: `/tmp/model_cache/{sha1_hash}/`
  - Lazy loading: model loaded on first inference call
  - Batched inference with configurable batch size

### Training (Optional)

**File**: `src/model/train.py`

```bash
# Train a new model
make train
```

The training script:
- Loads dataset from configured source
- Fine-tunes DistilBERT on stress/non-stress classification
- Exports model as `.zip` to MinIO/S3
- Updates config with new model URI

### Model Configuration

Configure in `configs/config.yaml`:
```yaml
model:
  classification_model: 's3a://artifacts/models/20251104T073104Z/distilbert_finetuned.zip'
  classifier_label_index: 1  # Index for positive class (stress)
  classifier_batch_size: 16
  classifier_threshold: 0.6  # Binary classification threshold
  model_version: 'distilbert-finetuned-v1'
```

## ⚙️ Configuration

### Configuration File

All application settings are centralized in `configs/config.yaml` with support for environment variable expansion using the pattern `${ENV_VAR:-default_value}`.

**Key configuration sections:**

```yaml
# Kafka settings
kafka:
  bootstrap_servers: '${KAFKA_BOOTSTRAP_SERVERS:-localhost:19092}'
  topic_posts: 'reddit_posts'

# MinIO/S3 settings
minio:
  endpoint_url: '${S3_ENDPOINT_URL:-http://localhost:9000}'
  access_key: '${AWS_ACCESS_KEY_ID:-minio}'
  secret_key: '${AWS_SECRET_ACCESS_KEY:-minio123}'
  bucket: 'datalake'

# Data layer paths
sink:
  bronze_prefix: 'bronze/reddit_posts'
  silver_prefix: 'silver/reddit_posts'
  gold_prefix: 'gold/reddit_posts'
  gold_backend: 'postgres'  # or 'delta'

# Reddit API
reddit:
  subreddits: ['Professors', 'PhD', 'GradSchool', 'csMajors', 'EngineeringStudents']
  poll_interval: 30  # seconds
```

### Environment Variables

Create a `.env` file from the template:
```bash
cp .env.example .env
```

Required variables:
- `REDDIT_CLIENT_ID` - Reddit API client ID
- `REDDIT_CLIENT_SECRET` - Reddit API client secret
- `REDDIT_USER_AGENT` - User agent for Reddit API

## 🔧 Development

### Running Tests

```bash
# Run all tests
make test
# or
pytest -q

# Run specific test file
pytest tests/test_infer.py -v
```

### Code Quality

```bash
# Lint code with Ruff
make lint
# or
ruff check .

# Format code with Black
make format
# or
black .
```

### Database Migrations

```bash
# Apply pending migrations
make migrate

# Check migration status
make migrate_info

# Repair migration checksums (if schema files changed)
make migrate_repair
```

Migration files are in `migrations/` directory using Flyway SQL format.

### Makefile Commands

```bash
make help              # Show all available commands
make env               # Show current environment configuration
make zip               # Zip source code for Spark
make producer          # Run Reddit producer
make stream_bronze     # Run Bronze layer
make stream_silver     # Run Silver layer
make stream_gold       # Run Gold layer (with ML inference)
make topics            # Create/list Kafka topics
make train             # Train ML model
make lint              # Lint code
make format            # Format code
make test              # Run tests
```

## 📊 Monitoring & Debugging

### View Kafka Messages

```bash
# Consume messages from Reddit posts topic
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic reddit_posts \
  --from-beginning
```

### Access MinIO Console

Navigate to http://localhost:9001 and login with:
- Username: `minio`
- Password: `minio123`

Browse the `datalake` bucket to see Bronze, Silver, and Gold data layers.

### Spark UI

When streaming jobs are running, access Spark UI at http://localhost:4040 to monitor:
- Active queries
- Job execution
- Stage details
- Executor metrics

### PostgreSQL Database

Connect to PostgreSQL to query the Gold table:

```bash
# Using psql
docker compose exec postgres psql -U postgres -d postgres

# Query gold table
SELECT post_id, title, score_stress, label_stress, created_utc
FROM gold_reddit_posts
ORDER BY created_utc DESC
LIMIT 10;
```

### Check Delta Table History

```python
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()
dt = DeltaTable.forPath(spark, "s3a://datalake/silver/reddit_posts")

# View table history
dt.history().show(10, truncate=False)

# View current version
spark.read.format("delta").load("s3a://datalake/silver/reddit_posts").show()
```

### Logs

Each layer outputs detailed logs:
- Reddit Producer: Real-time post ingestion stats
- Bronze: Kafka consumption metrics
- Silver: Deduplication and merge stats
- Gold: ML inference performance and batch metrics

## 🐛 Troubleshooting

### Issue: Kafka connection refused

**Solution**: Ensure Kafka is healthy
```bash
docker compose ps kafka
docker compose logs kafka
```

### Issue: S3/MinIO access errors

**Solution**: Check MinIO is running and credentials are correct
```bash
docker compose ps minio
docker compose logs minio

# Verify bucket exists
docker compose exec minio-mc mc ls local/datalake
```

### Issue: Model file not found

**Solution**: Ensure model URI in `configs/config.yaml` is correct and accessible:
```yaml
model:
  classification_model: 's3a://artifacts/models/.../model.zip'
```

Verify the model exists in MinIO console or check the path.

### Issue: Streaming checkpoint errors

**Solution**: Delete checkpoint directory to reset (WARNING: may cause reprocessing)
```bash
# Reset Bronze checkpoint
docker compose exec minio-mc mc rm --recursive --force \
  local/datalake/_checkpoints/bronze/
```

### Issue: Out of memory errors

**Solution**: Reduce Spark parallelism or batch size in `configs/config.yaml`:
```yaml
spark:
  shuffle_partitions: 2  # Reduce from 4

model:
  classifier_batch_size: 8  # Reduce from 16
```

### Issue: Reddit API rate limits

**Solution**: Increase poll interval in `configs/config.yaml`:
```yaml
reddit:
  poll_interval: 60  # Increase from 30 seconds
```

## 📚 References

- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/streaming/index.html)
- [Delta Lake](https://docs.delta.io/)
- [MinIO Documentation](https://min.io/docs/)
- [Medallion Architecture (Databricks)](https://www.databricks.com/glossary/medallion-architecture)
- [PRAW (Python Reddit API Wrapper)](https://praw.readthedocs.io/)
- [Flyway Migrations](https://documentation.red-gate.com/fd)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is open source and available under the MIT License.

## 🧑‍💻 Authors

**Reddit Stress Streaming** developed by:
- **[Karhdo](https://github.com/karhdo)**
- **[Ziduck](https://github.com/ziduck)**

October 2025

## 🙏 Acknowledgments

- Mental health-focused subreddit communities for providing valuable data
- Apache Spark and Delta Lake communities for excellent streaming tools
