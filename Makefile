# ===== Project =====
PYTHON ?= python3
SPARK_SUBMIT ?= spark-submit
PROJECT_ROOT := $(shell pwd)
PYFILES := src.zip

# ===== Versions =====
SPARK_VERSION ?= 3.5.1
SCALA_BINARY ?= 2.12
DELTA_VERSION ?= 3.2.0
HADOOP_AWS_VERSION ?= 3.3.4
AWS_SDK_BUNDLE_VERSION ?= 1.12.767

# Spark packages: Kafka + Delta + S3A + Postgres JDBC
SPARK_PACKAGES := \
  org.apache.spark:spark-sql-kafka-0-10_$(SCALA_BINARY):$(SPARK_VERSION),\
  io.delta:delta-spark_$(SCALA_BINARY):$(DELTA_VERSION),\
  org.apache.hadoop:hadoop-aws:$(HADOOP_AWS_VERSION),\
  com.amazonaws:aws-java-sdk-bundle:$(AWS_SDK_BUNDLE_VERSION),\
  org.postgresql:postgresql:42.7.4

# ===== Load .env (single source of truth) =====
ifneq (,$(wildcard .env))
  include .env
  export $(shell sed -n 's/^\([A-Za-z0-9_]\+\)=.*/\1/p' .env)
endif

# ===== App / Entrypoint =====
CHECKPOINT_BASE ?= s3a://$(MINIO_BUCKET)/_checkpoints
APP_ENTRY ?= src/streaming/app.py

# ===== Common Spark flags (DRY) =====
SPARK_COMMON := \
  --master local[*] \
  --packages $(SPARK_PACKAGES) \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.hadoop.fs.s3a.endpoint=$(S3_ENDPOINT_URL) \
  --conf spark.hadoop.fs.s3a.access.key=$(AWS_ACCESS_KEY_ID) \
  --conf spark.hadoop.fs.s3a.secret.key=$(AWS_SECRET_ACCESS_KEY) \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore \
  --conf spark.sql.shuffle.partitions=4

# ===== Phony =====
.PHONY: help zip env producer stream_bronze stream_silver stream_gold train topics lint format test migrate migrate_info migrate_repair

# Default: show help
help:
	@echo "Usage:"
	@echo "  make env              # Show key env values"
	@echo "  make zip              # Zip src -> $(PYFILES)"
	@echo "  make producer         # Run Reddit → Kafka producer (local)"
	@echo "  make stream_bronze    # Kafka → Delta Bronze (stream)"
	@echo "  make stream_silver    # Bronze → Silver (stream)"
	@echo "  make stream_gold      # Silver → Gold (inference, stream)"
	@echo "  make topics           # Create/list Kafka topics via docker compose"
	@echo "  make train            # Run training script"
	@echo "  make lint | format    # Ruff / Black"
	@echo "  make test             # Run tests"
	@echo "  make migrate*         # Flyway migrations (docker compose)"

zip:
	@echo "Zipping src/ -> $(PYFILES)"
	@(cd src && zip -qr ../$(PYFILES) .)

env:
	@echo "Using .env values:"
	@echo "  KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS)"
	@echo "  S3_ENDPOINT_URL=$(S3_ENDPOINT_URL)"
	@echo "  AWS_ACCESS_KEY_ID set?  " $(if $(AWS_ACCESS_KEY_ID),YES,NO)
	@echo "  AWS_SECRET_ACCESS_KEY set? " $(if $(AWS_SECRET_ACCESS_KEY),YES,NO)
	@echo "  MINIO_BUCKET=$(MINIO_BUCKET)"
	@echo "  CHECKPOINT_BASE=$(CHECKPOINT_BASE)"

producer:
	PYTHONPATH=$(PROJECT_ROOT) \
	KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) \
	$(PYTHON) -m src.producer.reddit_producer

# Kafka → Delta Bronze
stream_bronze: zip
	PYTHONPATH=$(PROJECT_ROOT) \
	APP_MODE=bronze \
	CHECKPOINT_BASE=$(CHECKPOINT_BASE) \
	$(SPARK_SUBMIT) \
	  $(SPARK_COMMON) \
	  --py-files $(PYFILES) \
	  $(APP_ENTRY)

# Bronze → Silver (clean/dedup/MERGE)
stream_silver: zip
	PYTHONPATH=$(PROJECT_ROOT) \
	APP_MODE=silver \
	CHECKPOINT_BASE=$(CHECKPOINT_BASE) \
	$(SPARK_SUBMIT) \
	  $(SPARK_COMMON) \
	  --py-files $(PYFILES) \
	  $(APP_ENTRY)

# Silver → Gold (HF inference → write sink)
stream_gold: zip
	PYTHONPATH=$(PROJECT_ROOT) \
	APP_MODE=gold \
	TORCH_DEVICE=cpu OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 TOKENIZERS_PARALLELISM=false \
	CHECKPOINT_BASE=$(CHECKPOINT_BASE) \
	$(SPARK_SUBMIT) \
	  $(SPARK_COMMON) \
	  --py-files $(PYFILES) \
	  $(APP_ENTRY)

train:
	PYTHONPATH=$(PROJECT_ROOT) \
	$(PYTHON) -m src.model.train

topics:
	@echo "Creating topics in container (kafka:9092)..." ; \
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
	  --create --topic reddit_posts --if-not-exists --replication-factor 1 --partitions 3 ; \
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

lint:
	ruff check .

format:
	black .

test:
	pytest -q

# Flyway migrations via docker compose
migrate:
	docker compose run --rm flyway

migrate_info:
	docker compose run --rm flyway info

migrate_repair:  # use sparingly (fix checksums when an old file changed)
	docker compose run --rm flyway repair