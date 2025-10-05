PYTHON ?= python3
SPARK_SUBMIT ?= spark-submit
PROJECT_ROOT := $(shell pwd)
PYFILES := src.zip

# ==== Versions ====
SPARK_VERSION ?= 3.5.1
SCALA_BINARY ?= 2.12
DELTA_VERSION ?= 3.2.0
HADOOP_AWS_VERSION ?= 3.3.4
AWS_SDK_BUNDLE_VERSION ?= 1.12.767

# Spark packages: Kafka + Delta + S3A (MinIO)
SPARK_PACKAGES := org.apache.spark:spark-sql-kafka-0-10_$(SCALA_BINARY):$(SPARK_VERSION),io.delta:delta-spark_$(SCALA_BINARY):$(DELTA_VERSION),org.apache.hadoop:hadoop-aws:$(HADOOP_AWS_VERSION),com.amazonaws:aws-java-sdk-bundle:$(AWS_SDK_BUNDLE_VERSION)

# ==== S3A / MinIO ====
S3A_ENDPOINT ?= http://localhost:9000
S3A_ACCESS_KEY ?= minio
S3A_SECRET_KEY ?= minio123
S3A_PATH_STYLE ?= true
S3A_SSL_ENABLED ?= false
DELTA_LOG_STORE ?= org.apache.spark.sql.delta.storage.S3SingleDriverLogStore

# ==== App / Config ====
KAFKA_BOOTSTRAP ?= localhost:9092    # if Spark in Docker network: kafka:29092
MINIO_BUCKET ?= datalake
APP_MODE ?= bronze                   # bronze | silver | gold
CHECKPOINT_BASE ?= s3a://$(MINIO_BUCKET)/_checkpoints
APP_ENTRY ?= src/streaming/app.py

.PHONY: producer stream_bronze stream_silver stream_gold train topics lint format test zip

zip:
	@echo "Zipping src/ -> $(PYFILES)"
	@(cd src && zip -qr ../$(PYFILES) .)

producer:
	PYTHONPATH=$(PROJECT_ROOT) \
	KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP) \
	$(PYTHON) -m src.producer.reddit_producer

# Bronze: Kafka -> Delta (MinIO)
stream_bronze: zip
	PYTHONPATH=$(PROJECT_ROOT) \
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
	MINIO_BUCKET=$(MINIO_BUCKET) \
	APP_MODE=bronze \
	CHECKPOINT_BASE=$(CHECKPOINT_BASE) \
	$(SPARK_SUBMIT) \
	  --master local[*] \
	  --packages $(SPARK_PACKAGES) \
	  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	  --conf spark.hadoop.fs.s3a.endpoint=$(S3A_ENDPOINT) \
	  --conf spark.hadoop.fs.s3a.access.key=$(S3A_ACCESS_KEY) \
	  --conf spark.hadoop.fs.s3a.secret.key=$(S3A_SECRET_KEY) \
	  --conf spark.hadoop.fs.s3a.path.style.access=$(S3A_PATH_STYLE) \
	  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=$(S3A_SSL_ENABLED) \
	  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
	  --conf spark.delta.logStore.class=$(DELTA_LOG_STORE) \
	  --conf spark.sql.shuffle.partitions=4 \
	  --py-files $(PYFILES) \
	  $(APP_ENTRY)

# Silver: Bronze -> clean/dedup -> MERGE to Silver
stream_silver: zip
	PYTHONPATH=$(PROJECT_ROOT) \
	APP_MODE=silver \
	$(SPARK_SUBMIT) \
	  --master local[*] \
	  --packages $(SPARK_PACKAGES) \
	  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	  --conf spark.hadoop.fs.s3a.endpoint=$(S3A_ENDPOINT) \
	  --conf spark.hadoop.fs.s3a.access.key=$(S3A_ACCESS_KEY) \
	  --conf spark.hadoop.fs.s3a.secret.key=$(S3A_SECRET_KEY) \
	  --conf spark.hadoop.fs.s3a.path.style.access=$(S3A_PATH_STYLE) \
	  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=$(S3A_SSL_ENABLED) \
	  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
	  --conf spark.delta.logStore.class=$(DELTA_LOG_STORE) \
	  --conf spark.sql.shuffle.partitions=4 \
	  --py-files $(PYFILES) \
	  $(APP_ENTRY)

# Gold: Silver -> stress classify -> write Gold (partitioned by dt)
stream_gold: zip
	PYTHONPATH=$(PROJECT_ROOT) \
	APP_MODE=gold \
	TORCH_DEVICE=cpu OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 TOKENIZERS_PARALLELISM=false \
	$(SPARK_SUBMIT) \
	  --master local[*] \
	  --packages $(SPARK_PACKAGES) \
	  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	  --conf spark.hadoop.fs.s3a.endpoint=$(S3A_ENDPOINT) \
	  --conf spark.hadoop.fs.s3a.access.key=$(S3A_ACCESS_KEY) \
	  --conf spark.hadoop.fs.s3a.secret.key=$(S3A_SECRET_KEY) \
	  --conf spark.hadoop.fs.s3a.path.style.access=$(S3A_PATH_STYLE) \
	  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=$(S3A_SSL_ENABLED) \
	  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
	  --conf spark.delta.logStore.class=$(DELTA_LOG_STORE) \
	  --conf spark.sql.shuffle.partitions=4 \
	  --py-files $(PYFILES) \
	  $(APP_ENTRY)

train:
	PYTHONPATH=$(PROJECT_ROOT) \
	$(PYTHON) -m src.model.train

topics:
	@echo "Creating topics..." ; \
	docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 \
	  --create --topic reddit_posts --if-not-exists --replication-factor 1 --partitions 3 ; \
	docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 \
	  --create --topic classified_posts --if-not-exists --replication-factor 1 --partitions 3

lint:
	ruff check .

format:
	black .

test:
	pytest -q