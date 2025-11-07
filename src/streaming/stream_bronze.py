from __future__ import annotations

from pyspark.sql import SparkSession, functions as F
from loguru import logger

from src.common.config import Config
from src.common.schema import reddit_post_schema


# Bronze stream: Kafka -> parse JSON -> add timestamps -> Delta Lake.
def stream_bronze(spark: SparkSession, cfg: Config) -> None:
    # 1. Read Kafka topic as a streaming DataFrame
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
        .option("subscribe", cfg.kafka.topic_posts)
        .option(
            "startingOffsets", cfg.kafka.starting_offsets
        )  # e.g., "latest" | "earliest"
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 5)
        .load()
    )

    # 2. Parse JSON payload to the Bronze schema and add time columns
    df = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json("json", reddit_post_schema).alias("j"))
        .select("j.*")
        .withColumn("event_time", F.to_timestamp(F.from_unixtime("created_utc")))
        .withColumn("dt", F.to_date("event_time"))
        .withColumn("ingest_ts", F.current_timestamp())
    )

    # 3. Write to Delta Lake with checkpointing
    bronze_path = f"s3a://{cfg.minio.bucket}/{cfg.sink.bronze_prefix}"
    checkpoint_path = f"{cfg.app.checkpoint_base.rstrip('/')}/bronze"

    (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start(bronze_path)
    )

    logger.info(
        f"[BRONZE] Kafka → Delta @ {bronze_path} | checkpoint @ {checkpoint_path}"
    )
