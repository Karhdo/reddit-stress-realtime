from pyspark.sql import SparkSession, functions as F

from src.common.logging_utils import get_logger
from src.common.schema import reddit_post_schema
from src.common.config import Config

log = get_logger(__name__)


def stream_bronze(spark: SparkSession, cfg: Config) -> None:
    """Read from Kafka, parse JSON to Bronze schema, add timestamps, and write Delta to MinIO."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
        .option("subscribe", cfg.kafka.topic_posts)
        .option("startingOffsets", cfg.kafka.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    df = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json("json", reddit_post_schema).alias("j"))
        .select("j.*")
        .withColumn("event_time", F.to_timestamp(F.from_unixtime("created_utc")))
        .withColumn("dt", F.to_date("event_time"))
        .withColumn("ingest_ts", F.current_timestamp())
    )

    bronze_path = f"s3a://{cfg.minio.bucket}/{cfg.sink.bronze_prefix}"
    checkpoint_path = f"{cfg.app.checkpoint_base.rstrip('/')}/bronze"

    (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .start(bronze_path)
    )

    log.info(f"[BRONZE] Kafka → Delta @ {bronze_path} | checkpoint @ {checkpoint_path}")
