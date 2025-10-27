from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame, functions as F

from loguru import logger
from src.common.config import Config

# Target Silver schema (ensures stable MERGE columns)
SILVER_COLS = [
    "post_id",
    "subreddit",
    "title",
    "selftext",
    "permalink",
    "created_utc",
    "upvotes",
    "num_comments",
    "author",
    "event_time",
    "dt",
    "ingest_ts",
]


def _init_silver_if_needed(
    spark: SparkSession, silver_path: str, sample_df: DataFrame
) -> None:
    """Create empty Delta table with the desired schema if it doesn't exist."""
    if not DeltaTable.isDeltaTable(spark, silver_path):
        (
            sample_df.select(*SILVER_COLS)
            .limit(0)
            .write.format("delta")
            .mode("overwrite")
            .save(silver_path)
        )
        logger.info(f"[SILVER] Initialized empty Delta table at {silver_path}")


def _clean_bronze(batch_df: DataFrame) -> DataFrame:
    """Trim long text, fill numeric nulls, drop empty posts, ensure dt exists."""
    df = (
        batch_df.withColumn("title", F.substring(F.col("title"), 1, 5000))
        .withColumn("selftext", F.substring(F.col("selftext"), 1, 20000))
        .na.fill({"upvotes": 0, "num_comments": 0})
        .where((F.length("title") > 0) | (F.length("selftext") > 0))
    )
    if "dt" not in df.columns:
        df = df.withColumn("dt", F.to_date("event_time"))
    return df


def _upsert_silver(batch_df: DataFrame, batch_id: int, cfg: Config) -> None:
    """Deduplicate on post_id (latest ingest_ts wins) and MERGE into Silver."""
    spark = batch_df.sparkSession
    silver_path = f"s3a://{cfg.minio.bucket}/{cfg.sink.silver_prefix}"

    n_in = batch_df.count()
    logger.info(f"[SILVER] batch_id={batch_id} | input_rows={n_in}")
    if n_in == 0:
        return

    cleaned = _clean_bronze(batch_df)
    _init_silver_if_needed(spark, silver_path, cleaned)

    cleaned.createOrReplaceTempView("bronze_view")
    spark.sql(
        """
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY post_id
            ORDER BY ingest_ts DESC NULLS LAST, event_time DESC NULLS LAST
          ) AS rn
        FROM bronze_view
    """
    ).createOrReplaceTempView("staged")

    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW projected AS
        SELECT {", ".join(SILVER_COLS)} FROM staged WHERE rn = 1
    """
    )

    spark.sql(
        f"""
        MERGE INTO delta.`{silver_path}` AS t
        USING projected AS s
        ON t.post_id = s.post_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    )
    logger.info(f"[SILVER] ✅ Upserted batch_id={batch_id} into {silver_path}")


def stream_silver(spark: SparkSession, cfg: Config) -> None:
    """Main entry for Silver stream: read Bronze, clean/dedup, MERGE to Silver."""
    logger.info("🚀 Starting Silver streaming job…")

    bronze_path = f"s3a://{cfg.minio.bucket}/{cfg.sink.bronze_prefix}"
    checkpoint_path = f"{cfg.app.checkpoint_base.rstrip('/')}/silver"

    logger.info(f"[SILVER] bronze_path={bronze_path}")
    logger.info(f"[SILVER] checkpoint={checkpoint_path}")

    bronze_stream = (
        spark.readStream.format("delta")
        .option("startingVersion", 0) # Only for first bootstrap
        .load(bronze_path)
        .withWatermark("event_time", "15 minutes")
    )

    (
        bronze_stream.writeStream.queryName("silver-upsert")
        .foreachBatch(lambda df, bid: _upsert_silver(df, bid, cfg))
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info(
        f"[SILVER] Streaming started from {bronze_path} -> checkpoint @ {checkpoint_path}"
    )
