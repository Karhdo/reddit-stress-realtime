from __future__ import annotations

from delta.tables import DeltaTable
from loguru import logger
from pyspark.sql import DataFrame, SparkSession, functions as F, Window

from src.common.config import Config
from src.common.schema import SILVER_COLS, get_silver_schema


def _ensure_silver_table(spark: SparkSession, silver_path: str) -> None:
    # Create an empty Silver Delta table once (idempotent).
    if DeltaTable.isDeltaTable(spark, silver_path):
        return
    (
        spark.createDataFrame([], get_silver_schema())
        .write.format("delta")
        .mode("overwrite")
        .save(silver_path)
    )
    logger.info(f"[SILVER] Initialized Delta table at {silver_path}")


def _project_and_clean(df: DataFrame) -> DataFrame:
    # Select, sanitize, and complete required columns.
    # Keep only needed columns (if present)
    cols_present = [c for c in SILVER_COLS if c in df.columns]
    df = df.select(*cols_present)

    # Trim long text & fill numeric nulls
    df = (
        df.withColumn("title", F.substring(F.col("title").cast("string"), 1, 5000))
        .withColumn("selftext", F.substring(F.col("selftext").cast("string"), 1, 20000))
        .withColumn("upvotes", F.col("upvotes").cast("long"))
        .withColumn("num_comments", F.col("num_comments").cast("long"))
        .na.fill({"upvotes": 0, "num_comments": 0})
    )

    # Drop empty posts (no title & no selftext)
    df = df.where((F.length(F.col("title")) > 0) | (F.length(F.col("selftext")) > 0))

    # Normalize timestamps & derive dt
    df = df.withColumn("event_time", F.col("event_time").cast("timestamp"))
    df = df.withColumn("ingest_ts", F.col("ingest_ts").cast("timestamp"))
    df = df.withColumn("created_utc", F.col("created_utc").cast("timestamp"))
    if "dt" not in df.columns:
        df = df.withColumn("dt", F.to_date(F.col("event_time")))
    return df


def _dedup_latest(df: DataFrame) -> DataFrame:
    # Deduplicate by post_id (latest ingest_ts, then latest event_time).
    w = Window.partitionBy("post_id").orderBy(
        F.col("ingest_ts").desc_nulls_last(), F.col("event_time").desc_nulls_last()
    )
    # Keep row_number == 1 for each post_id
    df = df.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1).drop("rn")
    # Project to fixed Silver columns (stable schema)
    return df.select(*SILVER_COLS)


def _merge_into_silver(spark: SparkSession, df: DataFrame, silver_path: str) -> None:
    # MERGE deduplicated rows into the Silver Delta table.
    tgt = DeltaTable.forPath(spark, silver_path)
    src = df.alias("s")
    tgt_alias = tgt.alias("t")

    # Build SET map dynamically (t.col = s.col for all SILVER_COLS)
    set_map = {c: F.col(f"s.{c}") for c in SILVER_COLS}

    (
        tgt_alias.merge(src, "t.post_id = s.post_id")
        .whenMatchedUpdate(set=set_map)
        .whenNotMatchedInsert(values=set_map)
        .execute()
    )


def _process_batch(batch_df: DataFrame, batch_id: int, cfg: Config) -> None:
    # Per-batch handler: clean → dedup → merge.
    spark = batch_df.sparkSession
    silver_path = f"s3a://{cfg.minio.bucket}/{cfg.sink.silver_prefix}"

    n_in = batch_df.count()
    logger.info(f"[SILVER] batch_id={batch_id} | input_rows={n_in}")
    if n_in == 0:
        return

    _ensure_silver_table(spark, silver_path)
    cleaned = _project_and_clean(batch_df)
    staged = _dedup_latest(cleaned)
    _merge_into_silver(spark, staged, silver_path)

    logger.info(
        f"[SILVER] ✅ Upserted batch_id={batch_id} into {silver_path} | rows={staged.count()}"
    )


def stream_silver(spark: SparkSession, cfg: Config) -> None:
    # 1. Entry point: read Bronze stream → foreachBatch upsert to Silver.
    logger.info("🚀 Starting Silver streaming job…")

    bronze_path = f"s3a://{cfg.minio.bucket}/{cfg.sink.bronze_prefix}"
    checkpoint_path = f"{cfg.app.checkpoint_base.rstrip('/')}/silver"

    logger.info(f"[SILVER] bronze_path={bronze_path}")
    logger.info(f"[SILVER] checkpoint={checkpoint_path}")

    # 2. Read Bronze as stream, set watermark for late data
    bronze_stream = (
        spark.readStream.format("delta")
        .option("startingVersion", 0)  # only meaningful for first bootstrap
        .option("maxOffsetsPerTrigger", 5)
        .load(bronze_path)
        .withWatermark("event_time", "15 minutes")
    )

    # 3. Upsert Silver in foreachBatch
    (
        bronze_stream.writeStream.queryName("silver-upsert")
        .foreachBatch(lambda df, bid: _process_batch(df, bid, cfg))
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info(
        f"[SILVER] Streaming started from {bronze_path} -> checkpoint @ {checkpoint_path}"
    )
