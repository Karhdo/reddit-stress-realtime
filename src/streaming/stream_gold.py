from __future__ import annotations


from typing import Iterator, List
import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from pyspark.sql.functions import pandas_udf
from loguru import logger

from src.common.config import Config
from src.model.infer import get_service  # worker-local singleton

# Gold layer (stress classifier only, no embeddings)
# Data is written to Delta Lake partitioned by `dt`.
SILVER_INPUT_COLS: List[str] = [
    "post_id",
    "subreddit",
    "title",
    "selftext",
    "permalink",
    "upvotes",
    "num_comments",
    "event_time",
    "ingest_ts",
]

GOLD_COLS: List[str] = [
    "post_id",
    "subreddit",
    "created_utc",
    "dt",
    "title",
    "text",
    "interaction_rate",
    "score_stress",
    "label_stress",
    "permalink",
    "feature_version",
    "model_version",
]


# ====== UDF factory (Stress classifier) ======
def _make_stress_score_udf(
    classification_model: str,
    classifier_label_index: int,
    batch_size: int,
    max_len: int = 256,
):
    """Scalar iterator pandas UDF that runs InferenceService.classify() on each worker."""

    @pandas_udf(T.DoubleType())
    def stress_score_iter(text_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
        svc = get_service(
            embed_model=classification_model,  # cache key for singleton
            clf_model=classification_model,
            pos_idx=classifier_label_index,
            max_len=max_len,
        )
        for text_col in text_iter:
            texts = text_col.fillna("").tolist()
            scores = svc.classify(texts, batch_size=batch_size)
            yield pd.Series([float(s) for s in scores])

    return stress_score_iter


# ====== Delta table bootstrap ======
def _init_gold_if_needed(spark: SparkSession, gold_path: str) -> None:
    """Ensure the Gold Delta table exists and is partitioned by `dt`."""
    if not DeltaTable.isDeltaTable(spark, gold_path):
        schema = T.StructType(
            [
                T.StructField("post_id", T.StringType(), True),
                T.StructField("subreddit", T.StringType(), True),
                T.StructField("created_utc", T.TimestampType(), True),
                T.StructField("dt", T.DateType(), True),
                T.StructField("title", T.StringType(), True),
                T.StructField("text", T.StringType(), True),
                T.StructField("interaction_rate", T.DoubleType(), True),
                T.StructField("score_stress", T.DoubleType(), True),
                T.StructField("label_stress", T.IntegerType(), True),
                T.StructField("permalink", T.StringType(), True),
                T.StructField("feature_version", T.StringType(), True),
                T.StructField("model_version", T.StringType(), True),
            ]
        )
        (
            spark.createDataFrame([], schema)
            .write.format("delta")
            .mode("overwrite")
            .partitionBy("dt")
            .save(gold_path)
        )
        logger.info(
            f"[GOLD] Initialized empty Delta table at {gold_path} (partitionBy=dt)"
        )


# ====== Per-batch transform ======
def _transform_to_gold(batch_df: DataFrame, cfg: Config) -> DataFrame:
    """Builds final Gold records: combine text, compute interaction rate, classify stress."""
    feature_version = cfg.model.feature_version
    cls_model = cfg.model.classification_model
    cls_label_idx = int(cfg.model.classifier_label_index)
    cls_bs = int(cfg.model.classifier_batch_size)
    cls_threshold = float(cfg.model.classifier_threshold)
    model_version = cfg.model.model_version

    stress_udf = _make_stress_score_udf(cls_model, cls_label_idx, cls_bs, max_len=256)

    # Select required columns only
    cols_present = [c for c in SILVER_INPUT_COLS if c in batch_df.columns]
    df = batch_df.select(*cols_present)

    # Build `text` field
    df = (
        df.withColumn("title", F.coalesce(F.col("title").cast("string"), F.lit("")))
        .withColumn("selftext", F.coalesce(F.col("selftext").cast("string"), F.lit("")))
        .withColumn("text", F.concat_ws(" ", F.col("title"), F.col("selftext")))
    )

    # Compute interaction rate in Spark
    now_ts = F.current_timestamp()
    age_hours = F.greatest(
        (F.unix_timestamp(now_ts) - F.unix_timestamp(F.col("event_time")))
        / F.lit(3600.0),
        F.lit(1.0),
    )
    up = F.coalesce(F.col("upvotes").cast("double"), F.lit(0.0))
    nc = F.coalesce(F.col("num_comments").cast("double"), F.lit(0.0))
    df = df.withColumn(
        "interaction_rate", ((up + F.lit(0.5) * nc) / age_hours).cast("double")
    )

    # Run classifier and threshold the scores
    df = df.withColumn("score_stress", stress_udf(F.col("text")).cast("double"))
    df = df.withColumn(
        "label_stress",
        F.when(F.col("score_stress") >= F.lit(cls_threshold), F.lit(1)).otherwise(
            F.lit(0)
        ),
    )

    # Metadata & time normalization (avoid __HIVE_DEFAULT_PARTITION__)
    df = df.withColumn("feature_version", F.lit(feature_version))
    df = df.withColumn("model_version", F.lit(model_version))
    df = df.withColumn(
        "created_utc",
        F.coalesce(
            F.col("event_time").cast("timestamp"), F.col("ingest_ts").cast("timestamp")
        ),
    ).where(F.col("created_utc").isNotNull())
    df = df.withColumn("dt", F.to_date(F.col("created_utc")))

    return df.select(*GOLD_COLS)


# ====== Batch sink ======
def _to_gold(batch_df: DataFrame, batch_id: int, cfg: Config) -> None:
    spark = batch_df.sparkSession
    gold_path = f"s3a://{cfg.minio.bucket}/{cfg.sink.gold_prefix}"

    n_in = batch_df.count()
    logger.info(f"[GOLD] batch_id={batch_id} | input_rows={n_in}")
    if n_in == 0:
        return

    _init_gold_if_needed(spark, gold_path)
    out_df = _transform_to_gold(batch_df, cfg)

    (
        out_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("dt")
        .save(gold_path)
    )

    logger.info(
        f"[GOLD] ✅ Wrote {out_df.count()} rows to {gold_path} (batch_id={batch_id})"
    )


# ====== Stream entry ======
def stream_gold(spark: SparkSession, cfg: Config) -> None:
    """Main entry point for Gold stream — runs stress classification and writes partitioned Delta."""
    logger.info(
        "🚀 Starting Gold streaming job (stress classifier only, partitioned by dt)…"
    )

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    silver_path = f"s3a://{cfg.minio.bucket}/{cfg.sink.silver_prefix}"
    checkpoint_path = f"{cfg.app.checkpoint_base.rstrip('/')}/gold"

    logger.info(f"[GOLD] silver_path={silver_path}")
    logger.info(f"[GOLD] checkpoint={checkpoint_path}")

    silver_stream = spark.readStream.format("delta").load(silver_path)

    (
        silver_stream.writeStream.queryName("gold-stress-classifier")
        .foreachBatch(lambda df, bid: _to_gold(df, bid, cfg))
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="15 seconds")
        .start()
    )

    logger.info(
        f"[GOLD] Streaming started from {silver_path} -> checkpoint @ {checkpoint_path}"
    )
