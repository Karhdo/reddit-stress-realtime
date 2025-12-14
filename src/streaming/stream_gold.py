from __future__ import annotations

from typing import Iterator
import pandas as pd
from loguru import logger
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T
from pyspark.sql.functions import pandas_udf

from src.common.config import Config
from src.model.infer import get_service
from src.common.schema import SILVER_COLS, GOLD_COLS, get_gold_schema


# Preflight: load model once on driver to fail fast
def _preflight_model(cfg: Config) -> None:
    uri = cfg.model.classification_model
    pos_idx = int(cfg.model.classifier_label_index)
    max_len = int(getattr(cfg.model, "max_len", 256))

    if not uri or not uri.lower().endswith(".zip"):
        raise ValueError(
            "classification_model must be a .zip path (e.g., s3a://.../distilbert_finetuned.zip)"
        )

    svc = get_service(clf_model=uri, pos_idx=pos_idx, max_len=max_len, cfg=cfg)
    _ = svc.classify(["warmup"], batch_size=1)
    logger.info(
        f"[GOLD] Model preflight OK: {uri} (pos_idx={pos_idx}, max_len={max_len})"
    )


# Pandas UDF factory (each executor keeps a cached service)
def _make_stress_score_udf(cfg: Config):
    uri = cfg.model.classification_model
    pos_idx = int(cfg.model.classifier_label_index)
    batch_size = int(cfg.model.classifier_batch_size)
    max_len = int(getattr(cfg.model, "max_len", 256))

    @pandas_udf(T.DoubleType())
    def stress_score_iter(text_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
        svc = get_service(clf_model=uri, pos_idx=pos_idx, max_len=max_len, cfg=cfg)
        for s in text_iter:
            texts = s.fillna("").tolist()
            scores = svc.classify(texts, batch_size=batch_size)
            yield pd.Series([float(x) for x in scores])

    return stress_score_iter


# Ensure Gold Delta table exists (run once on driver)
def _init_gold_if_needed(spark: SparkSession, gold_path: str) -> None:
    if DeltaTable.isDeltaTable(spark, gold_path):
        return

    (
        spark.createDataFrame([], get_gold_schema())
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("dt")
        .save(gold_path)
    )
    logger.info(f"[GOLD] Initialized empty Delta table at {gold_path} (partitionBy=dt)")


# Per-batch transform: select, build text, score, threshold, normalize time
def _transform_to_gold(batch_df: DataFrame, cfg: Config) -> DataFrame:
    feature_version = cfg.model.feature_version
    model_version = cfg.model.model_version
    cls_threshold = float(cfg.model.classifier_threshold)

    stress_udf = _make_stress_score_udf(cfg)

    # Select only required columns from Silver
    cols = [c for c in SILVER_COLS if c in batch_df.columns]
    df = batch_df.select(*cols)

    # Build text = title + selftext (trim & normalize whitespace)
    df = (
        df.withColumn("title", F.coalesce(F.col("title").cast("string"), F.lit("")))
        .withColumn("selftext", F.coalesce(F.col("selftext").cast("string"), F.lit("")))
        .withColumn("text", F.concat_ws(" ", F.col("title"), F.col("selftext")))
        .withColumn("text", F.regexp_replace(F.col("text"), r"\s+", " "))
        .withColumn("text", F.trim(F.col("text")))
    )

    # Compute interaction_rate
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

    # Score & threshold
    df = df.withColumn("score_stress", stress_udf(F.col("text")).cast("double"))
    df = df.withColumn(
        "label_stress",
        F.when(F.col("score_stress") >= F.lit(cls_threshold), F.lit(1)).otherwise(
            F.lit(0)
        ),
    )

    # Metadata & time normalization
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


# Per-batch sink: transform then append to Delta (no extra counts)
def _to_gold(batch_df: DataFrame, batch_id: int, cfg: Config) -> None:
    n_in = batch_df.count()
    logger.info(f"[GOLD] batch_id={batch_id} | input_rows={n_in}")
    if n_in == 0:
        return

    out_df = _transform_to_gold(batch_df, cfg)

    if cfg.sink.is_delta():
        gold_path = cfg.s3_uri(cfg.sink.gold_prefix)
        (
            out_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("dt")
            .save(gold_path)
        )
        logger.info(f"[GOLD] ✅ Wrote batch_id={batch_id} → {gold_path}")

    elif cfg.sink.is_postgres():
        jdbc_url = cfg.sink.jdbc_url(cfg.postgres)
        (
            out_df.write.format("jdbc")
            .mode("append")
            .option("url", jdbc_url)
            .option("dbtable", "public.gold_reddit_posts")
            .option("user", cfg.postgres.user)
            .option("password", cfg.postgres.password)
            .option("driver", "org.postgresql.Driver")
            .save()
        )
        logger.info(
            f"[GOLD] ✅ Wrote batch_id={batch_id} → {jdbc_url}#gold_reddit_posts"
        )

    else:
        raise ValueError(
            f"Unsupported gold_backend={cfg.sink.gold_backend!r}. Use 'delta' or 'postgres'."
        )


# Stream entry: init once, start foreachBatch
def stream_gold(spark: SparkSession, cfg: Config) -> None:
    # Enable Arrow for pandas UDF
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # Fail fast on model
    _preflight_model(cfg)

    # Paths
    silver_path = cfg.s3_uri(cfg.sink.silver_prefix)
    gold_path = cfg.s3_uri(cfg.sink.gold_prefix)
    checkpoint_path = f"{cfg.app.checkpoint_base.rstrip('/')}/gold"

    # Ensure Gold table exists once on driver
    _init_gold_if_needed(spark, gold_path)

    # Start stream
    logger.info(
        f"[GOLD] silver={silver_path} | gold={gold_path} | checkpoint={checkpoint_path}"
    )
    silver_stream = (
        spark.readStream.format("delta")
        .option("maxOffsetsPerTrigger", 5)
        .load(silver_path)
    )

    (
        silver_stream.writeStream.queryName("gold-stress-classifier")
        .foreachBatch(lambda df, bid: _to_gold(df, bid, cfg))
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info("🚀 [GOLD] Streaming started.")
