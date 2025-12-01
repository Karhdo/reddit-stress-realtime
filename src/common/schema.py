# src/common/schema.py
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from typing import List, Optional

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
    DateType,
)


# Bronze schema for Kafka → Delta ingestion
reddit_post_schema = StructType(
    [
        StructField("post_id", StringType(), True),
        StructField("subreddit", StringType(), True),
        StructField("title", StringType(), True),
        StructField("selftext", StringType(), True),
        StructField("permalink", StringType(), True),
        StructField("created_utc", DoubleType(), True),
        StructField("upvotes", IntegerType(), True),
        StructField("num_comments", IntegerType(), True),
        StructField("author", StringType(), True),
        StructField("ingest_ts", TimestampType(), True),
    ]
)


# Dataclass for Kafka producer messages
@dataclass
class RedditPost:
    post_id: str
    subreddit: str
    title: str
    selftext: str
    permalink: Optional[str] = None
    created_utc: Optional[float] = None
    upvotes: Optional[int] = None
    num_comments: Optional[int] = None
    author: Optional[str] = None

    def to_kafka_value(self) -> bytes:
        """Serialize to UTF-8 JSON for Kafka producer."""
        return json.dumps(asdict(self), ensure_ascii=False).encode("utf-8")


# Column contracts (shared across all layers)
BRONZE_COLS: List[str] = [
    "post_id",
    "subreddit",
    "title",
    "selftext",
    "permalink",
    "upvotes",
    "num_comments",
    "author",
    "event_time",
    "ingest_ts",
]

SILVER_COLS: List[str] = BRONZE_COLS + ["created_utc", "dt", "vi_ratio", "lang_bucket"]

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


# Spark schema helpers (used by Silver/Gold initialization)
def get_silver_schema() -> StructType:
    """Return a Spark StructType for the Silver layer."""
    return StructType(
        [
            StructField("post_id", StringType(), True),
            StructField("subreddit", StringType(), True),
            StructField("title", StringType(), True),
            StructField("selftext", StringType(), True),
            StructField("permalink", StringType(), True),
            StructField("created_utc", TimestampType(), True),
            StructField("upvotes", IntegerType(), True),
            StructField("num_comments", IntegerType(), True),
            StructField("author", StringType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("dt", DateType(), True),
            StructField("ingest_ts", TimestampType(), True),
            StructField("vi_ratio", DoubleType(), True),
            StructField("lang_bucket", StringType(), True),
        ]
    )


def get_gold_schema() -> StructType:
    """Return a Spark StructType for the Gold layer."""
    return StructType(
        [
            StructField("post_id", StringType(), True),
            StructField("subreddit", StringType(), True),
            StructField("created_utc", TimestampType(), True),
            StructField("dt", DateType(), True),
            StructField("title", StringType(), True),
            StructField("text", StringType(), True),
            StructField("interaction_rate", DoubleType(), True),
            StructField("score_stress", DoubleType(), True),
            StructField("label_stress", IntegerType(), True),
            StructField("permalink", StringType(), True),
            StructField("feature_version", StringType(), True),
            StructField("model_version", StringType(), True),
        ]
    )
