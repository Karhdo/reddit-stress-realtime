import json

from dataclasses import dataclass, asdict
from typing import Optional
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)


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
        """Serialize to JSON bytes for Kafka"""
        data = asdict(self)
        return json.dumps(data, ensure_ascii=False).encode("utf-8")
