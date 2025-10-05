from dataclasses import dataclass


@dataclass
class SinkCfg:
    bronze_prefix: str = "bronze/reddit_posts"
    silver_prefix: str = "silver/reddit_posts"
    gold_prefix: str = "gold/reddit_posts"

    bronze_table: str = "bronze_reddit_posts"
    silver_table: str = "silver_reddit_posts"
    gold_table: str = "gold_reddit_posts"
