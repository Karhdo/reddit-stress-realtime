from dataclasses import dataclass


@dataclass
class KafkaCfg:
    bootstrap_servers: str = "localhost:9092"
    topic_posts: str = "reddit_posts"
    starting_offsets: str = "latest"
    fail_on_data_loss: bool = False
