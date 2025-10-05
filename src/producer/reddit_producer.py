import json
import time
from typing import Any

import praw
from kafka import KafkaProducer
from loguru import logger

from src.common.config import load_config
from src.common.schema import RedditPost


def _to_bytes(v: Any) -> bytes:
    """
    Chuẩn hoá value_serializer:
    - Nếu đã là bytes -> trả nguyên.
    - Nếu là str/dict -> json.dumps và encode utf-8.
    - Nếu là object có .to_kafka_value() -> gọi và đảm bảo trả bytes.
    """
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    if hasattr(v, "to_kafka_value"):
        data = v.to_kafka_value()
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        if isinstance(data, str):
            return data.encode("utf-8")
        return json.dumps(data, ensure_ascii=False).encode("utf-8")
    if isinstance(v, str):
        return v.encode("utf-8")
    # fallback: JSON
    return json.dumps(v, ensure_ascii=False).encode("utf-8")


def make_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=_to_bytes,
        linger_ms=10,
        acks="all",
        retries=3,
        compression_type="gzip",
    )


def run() -> None:
    cfg = load_config()  # đọc từ configs/config.yaml + ${ENV}
    # ---- Lấy config cần thiết ----
    bootstrap = cfg.kafka.bootstrap_servers
    topic = cfg.kafka.topic_posts

    # Các trường dưới giả định bạn có dataclass RedditCfg trong config_types (client_id, client_secret, user_agent, subreddits, query, poll_interval)
    # Nếu tên khác, đổi lại cho khớp.
    rcfg = getattr(cfg, "reddit", None)
    if rcfg is None:
        raise ValueError(
            "Missing 'reddit' section in config. Please add it to configs/config.yaml"
        )

    client_id = rcfg.client_id
    client_secret = rcfg.client_secret
    user_agent = rcfg.user_agent or "stress-stream/0.1"
    subreddits = rcfg.subreddits or ["stress", "depression"]
    query = rcfg.query or "stress"
    poll_interval = getattr(rcfg, "poll_interval", 2)

    logger.info(
        f"Starting Reddit producer | bootstrap={bootstrap} | topic={topic} | subreddits={subreddits} | query={query}"
    )

    # ---- Reddit client ----
    reddit = praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent,
    )

    # ---- Kafka producer ----
    producer = make_producer(bootstrap)

    while True:
        try:
            for sub in subreddits:
                # Tìm bài mới theo query mỗi vòng lặp
                for post in reddit.subreddit(sub).search(query, sort="new", limit=10):
                    logger.debug(f"Post: {post.id} | {post.title} | {post.created_utc}")

                    rp = RedditPost(
                        post_id=post.id,
                        author=str(post.author) if post.author else None,
                        subreddit=str(post.subreddit),
                        created_utc=float(post.created_utc),
                        title=post.title or "",
                        selftext=post.selftext or "",
                        permalink=getattr(post, "permalink", None),
                        upvotes=getattr(post, "ups", 0),  # map 'ups' -> 'upvotes'
                        num_comments=getattr(post, "num_comments", 0),
                    )
                    producer.send(topic, rp)  # value_serializer sẽ xử lý thành bytes
            producer.flush()

            logger.info(
                f"Sent posts to Kafka topic {topic}. Sleeping {poll_interval}s..."
            )

            time.sleep(int(poll_interval))
        except Exception as e:
            logger.exception(e)
            time.sleep(5)


if __name__ == "__main__":
    run()
