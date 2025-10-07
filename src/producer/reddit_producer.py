import json
import time
from typing import Any, Iterable

import praw
from kafka import KafkaProducer
from loguru import logger

from src.common.config import load_config
from src.common.schema import RedditPost


def _to_bytes(v: Any) -> bytes:
    """Normalize messages to bytes for Kafka."""
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
    return json.dumps(v, ensure_ascii=False).encode("utf-8")


def make_producer(bootstrap_servers: str) -> KafkaProducer:
    """Build a KafkaProducer tuned for low-latency small batches."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=_to_bytes,
        linger_ms=10,
        acks="all",
        retries=3,
        compression_type="gzip",
    )


def _iter_posts(reddit: praw.Reddit, subreddits: Iterable[str], query: str):
    """Yield posts from multiple subreddits using a simple search strategy."""
    for sub in subreddits:
        for post in reddit.subreddit(sub).search(query, sort="new", limit=10):
            yield post


def run() -> None:
    """
    Stream Reddit posts to Kafka.

    Config expects:
    - cfg.kafka.bootstrap_servers, cfg.kafka.topic_posts
    - cfg.reddit.client_id, client_secret, user_agent, subreddits, query, poll_interval
    """
    cfg = load_config()

    bootstrap = cfg.kafka.bootstrap_servers
    topic = cfg.kafka.topic_posts
    rcfg = getattr(cfg, "reddit", None)
    if rcfg is None:
        raise ValueError("Missing 'reddit' section in config.yaml")

    reddit = praw.Reddit(
        client_id=rcfg.client_id,
        client_secret=rcfg.client_secret,
        user_agent=rcfg.user_agent or "stress-stream/0.1",
    )
    producer = make_producer(bootstrap)

    subreddits = rcfg.subreddits or ["stress", "depression"]
    query = rcfg.query or "stress"
    poll_interval = int(getattr(rcfg, "poll_interval", 2))

    logger.info(
        f"Starting Reddit producer | bootstrap={bootstrap} | topic={topic} | "
        f"subreddits={subreddits} | query={query}"
    )

    while True:
        try:
            sent = 0
            for post in _iter_posts(reddit, subreddits, query):
                logger.debug(f"Post: {post.id} | {post.title} | {post.created_utc}")

                msg = RedditPost(
                    post_id=post.id,
                    author=str(post.author) if post.author else None,
                    subreddit=str(post.subreddit),
                    created_utc=float(getattr(post, "created_utc", 0.0)),
                    title=post.title or "",
                    selftext=post.selftext or "",
                    permalink=getattr(post, "permalink", None),
                    upvotes=int(getattr(post, "ups", 0)),
                    num_comments=int(getattr(post, "num_comments", 0)),
                )
                producer.send(topic, msg)
                sent += 1

            producer.flush()
            logger.info(
                f"Sent {sent} post(s) to Kafka topic '{topic}'. Sleeping {poll_interval}s..."
            )
            time.sleep(poll_interval)

        except Exception as e:
            logger.exception(e)
            time.sleep(5)


if __name__ == "__main__":
    run()
