from __future__ import annotations

import json
import time
from typing import Any, Iterable, Set

import praw
from kafka import KafkaProducer
from loguru import logger

from src.common.config import load_config
from src.common.schema import RedditPost


# Helpers: normalize any message to Kafka bytes
def _to_bytes(v: Any) -> bytes:
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


# Build a Kafka producer (small batches, safe acks)
def make_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=_to_bytes,
        acks="all",
        linger_ms=10,
        retries=3,
        compression_type="gzip",
    )


# Simple iterator over subreddit search results
def _iter_posts(reddit: praw.Reddit, subreddits, query: str, limit: int):
    query = (query or "").strip()

    for sub in subreddits:
        sr = reddit.subreddit(sub)

        if query:
            generator = sr.search(query, sort="new", limit=limit)
        else:
            generator = sr.new(limit=limit)

        for post in generator:
            yield post


# Main loop: read from Reddit, send to Kafka, repeat
def run() -> None:
    # 1. Load config
    cfg = load_config()
    bootstrap = cfg.kafka.bootstrap_servers
    topic = cfg.kafka.topic_posts
    rcfg = cfg.reddit
    if rcfg is None:
        raise ValueError("Missing 'reddit' section in config.yaml")

    # 2. Build Reddit API client
    reddit = praw.Reddit(
        client_id=rcfg.client_id,
        client_secret=rcfg.client_secret,
        user_agent=rcfg.user_agent or "stress-stream/0.1",
    )

    # 3. Build Kafka producer
    producer = make_producer(bootstrap)

    # 4. Read runtime params
    subreddits = list(rcfg.subreddits or ["stress", "depression"])
    query = rcfg.query or ""
    poll_interval = int(getattr(rcfg, "poll_interval", 5))
    search_limit = int(getattr(rcfg, "search_limit", 25))

    logger.info(
        "Starting Reddit producer | bootstrap={} | topic={} | subreddits={} | query='{}' | limit={} | interval={}s",
        bootstrap,
        topic,
        subreddits,
        query,
        search_limit,
        poll_interval,
    )

    # 5. Dedup memory (avoid resending the same IDs repeatedly)
    seen: Set[str] = set()
    max_seen = 10_000

    try:
        while True:
            try:
                sent = 0
                for post in _iter_posts(reddit, subreddits, query, search_limit):
                    pid = getattr(post, "id", None)
                    if not pid or pid in seen:
                        continue

                    msg = RedditPost(
                        post_id=pid,
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
                    seen.add(pid)

                    # keep dedup set bounded
                    if len(seen) > max_seen:
                        # simple trimming: drop oldest chunk by recreating a small set
                        seen = set(list(seen)[-max_seen // 2 :])

                producer.flush()
                logger.info(
                    "Sent {} post(s) → topic='{}' | sleep {}s",
                    sent,
                    topic,
                    poll_interval,
                )
                time.sleep(poll_interval)

            except Exception as e:
                logger.exception("Producer loop error: {}", e)
                time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Stopping Reddit producer (KeyboardInterrupt).")

    finally:
        try:
            producer.flush(10)
            producer.close(10)
        except Exception:
            pass
        logger.info("Producer closed.")


if __name__ == "__main__":
    run()
