from pyspark.sql import SparkSession
from src.common.config import load_config
from src.common.logging_utils import get_logger

from src.streaming.stream_bronze import stream_bronze
from src.streaming.stream_silver import stream_silver
from src.streaming.stream_gold import stream_gold

log = get_logger(__name__)


def build_spark(cfg) -> SparkSession:
    """Build SparkSession with app/spark configs."""
    b = SparkSession.builder.appName(f"{cfg.app.name}-{cfg.app.mode}").master(
        cfg.spark.master
    )
    for k, v in cfg.spark.extra_conf.items():
        b = b.config(k, v)
    b = b.config("spark.sql.shuffle.partitions", cfg.spark.shuffle_partitions)
    return b.getOrCreate()


def main():
    cfg = load_config()
    spark = build_spark(cfg)

    log.info(f"Running mode = {cfg.app.mode}")
    log.info(
        f"Kafka bootstrap = {cfg.kafka.bootstrap_servers} | topic = {cfg.kafka.topic_posts}"
    )

    if cfg.app.mode == "bronze":
        stream_bronze(spark, cfg)
    elif cfg.app.mode == "silver":
        stream_silver(spark, cfg)
    elif cfg.app.mode == "gold":
        stream_gold(spark, cfg)
    else:
        raise ValueError("app.mode must be bronze|silver|gold")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
