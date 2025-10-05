from dataclasses import dataclass, field
from typing import Dict


@dataclass
class SparkCfg:
    master: str = "local[*]"
    shuffle_partitions: int = 4
    extra_conf: Dict[str, str] = field(
        default_factory=lambda: {
            "spark.jars.packages": (
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.767"
            ),
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.hadoop.fs.s3a.endpoint": "http://localhost:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        }
    )
