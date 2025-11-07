import os
import re
import yaml
from pathlib import Path
from typing import Any
from dataclasses import dataclass, field

from dotenv import load_dotenv

from src.common.config_types.kafka_config import KafkaCfg
from src.common.config_types.minio_config import MinioCfg
from src.common.config_types.sink_config import SinkCfg
from src.common.config_types.spark_config import SparkCfg
from src.common.config_types.app_config import AppCfg
from src.common.config_types.reddit_config import RedditCfg
from src.common.config_types.model_config import ModelCfg
from src.common.config_types.postgres_config import PostgresCfg

load_dotenv()


@dataclass
class Config:
    kafka: KafkaCfg = field(default_factory=KafkaCfg)
    minio: MinioCfg = field(default_factory=MinioCfg)
    sink: SinkCfg = field(default_factory=SinkCfg)
    spark: SparkCfg = field(default_factory=SparkCfg)
    reddit: RedditCfg = field(default_factory=RedditCfg)
    model: ModelCfg = field(default_factory=ModelCfg)
    postgres: PostgresCfg = field(default_factory=PostgresCfg)
    app: AppCfg = field(default_factory=AppCfg)

    # Common S3/MinIO storage options for fsspec, s3fs, deltalake
    def s3_storage_options(self) -> dict:
        """
        Returns a unified S3/MinIO connection config usable for:
        - fsspec / s3fs
        - deltalake (Delta-RS)
        - PySpark (fs.s3a.* is configured separately)
        """
        endpoint = getattr(self.minio, "endpoint_url", None)
        key = getattr(self.minio, "access_key", None)
        secret = getattr(self.minio, "secret_key", None)

        verify = not endpoint.startswith("http://") if endpoint else True

        # Compatible with both fsspec and deltalake
        return {
            # ---- For fsspec/s3fs ----
            "anon": False,
            "key": key,
            "secret": secret,
            "client_kwargs": {
                "endpoint_url": endpoint,
                "verify": verify,
                "region_name": "us-east-1",
            },
        }

    def s3_storage_options_delta(self) -> dict:
        endpoint = self.minio.endpoint_url
        allow_http = "true" if endpoint.startswith("http://") else "false"
        return {
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ACCESS_KEY_ID": self.minio.access_key or "",
            "AWS_SECRET_ACCESS_KEY": self.minio.secret_key or "",
            "AWS_REGION": "us-east-1",
            "USE_S3_STATIC_CREDENTIALS": "true",
            "AWS_ALLOW_HTTP": allow_http,
            "SSL_VERIFY": "false" if allow_http == "true" else "true",
            "AWS_S3_LOCKING_PROVIDER": "none",
        }

    def s3_uri(self, key: str) -> str:
        """Helper to build full s3a:// URI inside your MinIO bucket."""
        bucket = getattr(self.minio, "bucket", "datalake")
        return f"s3a://{bucket}/{key.lstrip('/')}"


# YAML + ENV loader

_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(?::-(.*?)?)?\}")


def _expand_env(val: str) -> str:
    def repl(m):
        key = m.group(1)
        default = m.group(2)
        return os.environ.get(key, default or m.group(0))

    return _ENV_PATTERN.sub(repl, val)


def _walk(x: Any) -> Any:
    if isinstance(x, dict):
        return {k: _walk(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_walk(v) for v in x]
    if isinstance(x, str):
        return _expand_env(x)
    return x


def load_config(path: str = "configs/config.yaml") -> Config:
    """Load YAML + expand env vars, return Config dataclass."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    expanded = _walk(data)

    return Config(
        kafka=KafkaCfg(**(expanded.get("kafka", {}) or {})),
        minio=MinioCfg(**(expanded.get("minio", {}) or {})),
        sink=SinkCfg(**(expanded.get("sink", {}) or {})),
        spark=SparkCfg(**(expanded.get("spark", {}) or {})),
        reddit=RedditCfg(**(expanded.get("reddit", {}) or {})),
        model=ModelCfg(**(expanded.get("model", {}) or {})),
        postgres=PostgresCfg(**(expanded.get("postgres", {}) or {})),
        app=AppCfg(**(expanded.get("app", {}) or {})),
    )
