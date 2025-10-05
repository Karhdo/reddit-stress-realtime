import os
import re
import yaml
from pathlib import Path
from typing import Any, Dict
from dataclasses import dataclass, field

from dotenv import load_dotenv

from src.common.config_types.kafka_config import KafkaCfg
from src.common.config_types.minio_config import MinioCfg
from src.common.config_types.sink_config import SinkCfg
from src.common.config_types.spark_config import SparkCfg
from src.common.config_types.app_config import AppCfg
from src.common.config_types.reddit_config import RedditCfg
from src.common.config_types.model_config import ModelCfg

load_dotenv()


@dataclass
class Config:
    kafka: KafkaCfg = field(default_factory=KafkaCfg)
    minio: MinioCfg = field(default_factory=MinioCfg)
    sink: SinkCfg = field(default_factory=SinkCfg)
    spark: SparkCfg = field(default_factory=SparkCfg)
    reddit: RedditCfg = field(default_factory=RedditCfg)
    model: ModelCfg = field(default_factory=ModelCfg)
    app: AppCfg = field(default_factory=AppCfg)


_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(?::-(.*?)?)?\}")


def _expand_env(val: str) -> str:
    def repl(m):
        key = m.group(1)
        default = m.group(2)
        if key in os.environ:
            return os.environ[key]
        return default if default is not None else m.group(0)

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
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    raw = p.read_text(encoding="utf-8")
    data: Dict[str, Any] = yaml.safe_load(raw) or {}
    expanded = _walk(data)

    # Parsing order: kafka, minio, sink, spark, app
    kafka = KafkaCfg(**(expanded.get("kafka", {}) or {}))
    minio = MinioCfg(**(expanded.get("minio", {}) or {}))
    sink = SinkCfg(**(expanded.get("sink", {}) or {}))
    spark = SparkCfg(**(expanded.get("spark", {}) or {}))
    reddit = RedditCfg(**(expanded.get("reddit", {}) or {}))
    app = AppCfg(**(expanded.get("app", {}) or {}))
    model = ModelCfg(**(expanded.get("model", {}) or {}))

    return Config(
        kafka=kafka,
        minio=minio,
        sink=sink,
        spark=spark,
        reddit=reddit,
        model=model,
        app=app,
    )
