from dataclasses import dataclass
from src.common.config_types.postgres_config import PostgresCfg


@dataclass
class SinkCfg:
    bronze_prefix: str = "bronze/reddit_posts"
    silver_prefix: str = "silver/reddit_posts"
    gold_prefix: str = "gold/reddit_posts"

    # Target backend: "delta" | "postgres"
    gold_backend: str = "delta"

    def is_delta(self) -> bool:
        return self.gold_backend.lower() == "delta"

    def is_postgres(self) -> bool:
        return self.gold_backend.lower() == "postgres"

    def jdbc_url(self, pg: PostgresCfg) -> str:
        if not self.is_postgres():
            raise ValueError("jdbc_url is only applicable for Postgres sink.")
        host = (
            pg.host
        )  # 'localhost' when used locally (make), 'postgres' in Docker Compose
        return f"jdbc:postgresql://{host}:{pg.port}/{pg.database}?sslmode=disable"
