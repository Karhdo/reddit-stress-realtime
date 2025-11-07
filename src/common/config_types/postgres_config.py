from dataclasses import dataclass


@dataclass
class PostgresCfg:
    host: str = "postgres"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = "postgres"
