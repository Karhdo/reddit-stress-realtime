from dataclasses import dataclass


@dataclass
class MinioCfg:
    endpoint_url: str = "http://localhost:9000"
    access_key: str = "minio"
    secret_key: str = "minio123"
    bucket: str = "datalake"
