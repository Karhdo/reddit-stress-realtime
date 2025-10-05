from dataclasses import dataclass
from typing import Literal

ModeType = Literal["bronze", "silver", "gold"]


@dataclass
class AppCfg:
    name: str = "reddit-pipeline"
    mode: ModeType = "bronze"
    checkpoint_base: str = "s3a://datalake/_checkpoints"
