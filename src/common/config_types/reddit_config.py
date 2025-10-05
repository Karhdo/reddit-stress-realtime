from dataclasses import dataclass, field
from typing import List


@dataclass
class RedditCfg:
    client_id: str = ""
    client_secret: str = ""
    user_agent: str = "stress-stream/0.1"

    subreddits: List[str] = field(default_factory=lambda: ["stress", "depression"])
    query: str = "stress"

    poll_interval: int = 2
