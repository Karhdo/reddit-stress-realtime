from dataclasses import dataclass, field
from typing import List


@dataclass
class RedditCfg:
    client_id: str = ""
    client_secret: str = ""
    user_agent: str = "stress-stream/0.1"

    subreddits: List[str] = field(
        default_factory=lambda: [
            "Professors",
            "PhD",
            "GradSchool",
            "csMajors",
            "EngineeringStudents",
        ]
    )
    query: str = ""

    search_limit: int = 25
    poll_interval: int = 2
