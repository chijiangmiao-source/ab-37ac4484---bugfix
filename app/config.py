from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    # Maximum seconds a concurrent chunk upload waits for an in-flight upload
    # of the same chunk before it receives a structured in-progress response.
    inflight_wait_timeout: float = 30.0

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            data_dir=Path(os.environ.get("DATA_DIR", "./data")).resolve(),
            inflight_wait_timeout=float(os.environ.get("INFLIGHT_WAIT_TIMEOUT", "30")),
        )
