from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class AppConfig:
    lookback_days: int = 180
    exclude_forks: bool = True
    exclude_archived: bool = True
    exclude_bots: bool = True

    @property
    def lookback_delta(self) -> timedelta:
        return timedelta(days=self.lookback_days)


DEFAULT_CONFIG = AppConfig()
