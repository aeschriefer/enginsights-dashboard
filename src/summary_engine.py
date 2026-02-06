from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import polars as pl

from .config import AppConfig, DEFAULT_CONFIG


REQUIRED_COLUMNS = {
    "author",
    "created_at",
    "merged_at",
    "review_requested_at",
    "first_reviewed_at",
    "additions",
    "deletions",
    "is_fork",
    "is_archived",
    "is_bot",
}


@dataclass(frozen=True)
class ScopeSelection:
    scope: str
    selected_user: Optional[str] = None
    selected_team: Optional[str] = None


class SummaryEngine:
    def __init__(
        self,
        prs_df: pl.DataFrame,
        team_df: Optional[pl.DataFrame] = None,
        config: AppConfig = DEFAULT_CONFIG,
        now: Optional[datetime] = None,
    ) -> None:
        self._raw_prs_df = prs_df
        self._team_df = team_df
        self._config = config
        self._now = now or datetime.now(timezone.utc)
        self._validate_schema(prs_df)

    def available_authors(self) -> list[str]:
        df = self._base_df()
        if "author" not in df.columns:
            return []
        return df.select(pl.col("author").unique().sort()).to_series().to_list()

    def available_teams(self) -> list[str]:
        df = self._base_df()
        if "team" not in df.columns:
            return []
        return df.select(pl.col("team").unique().sort()).to_series().to_list()

    def available_repos(self) -> list[str]:
        df = self._base_df()
        if "repository" not in df.columns:
            return []
        return df.select(pl.col("repository").unique().sort()).to_series().to_list()

    def scoped_df(self, selection: ScopeSelection) -> pl.DataFrame:
        df = self._base_df()
        if selection.scope == "individual":
            if not selection.selected_user:
                return df.head(0)
            return df.filter(pl.col("author") == selection.selected_user)
        if selection.scope == "team":
            if "team" not in df.columns:
                raise ValueError("Team data not available; provide teams.csv or team column.")
            if not selection.selected_team:
                return df.head(0)
            return df.filter(pl.col("team") == selection.selected_team)
        return df

    def aggregate(self, df: pl.DataFrame, group_by_col: Optional[str]) -> pl.DataFrame:
        if group_by_col is None:
            df = df.with_columns(pl.lit("all").alias("_scope"))
            group_by_col = "_scope"

        df = self._add_pr_size_class(df)
        lead_time_hours = (
            pl.when(pl.col("merged_at").is_not_null())
            .then((pl.col("merged_at") - pl.col("created_at")).dt.total_minutes() / 60)
            .otherwise(None)
        )
        review_latency_hours = (
            pl.when(
                pl.col("first_reviewed_at").is_not_null()
                & pl.col("review_requested_at").is_not_null()
            )
            .then(
                (pl.col("first_reviewed_at") - pl.col("review_requested_at")).dt.total_minutes()
                / 60
            )
            .otherwise(None)
        )

        return (
            df.group_by(group_by_col)
            .agg(
                pl.col("merged_at").is_not_null().sum().alias("total_merged_prs"),
                lead_time_hours.median().alias("lead_time_median_hrs"),
                review_latency_hours.median().alias("review_latency_median_hrs"),
                (pl.col("deletions") / (pl.col("additions") + 1)).mean().alias("code_churn_avg"),
                pl.len().alias("total_prs"),
                pl.col("pr_size_class").eq("Small").sum().alias("prs_small"),
                pl.col("pr_size_class").eq("Medium").sum().alias("prs_medium"),
                pl.col("pr_size_class").eq("Large").sum().alias("prs_large"),
            )
            .sort(group_by_col)
        )

    def _base_df(self) -> pl.DataFrame:
        df = self._raw_prs_df.clone()
        if "repository" not in df.columns and "repo" in df.columns:
            df = df.rename({"repo": "repository"})
        df = self._normalize_types(df)
        df = self._apply_filters(df)
        if "team" not in df.columns and self._team_df is not None:
            join_keys = ["author"]
            if "org" in df.columns and "org" in self._team_df.columns:
                join_keys = ["author", "org"]
            df = df.join(self._team_df, on=join_keys, how="left")
        return df

    def _normalize_types(self, df: pl.DataFrame) -> pl.DataFrame:
        datetime_cols = [
            "created_at",
            "merged_at",
            "review_requested_at",
            "first_reviewed_at",
        ]
        bool_cols = ["is_fork", "is_archived", "is_bot"]
        return df.with_columns(
            pl.col(datetime_cols).cast(pl.Datetime, strict=False),
            pl.col(bool_cols).cast(pl.Boolean, strict=False),
        )

    def _apply_filters(self, df: pl.DataFrame) -> pl.DataFrame:
        cutoff = (self._now - self._config.lookback_delta).replace(tzinfo=None)
        df = df.filter(
            pl.coalesce([pl.col("merged_at"), pl.col("created_at")]) >= cutoff
        )
        if self._config.exclude_forks:
            df = df.filter(~pl.col("is_fork"))
        if self._config.exclude_archived:
            df = df.filter(~pl.col("is_archived"))
        if self._config.exclude_bots:
            df = df.filter(~pl.col("is_bot"))
        return df

    @staticmethod
    def _add_pr_size_class(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(
            pl.when(pl.col("additions") < 50)
            .then(pl.lit("Small"))
            .when(pl.col("additions") < 300)
            .then(pl.lit("Medium"))
            .otherwise(pl.lit("Large"))
            .alias("pr_size_class")
        )

    @staticmethod
    def _validate_schema(df: pl.DataFrame) -> None:
        missing = REQUIRED_COLUMNS - set(df.columns)
        if "repository" not in df.columns and "repo" not in df.columns:
            missing.add("repository")
        if missing:
            missing_list = ", ".join(sorted(missing))
            raise ValueError(f"PR dataset missing required columns: {missing_list}")
