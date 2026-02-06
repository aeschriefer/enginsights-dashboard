from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl

from src.config import AppConfig
from src.summary_engine import ScopeSelection, SummaryEngine


def _base_row(**overrides):
    base = {
        "author": "alice",
        "repository": "org/repo",
        "created_at": datetime(2026, 2, 1, tzinfo=timezone.utc),
        "merged_at": datetime(2026, 2, 2, tzinfo=timezone.utc),
        "review_requested_at": datetime(2026, 2, 1, 12, tzinfo=timezone.utc),
        "first_reviewed_at": datetime(2026, 2, 1, 14, tzinfo=timezone.utc),
        "additions": 10,
        "deletions": 5,
        "is_fork": False,
        "is_archived": False,
        "is_bot": False,
    }
    base.update(overrides)
    return base


def test_filters_and_scope():
    now = datetime(2026, 2, 6, tzinfo=timezone.utc)
    config = AppConfig(lookback_days=5)

    rows = [
        _base_row(author="alice"),
        _base_row(author="bot", is_bot=True),
        _base_row(author="forked", is_fork=True),
        _base_row(
            author="old",
            created_at=now - timedelta(days=10),
            merged_at=now - timedelta(days=9),
        ),
    ]
    df = pl.DataFrame(rows)
    engine = SummaryEngine(df, config=config, now=now)

    scoped = engine.scoped_df(ScopeSelection(scope="org"))
    assert scoped.height == 1
    assert scoped.select(pl.col("author")).to_series().to_list() == ["alice"]


def test_median_lead_time_and_review_latency():
    now = datetime(2026, 2, 6, tzinfo=timezone.utc)
    config = AppConfig(lookback_days=30)

    rows = [
        _base_row(
            created_at=now - timedelta(days=3),
            merged_at=now - timedelta(days=3) + timedelta(hours=10),
            review_requested_at=now - timedelta(days=3) + timedelta(hours=1),
            first_reviewed_at=now - timedelta(days=3) + timedelta(hours=3),
        ),
        _base_row(
            created_at=now - timedelta(days=2),
            merged_at=now - timedelta(days=2) + timedelta(hours=20),
            review_requested_at=now - timedelta(days=2) + timedelta(hours=2),
            first_reviewed_at=now - timedelta(days=2) + timedelta(hours=6),
        ),
    ]

    df = pl.DataFrame(rows)
    engine = SummaryEngine(df, config=config, now=now)
    scoped = engine.scoped_df(ScopeSelection(scope="org"))
    agg = engine.aggregate(scoped, None)
    row = agg.row(0, named=True)

    assert row["lead_time_median_hrs"] == 15.0
    assert row["review_latency_median_hrs"] == 3.0


def test_pr_size_classes():
    now = datetime(2026, 2, 6, tzinfo=timezone.utc)
    rows = [
        _base_row(additions=10),
        _base_row(additions=100),
        _base_row(additions=400),
    ]
    df = pl.DataFrame(rows)
    engine = SummaryEngine(df, config=AppConfig(lookback_days=30), now=now)
    scoped = engine.scoped_df(ScopeSelection(scope="org"))
    agg = engine.aggregate(scoped, None)
    row = agg.row(0, named=True)

    assert row["prs_small"] == 1
    assert row["prs_medium"] == 1
    assert row["prs_large"] == 1


def test_team_join_uses_org_when_present():
    now = datetime(2026, 2, 6, tzinfo=timezone.utc)
    rows = [
        _base_row(org="org-a", author="alice", repository="org-a/repo"),
        _base_row(org="org-b", author="alice", repository="org-b/repo"),
    ]
    prs_df = pl.DataFrame(rows)
    teams_df = pl.DataFrame(
        [
            {"org": "org-a", "author": "alice", "team": "alpha"},
            {"org": "org-b", "author": "alice", "team": "beta"},
        ]
    )

    engine = SummaryEngine(prs_df, team_df=teams_df, config=AppConfig(30), now=now)
    scoped = engine.scoped_df(ScopeSelection(scope="org"))

    assert scoped.height == 2
    teams = scoped.select(["org", "team"]).sort("org").to_dict(as_series=False)
    assert teams == {"org": ["org-a", "org-b"], "team": ["alpha", "beta"]}
