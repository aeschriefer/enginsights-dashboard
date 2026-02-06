# Engineering Effectiveness Dashboard

## Quickstart
1. Add `data/prs.ipc` (and optional `data/teams.csv`).
2. Install deps: `uv sync`.
3. Run: `uv run python app.py`.

## Notes
- Default lookback is 180 days.
- PRs from forks, archived repos, and bot authors are excluded.
- When multiple orgs are fetched, user/team metrics aggregate across all included orgs.

## Fetch Data (PyGithub)
Set `GITHUB_TOKEN` with `read:org` and repo access, then run:

```bash
PYTHONPATH=src uv run python -m enginsights_dashboard.fetch_github_data --org YOUR_ORG --lookback 180
```

Optional:
- `--orgs org1,org2` to fetch multiple orgs in one run.
- `--repos repo1,repo2` to scope to specific repos.
- `--repos org1/repo1,org2/repo2` to target repos across orgs.
- `--no-teams` to skip team mapping.

## Metrics and Calculations

All calculations are done in Polars using vectorized expressions (no row-wise Python).

- Total Merged PRs:
  - Count of PRs where `merged_at` is not null.
  - Aggregation: sum of `merged_at.is_not_null()` in each group.

- PR Lead Time (Median, hours):
  - `(merged_at - created_at)` in hours.
  - Only PRs with `merged_at` are considered.
  - Aggregation: median across the group.

- Review Latency (Median, hours):
  - `(first_reviewed_at - review_requested_at)` in hours.
  - `review_requested_at` is the earliest issue event with `event = review_requested`.
  - `first_reviewed_at` is the earliest review with state `COMMENTED` or `APPROVED`,
    and must be on/after `review_requested_at` when present.
  - Aggregation: median across the group.

- Code Churn (Average):
  - `deletions / (additions + 1)` per PR.
  - Aggregation: mean across the group.

- PR Size Class:
  - Based on `additions`:
    - Small: `< 50`
    - Medium: `50–299`
    - Large: `>= 300`
  - Aggregation: counts per class (`prs_small`, `prs_medium`, `prs_large`).

## Tests
Run:

```bash
uv run pytest -q
```
