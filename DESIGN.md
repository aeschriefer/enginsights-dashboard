# GitHub Engineering Effectiveness Dashboard (V2)

## 1. Vision & Hierarchical Scope
Goal: Provide a tiered view of engineering health.

Aggregation Levels:
- Org Level: High-level trends for the entire corporate entity.
- Team Level: Grouped by GitHub Teams (requires read:org permissions).
- Individual Level: Drill-down into a specific developer’s flow and workload.

## 2. Updated Metrics (The "Flow & Health" Framework)

| Category | Metric | Polars Transformation Logic |
| --- | --- | --- |
| Throughput | Total PRs Merged | `df.filter(pl.col("merged_at").is_not_null()).len()` |
| Velocity | PR Lead Time (Median) | `median((pl.col("merged_at") - pl.col("created_at")).dt.total_minutes() / 60)` |
| Flow | Review Latency | Delta between review_requested and first COMMENTED/APPROVED event. |
| Quality | Code Churn | `(pl.col("deletions") / (pl.col("additions") + 1))` over a rolling window. |
| Efficiency | PR Size Class | `pl.when(pl.col("additions") < 50).then("Small")...` |

## 3. Aggregation & Filtering Logic
The application must support a "View Mode" toggle in the sidebar. The agent should implement the following Polars logic for dynamic switching:

- Individual View:
  - `df.filter(pl.col("author") == selected_user)`
- Team View:
  - Requires a mapping file (e.g., `teams.csv`) or API call to fetch team members.
  - The agent should join the PR data with the team mapping:
    - `df.join(team_df, on="author").filter(pl.col("team") == selected_team)`
- Org View:
  - The default state where `group_by("team")` or `group_by("repository")` is used to show comparative bar charts.

Global Filters and Defaults:
- Lookback Window: 180 days (default).
- Exclusions: PRs from forks or archived repositories are excluded.
- Exclusions: Bot-authored PRs are excluded.
- Reverts/Backports: No special handling; they are not explicitly identified or removed.
- Update Cadence: On-demand refresh.

## 4. Technical Instructions for the AI Agent

Technical Guardrails:
- Dynamic Grouping: Use a single helper function for aggregations that accepts a `group_by_col` parameter (e.g., `"author"`, `"team"`, or `"repo"`).
- Polars `agg()` Pattern: Standardize metrics into a single `.group_by().agg()` chain to maximize parallel execution.
- Dash Callbacks: Use the Input of a Dropdown (User/Team/Org) to update the `group_by` key in the Polars query.
- No Row-Wise Processing: Strictly forbid `.apply()` or Python loops. Use Polars native expressions for all date and math calculations.
- Multi-Tab Layout: Use `dcc.Tabs` to separate "Executive Summary" (Org), "Team Comparison," and "Contributor Deep-Dive."

## 5. Visual Layout Sketch

## 6. Implementation Phases

Phase 1 (Data): Fetch PRs and associate them with Teams (via GitHub Team API). Save to Polars `.ipc`.

Phase 2 (Analytics): Build a `SummaryEngine` class that uses Polars to return aggregated dataframes based on the selected "Scope" (User/Team/Org).

Phase 3 (UI): Implement a "Searchable Select" for Users/Teams and high-level KPI cards for "Total Merged PRs."
