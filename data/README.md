# Data Inputs

Place datasets in this directory.

Required files:
- `prs.ipc`: Polars IPC file with pull request data.
- `teams.csv`: Optional mapping of authors to teams.

Required columns in `prs.ipc`:
- `author`
- `repository` (or `repo`)
- `org` (recommended for multi-org datasets)
- `created_at`
- `merged_at`
- `review_requested_at`
- `first_reviewed_at`
- `additions`
- `deletions`
- `is_fork`
- `is_archived`
- `is_bot`

Optional columns in `teams.csv`:
- `author`
- `team`
- `org` (recommended for multi-org datasets)

Notes:
- `review_requested_at` is derived from issue events (event = `review_requested`).
- `first_reviewed_at` is derived from the earliest review with state COMMENTED or APPROVED.
- When both `org` columns are present in `prs.ipc` and `teams.csv`, joins are done on (`author`, `org`).
