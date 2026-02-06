from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional

import logging

import polars as pl
from github import Github
from github.PullRequest import PullRequest
from github.Repository import Repository

from .config import AppConfig, DEFAULT_CONFIG


@dataclass(frozen=True)
class FetchOptions:
    orgs: list[str]
    repositories: list[str]
    lookback_days: int


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _should_stop(pr: PullRequest, cutoff: datetime) -> bool:
    updated_at = pr.updated_at.replace(tzinfo=timezone.utc) if pr.updated_at else None
    created_at = pr.created_at.replace(tzinfo=timezone.utc) if pr.created_at else None
    merged_at = pr.merged_at.replace(tzinfo=timezone.utc) if pr.merged_at else None
    if updated_at and updated_at >= cutoff:
        return False
    if created_at and created_at >= cutoff:
        return False
    if merged_at and merged_at >= cutoff:
        return False
    return True


def _iter_repos(org, repo_names: list[str]) -> Iterable[Repository]:
    if not repo_names:
        yield from org.get_repos(type="all")
        return

    for name in repo_names:
        try:
            yield org.get_repo(name)
        except Exception:  # noqa: BLE001
            logging.warning("Repo %s not found in org %s. Skipping.", name, org.login)


def _review_requested_at(repo: Repository, pr: PullRequest) -> Optional[datetime]:
    try:
        issue = repo.get_issue(pr.number)
        events = issue.get_events()
    except Exception:  # noqa: BLE001
        return None
    timestamps = []
    for event in events:
        if getattr(event, "event", None) == "review_requested":
            if event.created_at:
                timestamps.append(event.created_at.replace(tzinfo=timezone.utc))
    return min(timestamps) if timestamps else None


def _first_reviewed_at(pr: PullRequest, requested_at: Optional[datetime]) -> Optional[datetime]:
    try:
        reviews = pr.get_reviews()
    except Exception:  # noqa: BLE001
        return None

    candidates: list[datetime] = []
    for review in reviews:
        state = getattr(review, "state", None)
        submitted_at = getattr(review, "submitted_at", None)
        if state not in {"COMMENTED", "APPROVED"}:
            continue
        if not submitted_at:
            continue
        ts = submitted_at.replace(tzinfo=timezone.utc)
        if requested_at and ts < requested_at:
            continue
        candidates.append(ts)

    return min(candidates) if candidates else None


def _is_bot(login: Optional[str], user_type: Optional[str]) -> bool:
    if user_type == "Bot":
        return True
    if login and login.endswith("[bot]"):
        return True
    return False


def fetch_prs(gh: Github, options: FetchOptions) -> pl.DataFrame:
    cutoff = _utc_now() - timedelta(days=options.lookback_days)
    rows: list[dict] = []

    repo_map = _map_repos_by_org(options.orgs, options.repositories)

    for org_name in options.orgs:
        org = gh.get_organization(org_name)
        repo_names = repo_map.get(org_name, [])
        for repo in _iter_repos(org, repo_names):
            pulls = repo.get_pulls(state="all", sort="updated", direction="desc")
            for pr in pulls:
                if _should_stop(pr, cutoff):
                    break

                author = pr.user.login if pr.user else "unknown"
                is_bot = _is_bot(author, getattr(pr.user, "type", None))
                created_at = (
                    pr.created_at.replace(tzinfo=timezone.utc) if pr.created_at else None
                )
                merged_at = pr.merged_at.replace(tzinfo=timezone.utc) if pr.merged_at else None

                review_requested_at = _review_requested_at(repo, pr)
                first_reviewed_at = _first_reviewed_at(pr, review_requested_at)

                rows.append(
                    {
                        "org": org_name,
                        "author": author,
                        "repository": repo.full_name,
                        "number": pr.number,
                        "created_at": created_at,
                        "merged_at": merged_at,
                        "review_requested_at": review_requested_at,
                        "first_reviewed_at": first_reviewed_at,
                        "additions": pr.additions,
                        "deletions": pr.deletions,
                        "is_fork": repo.fork,
                        "is_archived": repo.archived,
                        "is_bot": is_bot,
                        "html_url": pr.html_url,
                    }
                )

    return pl.DataFrame(rows)


def fetch_team_mapping(
    gh: Github, org_names: list[str], team_field: str = "slug"
) -> pl.DataFrame:
    rows: list[dict] = []
    for org_name in org_names:
        org = gh.get_organization(org_name)
        for team in org.get_teams():
            team_value = team.slug if team_field == "slug" else team.name
            for member in team.get_members():
                rows.append({"org": org_name, "author": member.login, "team": team_value})
    return pl.DataFrame(rows)


def _map_repos_by_org(org_names: list[str], repo_args: list[str]) -> dict[str, list[str]]:
    if not repo_args:
        return {org_name: [] for org_name in org_names}

    full_name_map: dict[str, list[str]] = {org_name: [] for org_name in org_names}
    plain_names: list[str] = []
    for value in repo_args:
        if "/" in value:
            org_name, repo_name = value.split("/", 1)
            if org_name not in org_names:
                raise ValueError(
                    f"Repo {value} does not match provided orgs: {', '.join(org_names)}"
                )
            full_name_map.setdefault(org_name, []).append(repo_name)
        else:
            plain_names.append(value)

    if plain_names:
        for org_name in org_names:
            full_name_map.setdefault(org_name, []).extend(plain_names)

    return full_name_map


def write_outputs(prs_df: pl.DataFrame, teams_df: Optional[pl.DataFrame], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    prs_path = out_dir / "prs.ipc"
    prs_df.write_ipc(prs_path)

    if teams_df is not None:
        teams_path = out_dir / "teams.csv"
        teams_df.write_csv(teams_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch GitHub PR data for dashboard.")
    parser.add_argument(
        "--org",
        default="",
        help="GitHub organization name (or comma-separated list).",
    )
    parser.add_argument(
        "--orgs",
        default="",
        help="Comma-separated GitHub organizations (preferred for multi-org).",
    )
    parser.add_argument(
        "--repos",
        default="",
        help=(
            "Comma-separated repo names. Use repo or org/repo. "
            "Plain repo names are applied to all orgs."
        ),
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=DEFAULT_CONFIG.lookback_days,
        help="Lookback window in days.",
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[2] / "data"),
        help="Output directory for prs.ipc and teams.csv",
    )
    parser.add_argument(
        "--no-teams",
        action="store_true",
        help="Skip fetching team mapping.",
    )
    parser.add_argument(
        "--team-field",
        choices=["slug", "name"],
        default="slug",
        help="Team identifier to store in teams.csv.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        import os

        token = os.environ.get("GITHUB_TOKEN")
    except Exception:  # noqa: BLE001
        token = None

    if not token:
        raise SystemExit("Missing GITHUB_TOKEN environment variable.")

    gh = Github(token, per_page=100)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    orgs = [name.strip() for name in args.orgs.split(",") if name.strip()]
    if args.org:
        orgs.extend([name.strip() for name in args.org.split(",") if name.strip()])
    orgs = [org for org in orgs if org]
    if not orgs:
        raise SystemExit("Provide at least one org via --org or --orgs.")

    repos = [name.strip() for name in args.repos.split(",") if name.strip()]
    options = FetchOptions(
        orgs=orgs,
        repositories=repos,
        lookback_days=args.lookback,
    )

    prs_df = fetch_prs(gh, options)

    teams_df = None
    if not args.no_teams:
        teams_df = fetch_team_mapping(gh, orgs, args.team_field)

    write_outputs(prs_df, teams_df, Path(args.output))


if __name__ == "__main__":
    main()
