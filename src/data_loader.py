from __future__ import annotations

from pathlib import Path
from typing import Optional

import polars as pl


DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def load_prs(path: Optional[Path] = None) -> pl.DataFrame:
    prs_path = path or (DATA_DIR / "prs.ipc")
    if not prs_path.exists():
        raise FileNotFoundError(f"PR dataset not found: {prs_path}")
    return pl.read_ipc(prs_path)


def load_teams(path: Optional[Path] = None) -> Optional[pl.DataFrame]:
    teams_path = path or (DATA_DIR / "teams.csv")
    if not teams_path.exists():
        return None
    return pl.read_csv(teams_path)
