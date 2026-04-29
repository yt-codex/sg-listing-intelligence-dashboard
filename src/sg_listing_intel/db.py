from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"Analytics database not found: {db_path}")
    uri = f"file:{db_path.resolve()}?mode=ro"
    return sqlite3.connect(uri, uri=True, check_same_thread=False)


def read_frame(con: sqlite3.Connection, sql: str, params: tuple = ()) -> pd.DataFrame:
    return pd.read_sql_query(sql, con, params=params)


def latest_week(con: sqlite3.Connection) -> str:
    row = con.execute("SELECT MAX(snapshot_week_id) FROM listing_week_panel").fetchone()
    if not row or row[0] is None:
        raise ValueError("No snapshot weeks found in analytics DB")
    return str(row[0])


def available_weeks(con: sqlite3.Connection) -> list[str]:
    rows = con.execute(
        "SELECT DISTINCT snapshot_week_id FROM listing_week_panel ORDER BY snapshot_week_id DESC"
    ).fetchall()
    return [str(row[0]) for row in rows]
