# db/duck.py
from __future__ import annotations
from pathlib import Path
import duckdb
import os

SCHEMA_DIR = Path(__file__).resolve().parents[1] / "schema"

def connect(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    """
    Open (or create) a DuckDB database file.
    """
    db_path = db_path or os.environ.get("DUCKDB_PATH", "data/musiccap.duckdb")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)
    # Optional: tune threads
    con.execute("PRAGMA threads=4")
    return con

def run_sql_file(con: duckdb.DuckDBPyConnection, path: str | Path) -> None:
    """
    Execute a .sql file if it exists (idempotent-friendly).
    """
    p = Path(path)
    if not p.exists():
        return
    con.execute(p.read_text())

def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Load base DDL, then DuckDB-specific views & seeds.
    Safe to call repeatedly.
    """
    run_sql_file(con, SCHEMA_DIR / "ddl.sql")
    run_sql_file(con, SCHEMA_DIR / "views.duckdb.sql")
    run_sql_file(con, SCHEMA_DIR / "seeds.duckdb.sql")

def show_tables(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """
    Convenience for quick verification.
    """
    return con.execute("SHOW TABLES").fetchall()

def upsert_df(con: duckdb.DuckDBPyConnection, table: str, df) -> None:
    """
    Naive 'insert or replace' into a table from a pandas DataFrame.
    Table must already exist and column names must match.
    """
    con.register("tmp_df", df)
    con.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM tmp_df;")
    con.unregister("tmp_df")

    
