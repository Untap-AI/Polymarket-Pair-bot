"""DuckDB connection helper for ad-hoc analysis against Parquet exports.

Usage:
    from duckdb_conn import get_connection, query, adapt_sql_for_duckdb

    conn = get_connection()
    rows = query(conn, "SELECT COUNT(*) AS n FROM Attempts")
    print(rows[0]["n"])

The returned connection has a view named ``Attempts`` (case-insensitive)
that reads all Parquet files under data/parquet/attempts/**/*.parquet
using hive-style date partitioning.

Export Parquet files first with:
    python scripts/export_to_parquet.py --days 30
"""

from __future__ import annotations

import os
import re
from typing import Any

_DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "parquet",
)


def get_connection(data_dir: str | None = None):
    """Return a DuckDB connection with ``Attempts`` and ``Ticks`` views.

    Args:
        data_dir: Root of the parquet export tree.  Defaults to
                  <repo>/data/parquet.

    Raises:
        ImportError:  duckdb is not installed.
        FileNotFoundError: No Parquet files found under data_dir.
    """
    import duckdb  # lazy import so the rest of the repo doesn't require it

    root = data_dir or _DATA_DIR
    pattern = os.path.join(root, "attempts", "**", "*.parquet")
    # Normalise to forward slashes for DuckDB glob
    pattern = pattern.replace("\\", "/")

    conn = duckdb.connect(":memory:")
    conn.execute(f"""
        CREATE VIEW Attempts AS
        SELECT * FROM read_parquet('{pattern}', hive_partitioning = true)
    """)

    # --- Ticks view: S3-backed or local fallback ---
    _setup_ticks_view(conn)

    return conn


def _setup_ticks_view(conn) -> None:
    """Create a Ticks view from S3 (preferred) or local Parquet files."""
    bucket = os.environ.get("TICK_S3_BUCKET")
    prefix = os.environ.get("TICK_S3_PREFIX", "ticks")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if bucket and key and secret:
        try:
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            conn.execute(f"SET s3_region='{region}';")
            conn.execute(f"SET s3_access_key_id='{key}';")
            conn.execute(f"SET s3_secret_access_key='{secret}';")
            s3_path = f"s3://{bucket}/{prefix}/**/*.parquet"
            conn.execute(f"""
                CREATE VIEW IF NOT EXISTS Ticks AS
                SELECT * FROM read_parquet('{s3_path}', hive_partitioning = true)
            """)
            return
        except Exception as e:
            import warnings
            warnings.warn(f"S3 Ticks view failed, trying local fallback: {e}")

    # Local fallback: look for tick parquet files in data/parquet/ticks/
    local_pattern = os.path.join(_DATA_DIR, "ticks", "**", "*.parquet")
    local_pattern = local_pattern.replace("\\", "/")
    try:
        conn.execute(f"""
            CREATE VIEW IF NOT EXISTS Ticks AS
            SELECT * FROM read_parquet('{local_pattern}', hive_partitioning = true)
        """)
    except Exception:
        pass  # No tick data available yet — view won't exist


def query(conn, sql: str, params: list | None = None) -> list[dict[str, Any]]:
    """Execute *sql* on *conn* and return a list of dicts.

    Parameters use ``$1``/``$2`` style (same as asyncpg) or ``?`` style.
    DuckDB accepts both.
    """
    result = conn.execute(sql, params or [])
    cols = [desc[0] for desc in result.description]
    return [dict(zip(cols, row)) for row in result.fetchall()]


def adapt_sql_for_duckdb(sql: str) -> str:
    """Remove Postgres-specific partition-pruning hints from a SQL string.

    The Postgres write path adds ``AND ts >= $N::timestamp`` (and ``< $N``)
    clauses to exploit the Attempts partition key.  These columns don't exist
    in the Parquet export, so they must be stripped before running against
    DuckDB.

    The corresponding ``t1_timestamp`` range clause is always present too, so
    removing the ``ts`` hint does not change the query's logical result.
    """
    # Remove "AND ts >= $N::timestamp" and "AND ts < $N::timestamp"
    sql = re.sub(
        r"\s+AND\s+ts\s*>=\s*\$\d+::timestamp",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\s+AND\s+ts\s*<\s*\$\d+::timestamp",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    return sql
