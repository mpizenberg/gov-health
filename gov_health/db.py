from pathlib import Path

import duckdb

from gov_health.config import SOURCE_DATA_DIR


def get_connection() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with all source parquet tables registered as views."""
    conn = duckdb.connect()
    register_source_tables(conn, SOURCE_DATA_DIR)
    return conn


def register_source_tables(conn: duckdb.DuckDBPyConnection, source_dir: str):
    """Register each subdirectory of source_dir as a DuckDB view over its parquet files."""
    src = Path(source_dir)
    for table_dir in sorted(src.iterdir()):
        if not table_dir.is_dir():
            continue
        name = table_dir.name
        # Check if hive-partitioned (has subdirectories) or flat
        has_subdirs = any(d.is_dir() for d in table_dir.iterdir())
        if has_subdirs:
            conn.execute(f"""
                CREATE OR REPLACE VIEW {name} AS
                SELECT * FROM read_parquet('{table_dir}/**/*.parquet', hive_partitioning=true)
            """)
        else:
            parquet_files = list(table_dir.glob("*.parquet"))
            if parquet_files:
                conn.execute(f"""
                    CREATE OR REPLACE VIEW {name} AS
                    SELECT * FROM read_parquet('{table_dir}/*.parquet')
                """)


def get_settled_epochs(conn: duckdb.DuckDBPyConnection) -> list[int]:
    """Return all epochs present in adapot (proxy for settled epochs)."""
    rows = conn.execute(
        "SELECT DISTINCT epoch FROM adapot ORDER BY epoch"
    ).fetchall()
    return [int(r[0]) for r in rows]


def get_max_epoch(conn: duckdb.DuckDBPyConnection) -> int:
    """Return the highest epoch number in the source data."""
    row = conn.execute("SELECT MAX(epoch) FROM adapot").fetchone()
    return int(row[0])


def get_conway_start_epoch(conn: duckdb.DuckDBPyConnection) -> int:
    """Return the first epoch that has governance data (drep_dist rows)."""
    row = conn.execute("SELECT MIN(epoch) FROM drep_dist").fetchone()
    return int(row[0]) if row and row[0] is not None else 0
