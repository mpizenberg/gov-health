import psycopg
from psycopg.rows import dict_row

from gov_health.config import (
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSCHEMA,
    SETTLEMENT_SECONDS,
)


def get_connection():
    conninfo = (
        f"host={PGHOST} port={PGPORT} dbname={PGDATABASE} "
        f"user={PGUSER} password={PGPASSWORD} "
        f"options=-csearch_path={PGSCHEMA},public"
    )
    return psycopg.connect(conninfo, row_factory=dict_row)


def get_settled_epochs(conn) -> list[int]:
    """Return epochs whose end_time is old enough to be considered immutable."""
    rows = conn.execute(
        "SELECT \"number\" FROM epoch "
        "WHERE end_time < EXTRACT(EPOCH FROM now())::bigint - %s "
        "ORDER BY \"number\"",
        [SETTLEMENT_SECONDS],
    ).fetchall()
    return [r["number"] for r in rows]


def get_max_epoch(conn) -> int:
    """Return the highest epoch number in the database."""
    row = conn.execute(
        "SELECT MAX(\"number\") AS max_epoch FROM epoch"
    ).fetchone()
    return row["max_epoch"]


def get_conway_start_epoch(conn) -> int:
    """Return the first epoch that has governance data (drep_dist rows)."""
    row = conn.execute(
        "SELECT MIN(epoch) AS min_epoch FROM drep_dist"
    ).fetchone()
    return row["min_epoch"] if row and row["min_epoch"] is not None else 0
