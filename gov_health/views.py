from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from gov_health.config import SOURCE_DATA_DIR
from gov_health.db import register_source_tables
from gov_health.kpis import ALL_KPI_VIEWS
from gov_health.kpis.chain_dashboard import CHAIN_DASHBOARD_VIEWS


# Epoch-partitioned datasets use hive partitioning glob
EPOCH_PARTITIONED = [
    "drep_epoch_stats",
    "pool_epoch_stats",
    "gov_action_votes",
]

# Single-file datasets
SINGLE_FILE = [
    "gov_action_lifecycle",
    "epoch_summary",
    "delegation_events",
    "cc_vote_details",
    "governance_params",
]


def create_views(*, parquet_dir: str = "output", db_path: str = "output/governance.duckdb"):
    parquet = Path(parquet_dir).resolve()
    conn = duckdb.connect(db_path)

    # Register source tables (needed by chain dashboard views)
    register_source_tables(conn, SOURCE_DATA_DIR)
    print("  registered source tables")

    # Materialize chain dashboard views as parquet files first
    _materialize_chain_dashboard(conn, parquet)

    for name in EPOCH_PARTITIONED:
        ds_path = parquet / name / "*.parquet"
        conn.execute(f"""
            CREATE OR REPLACE VIEW {name} AS
            SELECT * FROM read_parquet('{ds_path}', hive_partitioning=true)
        """)
        print(f"  view: {name} (hive-partitioned)")

    for name in SINGLE_FILE:
        ds_path = parquet / f"{name}.parquet"
        if ds_path.exists():
            conn.execute(f"""
                CREATE OR REPLACE VIEW {name} AS
                SELECT * FROM read_parquet('{ds_path}')
            """)
            print(f"  view: {name}")
        else:
            print(f"  skip: {name} (file not found)")

    for name, sql in ALL_KPI_VIEWS:
        # Chain dashboard views are now backed by parquet — re-point them
        if any(name == vn for vn, _ in CHAIN_DASHBOARD_VIEWS):
            ds_path = parquet / f"{name}.parquet"
            conn.execute(f"""
                CREATE OR REPLACE VIEW {name} AS
                SELECT * FROM read_parquet('{ds_path}')
            """)
            print(f"  view: {name} (materialized)")
        else:
            conn.execute(sql)
            print(f"  view: {name} (kpi)")

    conn.close()
    print(f"DuckDB database: {db_path}")


def _materialize_chain_dashboard(conn, parquet: Path):
    """Execute each chain dashboard view SQL and write results to parquet."""
    for name, create_sql in CHAIN_DASHBOARD_VIEWS:
        print(f"  materializing {name}...")
        # Create the view temporarily to query it
        conn.execute(create_sql)
        table = conn.execute(f"SELECT * FROM {name}").fetch_arrow_table()
        out_path = parquet / f"{name}.parquet"
        pq.write_table(table, out_path)
        print(f"  materialized: {name} ({table.num_rows} rows → {out_path.name})")
