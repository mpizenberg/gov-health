from pathlib import Path


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
    try:
        import duckdb
    except ImportError:
        raise SystemExit(
            "duckdb is required for create-views. Install with: pip install 'gov-health[duckdb]'"
        )

    parquet = Path(parquet_dir).resolve()
    conn = duckdb.connect(db_path)

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

    conn.close()
    print(f"DuckDB database: {db_path}")
