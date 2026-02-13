#!/bin/bash
set -e

# ---------------------------------------------------------------------------
# 1. Build /data/governance.duckdb with views pointing at /data parquet files
# ---------------------------------------------------------------------------
python3 -c "
import duckdb, os, glob as g, sys
from pathlib import Path

DB = '/data/governance.duckdb'
BASE = Path('/data')

# Remove stale DB so views always reflect current datasets
if os.path.exists(DB):
    os.remove(DB)

con = duckdb.connect(DB)
count = 0

# Directories containing .parquet files -> epoch-partitioned views
for d in sorted(BASE.iterdir()):
    if d.is_dir() and g.glob(str(d / '*.parquet')):
        parquet_glob = f'{d}/*.parquet'
        con.execute(
            f\"CREATE OR REPLACE VIEW {d.name} AS \"
            f\"SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=true)\"
        )
        count += 1

# Top-level .parquet files -> single-file views
for f in sorted(BASE.glob('*.parquet')):
    name = f.stem
    con.execute(
        f\"CREATE OR REPLACE VIEW {name} AS \"
        f\"SELECT * FROM read_parquet('{f}')\"
    )
    count += 1

# KPI analytic views (imported from mounted gov_health package)
sys.path.insert(0, '/app')
from gov_health.kpis import ALL_KPI_VIEWS
for name, sql in ALL_KPI_VIEWS:
    con.execute(sql)
    count += 1

con.close()
print(f'governance.duckdb created with {count} views')
"

# ---------------------------------------------------------------------------
# 2. Register the DuckDB database in Superset
#
# This section runs on every container start and MUST be idempotent:
#   - create-admin silently skips if the user already exists
#   - db upgrade only applies pending migrations
#   - init is a no-op when roles/perms are already synced
#   - database registration upserts (update if exists, insert if not)
# ---------------------------------------------------------------------------
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname Admin \
    --email admin@localhost \
    --password admin \
    &>/dev/null || true

superset db upgrade
superset init

python3 -c "
from superset.app import create_app
app = create_app()

with app.app_context():
    from superset.models.core import Database
    from superset.extensions import db as sa_db

    name = 'Cardano Governance (DuckDB)'
    uri = 'duckdb:////data/governance.duckdb?access_mode=READ_ONLY'

    existing = sa_db.session.query(Database).filter_by(database_name=name).first()
    if existing:
        existing.sqlalchemy_uri = uri
    else:
        new_db = Database(database_name=name, sqlalchemy_uri=uri)
        sa_db.session.add(new_db)

    sa_db.session.commit()
    print(f'Registered database: {name}')
"

echo "Bootstrap complete."
