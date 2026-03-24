# Implementation Notes

Governance health analytics pipeline — Parquet-to-Parquet via DuckDB.
Reads yaci-store Parquet exports directly (no PostgreSQL required) and builds analytics Parquet files for analysis via DuckDB / Apache Superset.

## Quick Start

```bash
cd /Users/piz/git/bloxbean/gov-health

# Install
uv sync

# Place yaci-store parquet export in data/analytics/main/
# (or set SOURCE_DATA_DIR in .env to point elsewhere)
cp .env.example .env

# Extract all datasets
uv run gov-health extract

# Extract a single dataset
uv run gov-health extract --only drep_epoch_stats

# Full re-extraction (ignore existing files)
uv run gov-health extract --full

# Point to a different source directory
uv run gov-health extract --source /path/to/parquet/export

# Create DuckDB views over the parquet files
uv run gov-health create-views
```

## File Structure

```
gov-health/
├── pyproject.toml                      # Python 3.11+, hatchling build
├── .env.example                        # SOURCE_DATA_DIR config
├── .gitignore                          # Excludes output/, data/, .env, .venv/
├── ANALYSIS.md                         # KPI + schema reference
├── PLAN.md                             # Design plan
├── gov_health/
│   ├── __init__.py
│   ├── cli.py                          # Click CLI entry point
│   ├── config.py                       # SOURCE_DATA_DIR + OUTPUT_DIR config
│   ├── db.py                           # DuckDB connection + source table registration
│   ├── extract.py                      # Orchestration loop
│   ├── views.py                        # DuckDB view creation
│   ├── kpis/
│   │   ├── __init__.py               # ALL_KPI_VIEWS registry
│   │   └── category_1.py            # Cat 1: Ada Holder Participation (5 views)
│   └── datasets/
│       ├── __init__.py                 # ALL_DATASETS registry (list of 8)
│       ├── base.py                     # EpochPartitionedDataset, SingleFileDataset
│       ├── drep_epoch_stats.py         # 1. DRep per-epoch delegation + voting
│       ├── pool_epoch_stats.py         # 2. Pool per-epoch stats + default stance
│       ├── gov_action_votes.py         # 3. Individual vote records
│       ├── gov_action_lifecycle.py     # 4. Action lifecycle + voting stats
│       ├── epoch_summary.py            # 5. Per-epoch aggregates + denominators
│       ├── delegation_events.py        # 6. Raw delegation-vote events
│       ├── cc_vote_details.py          # 7. CC member votes + hot→cold key mapping
│       └── governance_params.py        # 8. Protocol governance params per epoch
├── data/
│   └── analytics/main/                 # Yaci-store Parquet export (gitignored)
│       ├── adapot/
│       ├── block/
│       ├── committee/
│       ├── committee_member/
│       ├── committee_registration/
│       ├── delegation_vote/
│       ├── drep/
│       ├── drep_dist/
│       ├── drep_registration/
│       ├── epoch_param/
│       ├── epoch_stake/
│       ├── gov_action_proposal/
│       ├── gov_action_proposal_status/
│       ├── gov_epoch_activity/
│       ├── pool/
│       ├── pool_registration/
│       ├── transaction/
│       ├── voting_procedure/
│       └── ...                         # 46 tables total
├── superset/
│   ├── docker-compose.yml             # superset + superset-db + superset-redis
│   ├── Dockerfile                     # apache/superset:6.0.0 + duckdb-engine
│   ├── requirements-local.txt         # duckdb, duckdb-engine, psycopg2-binary
│   ├── superset_config.py             # Metadata DB, cache, Celery config
│   ├── bootstrap.sh                   # DuckDB views + admin user + DB registration
│   └── .env                           # COMPOSE_PROJECT_NAME, SUPERSET_SECRET_KEY
└── output/                             # Generated parquet files (gitignored)
```

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| duckdb | >=1.1 | SQL engine — reads source parquet, writes output parquet |
| pyarrow | >=18.0 | Parquet read/write, Arrow table merges |
| click | >=8.0 | CLI framework |
| tqdm | >=4.0 | Progress bars during extraction |
| python-dotenv | >=1.0 | Load .env for config |

## Architecture

### Data Flow

```
yaci-store Parquet export (data/analytics/main/)
  → DuckDB SQL transforms (in-memory)
    → output Parquet files (output/)
      → DuckDB views + KPI views (output/governance.duckdb)
        → Apache Superset dashboards
```

No PostgreSQL connection is needed. The source data is a yaci-store Parquet export with Hive-style partitioning (`date=YYYY-MM-DD` or `epoch=NNN`). DuckDB reads these directly via `read_parquet()` with `hive_partitioning=true`.

### Two Storage Strategies

**Epoch-partitioned** — one file per epoch in a hive-partitioned directory:
- `output/drep_epoch_stats/epoch=520.parquet`
- Settled epochs (present in `adapot`) are never rewritten
- The current unsettled epoch is rewritten on every run (`force=True`)
- Empty epochs produce no file (skipped)

**Single-file** — one `.parquet` per dataset with read-merge-write:
- `output/epoch_summary.parquet`
- On incremental runs: load existing file, filter out replaced epochs, concat with new data
- `gov_action_lifecycle` has a custom override that deduplicates by `(tx_hash, index)` instead of epoch, and always re-fetches actions with non-terminal status

### Core Modules

**`config.py`** — reads `SOURCE_DATA_DIR` (default: `data/analytics/main`) and defines `OUTPUT_DIR`.

**`db.py`** — DuckDB source connection management:
- `get_connection()` — creates an in-memory DuckDB connection and registers all source parquet table directories as views (auto-detects Hive partitioning)
- `get_settled_epochs(conn)` — all distinct epochs in `adapot` (proxy for settled)
- `get_max_epoch(conn)` — highest epoch in `adapot`
- `get_conway_start_epoch(conn)` — first epoch with `drep_dist` data (skips pre-governance eras)

**`datasets/base.py`** — two abstract base classes:
- `EpochPartitionedDataset` — subclasses implement `query_epoch(epoch) -> str` returning DuckDB SQL
- `SingleFileDataset` — subclasses implement `query_epochs(epochs) -> str` returning DuckDB SQL
- Both execute SQL via `conn.execute(sql).fetch_arrow_table()` and write via `pq.write_table()`

**`extract.py`** — orchestration:
1. Create DuckDB connection with all source tables registered
2. Discover epochs (settled, max, conway_start)
3. Filter to Conway-era only
4. For each dataset: determine needed epochs, extract with tqdm progress
5. `--full` flag deletes existing files before re-extracting

**`views.py`** — creates DuckDB views:
- Epoch-partitioned datasets: `read_parquet('.../*.parquet', hive_partitioning=true)`
- Single-file datasets: `read_parquet('.../<name>.parquet')`
- KPI views: `CREATE OR REPLACE VIEW` from `ALL_KPI_VIEWS` registry

## The 8 Datasets

### 1. `drep_epoch_stats` (epoch-partitioned, 12 columns)

Per-DRep per-epoch delegation and voting stats. Uses CTEs with `ROW_NUMBER()` to find latest DRep status and registration info. Feeds KPIs: 1.3, 1.8, 2.1-2.3, 2.8, 2.10, 2.11.

### 2. `pool_epoch_stats` (epoch-partitioned, 9 columns)

Per-pool per-epoch stats. Joins `pool` with `voting_procedure` (vote breakdown by YES/NO/ABSTAIN) and `delegation_vote` via `pool_registration.pool_owners` (default stance, using `CAST(pool_owners AS VARCHAR[])` for JSON array parsing). Feeds KPIs: 3.1-3.5.

### 3. `gov_action_votes` (epoch-partitioned, 12 columns)

Every individual vote record. Joins `voting_procedure` with `gov_action_proposal` for action metadata. Block times converted via `epoch(block_time)`. Feeds KPIs: 2.4, 2.5, 2.7, 2.9, 2.12, 3.4, 3.6, 4.2.

### 4. `gov_action_lifecycle` (single-file, 34 columns)

Per-action lifecycle with full voting stats flattened from `voting_stats` JSON using `json_extract_string()`. Flat JSON keys (e.g. `$.cc_yes`, `$.drep_yes_vote_stake`). Custom `extract()` always re-fetches non-terminal actions. Deduplicates by `(tx_hash, index)`. Feeds KPIs: 1.1, 3.7, 4.1, 4.2, 4.5, 4.7, 4.8.

### 5. `epoch_summary` (single-file, 24 columns)

Per-epoch context and denominators. Drives from `adapot` as the epoch base (since the `epoch` table is empty in yaci-store Parquet exports). Block and transaction counts derived from `block` and `transaction` tables. Treasury withdrawal count derived from `gov_action_proposal` + `gov_action_proposal_status` (enacted treasury withdrawal actions). Feeds KPIs: 1.1, 1.3, 2.2, 3.1, 4.3, 5.2.

### 6. `delegation_events` (single-file, 8 columns)

Raw delegation-vote events from the `delegation_vote` table. Feeds KPIs: 1.2, 1.4, 1.5, 1.7.

### 7. `cc_vote_details` (single-file, 12 columns)

CC member votes with timing. Joins `voting_procedure` (CC voter types only) with `gov_action_proposal` and `committee_registration` (latest hot_key → cold_key via `ROW_NUMBER()`). Feeds KPIs: 5.1-5.4.

### 8. `governance_params` (single-file, 22 columns)

Governance-relevant protocol parameters per epoch from `epoch_param.params` JSON. Uses `json_extract_string()` for scalar fields and computes `numerator/denominator` ratios for all pool voting thresholds (`pvt_*`) and DRep voting thresholds (`dvt_*`). Note: yaci-store JSON key names are `drep_voting_thresholds` (not `d_rep_voting_thresholds`) and `pvt_ppsecurity_group` / `dvt_ppnetwork_group` etc. (no underscore between `pp` and group name). Feeds KPI 4.9 and contextual threshold display.

## Design Decisions

1. **DuckDB-native** — all SQL is DuckDB dialect. No PostgreSQL dependency. Source tables are registered as views over `read_parquet()`.
2. **CTEs over LATERAL** — DuckDB doesn't support `LATERAL` joins. All lookups use CTEs with `ROW_NUMBER()` window functions.
3. **JSON via `json_extract_string()`** — DuckDB's JSON functions operate on VARCHAR columns. `voting_stats` and `epoch_param.params` are parsed at extraction time into typed Parquet columns.
4. **Network-agnostic** — no hardcoded thresholds; governance thresholds extracted from `epoch_param.params`.
5. **Conway-era filter** — `get_conway_start_epoch()` queries `MIN(epoch) FROM drep_dist` to skip pre-governance epochs.
6. **Incremental by default** — only new/unsettled epochs processed. `--full` for complete re-extraction.
7. **Empty epoch skip** — epoch-partitioned datasets write no file for epochs with zero rows.
8. **Lifecycle dedup** — `gov_action_lifecycle` deduplicates by `(tx_hash, index)` primary key and always re-fetches non-terminal actions.
9. **Epoch from adapot** — the `epoch` table is empty in yaci-store Parquet exports. Epoch boundaries are derived from the `block` table; financial data from `adapot`.

## KPI Analytic Views

The second layer on top of the base Parquet views. Each KPI is a `CREATE OR REPLACE VIEW` in DuckDB that computes a per-epoch time-series from one or more base views. Created automatically by `create-views`.

### Architecture

- **`gov_health/kpis/`** — one module per KPI category, each exporting a `CATEGORY_N_VIEWS` list of `(view_name, create_sql)` tuples
- **`kpis/__init__.py`** — collects all category lists into `ALL_KPI_VIEWS` (mirrors `datasets/__init__.py`)
- **`views.py`** — iterates `ALL_KPI_VIEWS` after base views, executing each SQL
- **Naming**: `kpi_<category>_<number>_<short_name>` (e.g. `kpi_1_3_delegation_rate`)

### Category 1 — Ada Holder Participation (5 implemented)

| View | KPI | Description | Base Views Used |
|------|-----|-------------|-----------------|
| `kpi_1_1_voting_turnout` | 1.1 | DRep voting turnout as % of circulating Ada | `gov_action_lifecycle`, `epoch_summary` |
| `kpi_1_2_active_stake_participation` | 1.2 | Distinct delegating addresses per epoch + cumulative | `delegation_events` |
| `kpi_1_3_delegation_rate` | 1.3 | Total delegated Ada as % of circulation | `epoch_summary` |
| `kpi_1_5_delegation_churn` | 1.5 | DRep re-delegations detected via LAG() window | `delegation_events` |
| `kpi_1_8_inactive_delegated_ada` | 1.8 | Ada delegated to retired/expired DReps | `drep_epoch_stats` |

### Deferred (3 KPIs)

| KPI | Reason |
|-----|--------|
| 1.4 New vs Returning Delegators | Needs per-address Ada balances |
| 1.6 Stake Registration Rate | Needs `stake_registration` events |
| 1.7 Future Delegation Status | Complex cross-correlation, deferred |

## Superset Stack

Docker Compose stack in `superset/` providing Apache Superset with DuckDB backend.

```
superset/
├── docker-compose.yml         # superset + superset-db + superset-redis
├── Dockerfile                 # apache/superset:6.0.0 + duckdb-engine
├── requirements-local.txt     # duckdb 1.4.4, duckdb-engine 0.17.0, psycopg2-binary
├── superset_config.py         # Metadata DB (Postgres), Redis cache, PREVENT_UNSAFE_DB_CONNECTIONS=False
├── bootstrap.sh               # DuckDB views + admin user + database registration
└── .env                       # COMPOSE_PROJECT_NAME, SUPERSET_SECRET_KEY
```

**Key details:**
- Mounts `../output` → `/data` (parquet files) and `../gov_health` → `/app/gov_health` (KPI SQL — single source of truth)
- `bootstrap.sh` auto-discovers parquet files for base views, imports `ALL_KPI_VIEWS` from mounted package for KPI views
- Database registered as `Cardano Governance (DuckDB)` with URI `duckdb:////data/governance.duckdb?access_mode=READ_ONLY`
- Admin credentials: admin / admin
- Start: `cd superset && docker compose up --build` → http://localhost:8088
