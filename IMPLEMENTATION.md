# Implementation Notes

Governance health Parquet extraction pipeline, built from the plan in `PLAN.md`.
Extracts data from a yaci-store PostgreSQL database into Parquet files for analysis via DuckDB / Apache Superset.

## Quick Start

```bash
cd /Users/piz/git/bloxbean/gov-health

# Install
uv sync --extra duckdb

# Configure database connection
cp .env.example .env
# Edit .env with your yaci-store credentials

# Extract all datasets
uv run gov-health extract

# Extract a single dataset
uv run gov-health extract --only drep_epoch_stats

# Full re-extraction (ignore existing files)
uv run gov-health extract --full

# Create DuckDB views over the parquet files
uv run gov-health create-views
```

## File Structure

```
gov-health/
├── pyproject.toml                      # Python 3.11+, hatchling build
├── .env.example                        # DB connection template
├── .gitignore                          # Excludes output/, .env, .venv/
├── ANALYSIS.md                         # KPI + schema reference (pre-existing)
├── PLAN.md                             # Original design plan (pre-existing)
├── gov_health/
│   ├── __init__.py
│   ├── cli.py                          # Click CLI entry point
│   ├── config.py                       # Env-var based DB config
│   ├── db.py                           # Connection factory + epoch discovery
│   ├── extract.py                      # Orchestration loop
│   ├── views.py                        # DuckDB view creation
│   └── datasets/
│       ├── __init__.py                 # ALL_DATASETS registry (list of 8)
│       ├── base.py                     # EpochPartitionedDataset, SingleFileDataset
│       ├── drep_epoch_stats.py         # 1. DRep per-epoch delegation + voting
│       ├── pool_epoch_stats.py         # 2. Pool per-epoch stats + default stance
│       ├── gov_action_votes.py         # 3. Individual vote records
│       ├── gov_action_lifecycle.py     # 4. Action lifecycle + JSONB voting stats
│       ├── epoch_summary.py            # 5. Per-epoch aggregates + denominators
│       ├── delegation_events.py        # 6. Raw delegation-vote events
│       ├── cc_vote_details.py          # 7. CC member votes + hot→cold key mapping
│       └── governance_params.py        # 8. Protocol governance params per epoch
└── output/                             # Generated parquet files (gitignored)
```

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| psycopg[binary] | >=3.1 | PostgreSQL driver (psycopg 3, dict_row) |
| pyarrow | >=18.0 | Parquet read/write, Arrow tables |
| click | >=8.0 | CLI framework |
| tqdm | >=4.0 | Progress bars during extraction |
| python-dotenv | >=1.0 | Load .env for DB credentials |
| duckdb | >=1.1 | Optional — only needed for `create-views` |

## Architecture

### Two Storage Strategies

**Epoch-partitioned** — one file per epoch in a hive-partitioned directory:
- `output/drep_epoch_stats/epoch=520.parquet`
- Settled epochs (end_time older than 24h) are never rewritten
- The current unsettled epoch is rewritten on every run (`force=True`)
- Empty epochs produce no file (skipped)

**Single-file** — one `.parquet` per dataset with read-merge-write:
- `output/epoch_summary.parquet`
- On incremental runs: load existing file, filter out replaced epochs, concat with new data
- `gov_action_lifecycle` has a custom override that deduplicates by `(tx_hash, index)` instead of epoch, and always re-fetches actions with non-terminal status

### Core Modules

**`config.py`** — reads six env vars (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_SCHEMA`) with sensible defaults. Defines `SETTLEMENT_SECONDS = 86400`.

**`db.py`** — three discovery functions on top of a psycopg3 connection:
- `get_connection()` — sets `search_path=yaci_store,public`, returns `dict_row` connection
- `get_settled_epochs(conn)` — epochs with `end_time < now() - 86400`
- `get_max_epoch(conn)` — highest epoch number
- `get_conway_start_epoch(conn)` — first epoch with `drep_dist` data (skips pre-governance eras)

**`datasets/base.py`** — two abstract base classes:
- `EpochPartitionedDataset` — subclasses implement `schema()` and `query_epoch(epoch) -> (sql, params)`
- `SingleFileDataset` — subclasses implement `schema()` and `query_epochs(epochs) -> (sql, params)`
- Both handle parquet I/O internally: `pa.Table.from_pylist()` + `pq.write_table()`, with `pa.concat_tables()` for merges

**`extract.py`** — orchestration:
1. Connect and discover epochs (settled, max, conway_start)
2. Filter to Conway-era only
3. For each dataset: determine needed epochs, extract with tqdm progress
4. `--full` flag deletes existing files before re-extracting

**`views.py`** — creates DuckDB views:
- Epoch-partitioned datasets: `read_parquet('.../*.parquet', hive_partitioning=true)`
- Single-file datasets: `read_parquet('.../<name>.parquet')`

## The 8 Datasets

### 1. `drep_epoch_stats` (epoch-partitioned, 12 columns)

Per-DRep per-epoch delegation and voting stats. Joins `drep_dist` with `drep` (status via LATERAL), `drep_registration` (first registration epoch, metadata check), and `voting_procedure` (vote counts). Feeds KPIs: 1.3, 1.8, 2.1-2.3, 2.8, 2.10, 2.11.

### 2. `pool_epoch_stats` (epoch-partitioned, 9 columns)

Per-pool per-epoch stats. Joins `pool` with `voting_procedure` (vote breakdown by YES/NO/ABSTAIN) and `delegation_vote` via `pool_registration.pool_owners` (default stance). Feeds KPIs: 3.1-3.5.

### 3. `gov_action_votes` (epoch-partitioned, 12 columns)

Every individual vote record. Joins `voting_procedure` with `gov_action_proposal` for action metadata. Partitioned by `vote_epoch`. Feeds KPIs: 2.4, 2.5, 2.7, 2.9, 2.12, 3.4, 3.6, 4.2.

### 4. `gov_action_lifecycle` (single-file, 34 columns)

Per-action lifecycle with full voting stats flattened from `voting_stats` JSONB (cc/drep/spo breakdowns and approval ratios). Custom `extract()` always re-fetches non-terminal actions. Deduplicates by `(tx_hash, index)`. Feeds KPIs: 1.1, 3.7, 4.1, 4.2, 4.5, 4.7, 4.8.

### 5. `epoch_summary` (single-file, 24 columns)

Per-epoch context and denominators. Large multi-join query aggregating from `epoch`, `adapot`, `epoch_stake`, `drep_dist`, `pool`, `committee_member`, `committee`, `gov_action_proposal`, `gov_action_proposal_status`, `drep_registration`, and `local_treasury_withdrawal`. Feeds KPIs: 1.1, 1.3, 2.2, 3.1, 4.3, 5.2.

### 6. `delegation_events` (single-file, 8 columns)

Raw delegation-vote events from the `delegation_vote` table. Feeds KPIs: 1.2, 1.4, 1.5, 1.7.

### 7. `cc_vote_details` (single-file, 12 columns)

CC member votes with timing. Joins `voting_procedure` (CC voter types only) with `gov_action_proposal` and `committee_registration` for hot_key → cold_key resolution. Computes `time_to_vote_seconds`. Feeds KPIs: 5.1-5.4.

### 8. `governance_params` (single-file, 22 columns)

Governance-relevant protocol parameters per epoch from `epoch_param.params` JSONB. Extracts scalar fields directly and computes `numerator/denominator` ratios for all pool voting thresholds (`pvt_*`) and DRep voting thresholds (`dvt_*`). Feeds KPI 4.9 and contextual threshold display.

## Design Decisions

1. **Named SQL parameters** — all queries use `%(epoch)s` / `%(epochs)s` with dict params for psycopg3 compatibility.
2. **Network-agnostic** — no hardcoded thresholds; governance thresholds extracted from `epoch_param.params` into `governance_params.parquet`.
3. **Schema prefix** — connection sets `search_path=yaci_store,public` so queries reference tables directly.
4. **JSONB flattened at extraction** — `voting_stats` and `epoch_param.params` are flattened into typed Parquet columns, not re-parsed at query time.
5. **Conway-era filter** — `get_conway_start_epoch()` queries `MIN(epoch) FROM drep_dist` to skip pre-governance epochs automatically.
6. **Incremental by default** — only new/unsettled epochs processed. `--full` for complete re-extraction.
7. **Empty epoch skip** — epoch-partitioned datasets write no file for epochs with zero rows.
8. **Lifecycle dedup** — `gov_action_lifecycle` deduplicates by `(tx_hash, index)` primary key rather than by epoch, and always re-fetches actions that haven't reached terminal status.
