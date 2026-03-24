# Analytics Pipeline Plan — gov-health

## Context

We're building a governance health analytics pipeline that transforms yaci-store Parquet exports into analytics-ready Parquet files. These output files serve as the source of truth for DuckDB analysis and Apache Superset dashboards, powering the 47 KPIs defined in the [Intersect Governance Health KPI Report v1.0](https://gov-health.intersectmbo.org/).

Yaci-store now exports its dataset directly to Parquet files (Hive-partitioned by date or epoch), eliminating the need for PostgreSQL extraction. The pipeline reads source Parquet → transforms via DuckDB SQL → writes output Parquet.

---

## Project Structure

```
gov-health/
├── ANALYSIS.md                     # KPI + schema reference
├── pyproject.toml                  # Python 3.11+, dependencies, CLI entry point
├── .env.example                    # SOURCE_DATA_DIR config
├── gov_health/
│   ├── __init__.py
│   ├── cli.py                      # Click CLI: extract, create-views
│   ├── config.py                   # SOURCE_DATA_DIR + OUTPUT_DIR
│   ├── db.py                       # DuckDB connection, source table registration
│   ├── extract.py                  # Orchestration loop
│   ├── views.py                    # DuckDB views creation
│   ├── kpis/                       # KPI analytic views
│   │   ├── __init__.py             # ALL_KPI_VIEWS registry
│   │   └── category_1.py          # Cat 1: Ada Holder Participation
│   └── datasets/
│       ├── __init__.py             # ALL_DATASETS registry
│       ├── base.py                 # Abstract base classes
│       ├── drep_epoch_stats.py     # 1. DRep per-epoch stats
│       ├── pool_epoch_stats.py     # 2. Pool per-epoch stats
│       ├── gov_action_votes.py     # 3. Individual votes
│       ├── gov_action_lifecycle.py # 4. Action lifecycle + voting stats
│       ├── epoch_summary.py        # 5. Per-epoch aggregates
│       ├── delegation_events.py    # 6. Raw delegation events
│       ├── cc_vote_details.py      # 7. CC member votes
│       └── governance_params.py    # 8. Protocol governance params
├── data/analytics/main/            # Yaci-store Parquet export (gitignored)
└── output/                         # Generated parquet files (gitignored)
```

---

## Data Source

Yaci-store exports 46 tables as Parquet files into `data/analytics/main/`, using Hive-style partitioning:
- Date-partitioned: `table/date=YYYY-MM-DD/*.parquet` (e.g. `voting_procedure`, `delegation_vote`, `block`)
- Epoch-partitioned: `table/epoch=NNN/*.parquet` (e.g. `drep_dist`, `epoch_stake`, `adapot`)
- Flat: `table/*.parquet` (some smaller tables)

DuckDB reads these via `read_parquet('path/**/*.parquet', hive_partitioning=true)`. Each table directory is auto-registered as a view on connection creation.

**Notable quirks:**
- The `epoch` table directory is empty — epoch data is derived from `adapot` (financial) and `block` (counts/times)
- `block_time` columns are `TIMESTAMP WITH TIME ZONE` (not unix integers as in PostgreSQL)
- JSON columns (`params`, `voting_stats`, `details`, `pool_owners`) are stored as `VARCHAR`
- `voting_stats` JSON is flat (e.g. `cc_yes`, `drep_yes_vote_stake`) not nested
- `epoch_param.params` uses `drep_voting_thresholds` (not `d_rep_voting_thresholds`) and `pvt_ppsecurity_group` / `dvt_ppnetwork_group` etc.

---

## Parquet Files (8 datasets)

### Storage Strategies

- **Epoch-partitioned**: One file per epoch (`dataset/epoch=N.parquet`). Settled epochs are immutable; only the current unsettled epoch is rewritten. DuckDB reads via hive partitioning.
- **Single-file**: One file per dataset with read-merge-write incremental updates (load existing, append new epochs, replace updated epochs).

Settlement rule: an epoch is settled if it appears in `adapot` (i.e. yaci-store has finalized it).

---

### 1. `drep_epoch_stats/` (epoch-partitioned)

Per-DRep per-epoch delegation and voting stats. Core building block for KPIs: 1.3, 1.8, 2.1–2.3, 2.8, 2.10, 2.11.

| Column | Type | Source |
|--------|------|--------|
| epoch | INT | drep_dist.epoch |
| drep_hash | STRING | drep_dist.drep_hash |
| drep_type | STRING | drep_dist.drep_type |
| drep_id | STRING | drep_dist.drep_id |
| delegated_amount | INT64 | drep_dist.amount |
| active_until | INT | drep_dist.active_until |
| expiry | INT | drep_dist.expiry |
| status | STRING | drep.status (latest via ROW_NUMBER) |
| registration_epoch | INT | MIN(drep_registration.epoch) |
| has_metadata | BOOL | latest drep_registration.anchor_url IS NOT NULL |
| votes_cast | INT | COUNT from voting_procedure (DREP types) |
| votes_with_rationale | INT | COUNT where anchor_url IS NOT NULL |

**SQL approach:** CTEs with `ROW_NUMBER()` for latest DRep status and registration info, joined with vote counts from `voting_procedure`.

---

### 2. `pool_epoch_stats/` (epoch-partitioned)

Per-pool per-epoch stats. Core for KPIs: 3.1–3.5.

| Column | Type | Source |
|--------|------|--------|
| epoch | INT | pool.epoch |
| pool_id | STRING | pool.pool_id |
| status | STRING | pool.status |
| stake_amount | INT64 | pool.amount |
| votes_cast | INT | COUNT from voting_procedure |
| votes_yes | INT | |
| votes_no | INT | |
| votes_abstain | INT | |
| default_stance | STRING | delegation_vote drep_type for pool operator |

**SQL approach:** Pool owners extracted from `pool_registration.pool_owners` via `CAST(pool_owners AS VARCHAR[])` + `unnest()`. Latest delegation stance found via `ROW_NUMBER()`.

---

### 3. `gov_action_votes/` (epoch-partitioned)

All individual votes. Core for KPIs: 2.4, 2.5, 2.7, 2.9, 2.12, 3.4, 3.6, 4.2.

| Column | Type | Source |
|--------|------|--------|
| gov_action_tx_hash | STRING | voting_procedure |
| gov_action_index | INT | |
| action_type | STRING | gov_action_proposal.type |
| action_epoch | INT | gov_action_proposal.epoch |
| action_block_time | INT64 | epoch(gov_action_proposal.block_time) |
| voter_type | STRING | voting_procedure.voter_type |
| voter_hash | STRING | voting_procedure.voter_hash |
| vote | STRING | voting_procedure.vote |
| has_rationale | BOOL | anchor_url IS NOT NULL |
| vote_epoch | INT | voting_procedure.epoch (partition key) |
| vote_block_time | INT64 | epoch(voting_procedure.block_time) |
| vote_slot | INT64 | voting_procedure.slot |

---

### 4. `gov_action_lifecycle.parquet` (single-file)

Per-action lifecycle with full voting stats from JSON. Core for KPIs: 1.1, 3.7, 4.1, 4.2, 4.5, 4.7, 4.8.

34 columns including all CC/DRep/SPO voting stats extracted via `json_extract_string(voting_stats, '$.field')`. Latest status found via CTE + `ROW_NUMBER()`. Re-fetches actions still in non-terminal status.

---

### 5. `epoch_summary.parquet` (single-file)

Per-epoch context and denominators. Core for KPIs: 1.1, 1.3, 2.2, 3.1, 4.3, 5.2.

| Column | Type | Source |
|--------|------|--------|
| epoch | INT | adapot.epoch (base) |
| treasury | INT64 | adapot.treasury |
| reserves | INT64 | adapot.reserves |
| circulation | INT64 | adapot.circulation |
| utxo | INT64 | adapot.utxo |
| fees | INT64 | adapot.fees |
| total_active_stake | INT64 | SUM(epoch_stake.amount) |
| total_drep_delegated | INT64 | SUM(drep_dist.amount) |
| total_drep_delegated_non_abstain | INT64 | SUM where drep_type != 'ABSTAIN' |
| total_drep_delegated_no_confidence | INT64 | SUM where drep_type = 'NO_CONFIDENCE' |
| active_drep_count | INT | COUNT DISTINCT from drep_dist (non-special) |
| active_pool_count | INT | COUNT from pool where not retired |
| cc_member_count | INT | COUNT from committee_member |
| cc_threshold | FLOAT64 | committee.threshold |
| block_count | INT | COUNT(*) from block |
| tx_count | INT64 | COUNT(*) from transaction |
| epoch_start_time | INT64 | MIN(epoch(block.block_time)) |
| epoch_end_time | INT64 | MAX(epoch(block.block_time)) |
| gov_actions_proposed | INT | COUNT(gov_action_proposal) |
| gov_actions_ratified | INT | COUNT status=RATIFIED |
| gov_actions_expired | INT | COUNT status=EXPIRED |
| drep_registrations | INT | COUNT drep_registration type=REG_DREP_CERT |
| drep_retirements | INT | COUNT drep_registration type=UNREG_DREP_CERT |
| treasury_withdrawal_total | INT64 | COUNT enacted TREASURY_WITHDRAWALS_ACTION proposals |

---

### 6. `delegation_events.parquet` (single-file)

Raw delegation-vote events. Core for KPIs: 1.2, 1.4, 1.5, 1.7.

| Column | Type | Source |
|--------|------|--------|
| address | STRING | delegation_vote.address |
| drep_hash | STRING | |
| drep_id | STRING | |
| drep_type | STRING | |
| credential | STRING | delegation_vote.credential |
| epoch | INT | |
| slot | INT64 | |
| block_time | INT64 | epoch(delegation_vote.block_time) |

---

### 7. `cc_vote_details.parquet` (single-file)

CC member votes with timing. Core for KPIs: 5.1–5.4.

| Column | Type | Source |
|--------|------|--------|
| gov_action_tx_hash | STRING | voting_procedure |
| gov_action_index | INT | |
| action_type | STRING | gov_action_proposal.type |
| action_epoch | INT | |
| voter_hash | STRING | hot key from voting_procedure |
| cold_key | STRING | mapped via committee_registration (ROW_NUMBER) |
| vote | STRING | |
| vote_epoch | INT | |
| vote_block_time | INT64 | epoch(voting_procedure.block_time) |
| action_block_time | INT64 | epoch(gov_action_proposal.block_time) |
| time_to_vote_seconds | INT64 | vote_block_time - action_block_time |

---

### 8. `governance_params.parquet` (single-file)

Governance-relevant protocol parameters per epoch, extracted from `epoch_param.params` JSON.

| Column | Type | Source |
|--------|------|--------|
| epoch | INT | |
| drep_deposit | INT64 | json_extract_string(params, '$.drep_deposit') |
| gov_action_deposit | INT64 | json_extract_string(params, '$.gov_action_deposit') |
| drep_activity | INT | json_extract_string(params, '$.drep_activity') |
| gov_action_lifetime | INT | json_extract_string(params, '$.gov_action_lifetime') |
| committee_min_size | INT | json_extract_string(params, '$.committee_min_size') |
| committee_max_term_length | INT | json_extract_string(params, '$.committee_max_term_length') |
| pvt_* | FLOAT64 | numerator/denominator ratios from pool_voting_thresholds |
| dvt_* | FLOAT64 | numerator/denominator ratios from drep_voting_thresholds |

---

## Key Design Decisions

1. **Parquet-to-Parquet via DuckDB** — no PostgreSQL required. Source data is yaci-store Parquet exports read by DuckDB in-memory.
2. **CTEs over LATERAL** — DuckDB doesn't support `LATERAL` joins. All "latest row" lookups use CTEs with `ROW_NUMBER()`.
3. **JSON via `json_extract_string()`** — DuckDB JSON functions on VARCHAR columns. Flat key access (e.g. `$.cc_yes`).
4. **Network-agnostic** — no hardcoded thresholds. Governance thresholds extracted from `epoch_param.params`.
5. **Incremental by default** — only new/unsettled epochs processed. `--full` for complete re-extraction.
6. **Empty epoch skip** — epoch-partitioned datasets write no file for epochs with zero rows.
7. **Epoch from adapot** — the `epoch` table is empty in Parquet exports. Financial data from `adapot`, block/tx counts from `block`/`transaction` tables.

---

## KPI Analytic Views

The second layer of the pipeline: DuckDB views that compute each KPI as a per-epoch time-series, ready for Superset charting.

### Architecture

- **Location**: `gov_health/kpis/` package, one module per category
- **Registry**: `kpis/__init__.py` collects `ALL_KPI_VIEWS` from all category modules
- **Format**: Each KPI is a `(view_name, create_sql)` tuple
- **Naming convention**: `kpi_<category>_<number>_<short_name>` (e.g. `kpi_1_1_voting_turnout`)
- **Integration**: `views.py` iterates `ALL_KPI_VIEWS` after creating base views
- **Superset**: KPI views appear as datasets via `duckdb:///` SQLAlchemy URI

### Category 1 — Ada Holder Participation (5 views)

| View | KPI | SQL Logic |
|------|-----|-----------|
| `kpi_1_1_voting_turnout` | 1.1 Voting Turnout (% Ada) | `gov_action_lifecycle` DRep stake totals / `epoch_summary.circulation`, grouped by `epoch_proposed` |
| `kpi_1_2_active_stake_participation` | 1.2 Active Stake Address Participation | `COUNT(DISTINCT address)` from `delegation_events` per epoch + cumulative unique addresses |
| `kpi_1_3_delegation_rate` | 1.3 Delegation Rate (% Ada) | `epoch_summary.total_drep_delegated / circulation` |
| `kpi_1_5_delegation_churn` | 1.5 Delegation Churn | `LAG()` window over `delegation_events` to detect DRep changes per address |
| `kpi_1_8_inactive_delegated_ada` | 1.8 Inactive Delegated Ada | `drep_epoch_stats` where `status='RETIRED'` or `epoch > active_until`, summing `delegated_amount` |

### Deferred KPIs

| KPI | Reason |
|-----|--------|
| 1.4 New vs Returning Delegators | Needs per-address Ada balances |
| 1.6 Stake Registration Rate | Needs `stake_registration` events |
| 1.7 Future Delegation Status | Complex cross-correlation of delegation status with activity predictions |

---

## Superset Stack

Docker Compose stack providing Apache Superset with a DuckDB backend.

### Services

| Service | Image | Purpose |
|---------|-------|---------|
| `superset` | Custom (apache/superset:6.0.0) | Superset web UI on port 8088 |
| `superset-db` | postgres:16-alpine | Superset metadata store |
| `superset-redis` | redis:7-alpine | Cache + Celery broker |

### Usage

```bash
cd superset
docker compose up --build     # http://localhost:8088 (admin / admin)
```

---

## Verification

```bash
cd /Users/piz/git/bloxbean/gov-health
uv sync

# Extract all datasets
uv run gov-health extract

# Create DuckDB views
uv run gov-health create-views

# Test sample queries
duckdb output/governance.duckdb <<'SQL'
-- KPI 1.1: Voting turnout per action
SELECT type, drep_approval_ratio, spo_approval_ratio
FROM gov_action_lifecycle WHERE status = 'RATIFIED';

-- KPI 2.1: Gini coefficient
SELECT epoch, 1 - 2 * SUM(cs) / COUNT(*) AS gini FROM (
  SELECT epoch, SUM(delegated_amount) OVER (PARTITION BY epoch ORDER BY delegated_amount)
    * 1.0 / SUM(delegated_amount) OVER (PARTITION BY epoch) AS cs
  FROM drep_epoch_stats WHERE drep_type NOT IN ('ABSTAIN','NO_CONFIDENCE')
) GROUP BY epoch;

-- KPI 4.3: Treasury balance
SELECT epoch, treasury, treasury - LAG(treasury) OVER (ORDER BY epoch) AS delta
FROM epoch_summary;
SQL
```
