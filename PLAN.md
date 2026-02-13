# Parquet Extraction Plan — gov-health

## Context

We're building a governance health analytics pipeline that extracts data from a yaci-store PostgreSQL database into Parquet files. These files serve as the efficient source of truth for analysis via DuckDB and Apache Superset, powering dashboards based on the 47 KPIs defined in the [Intersect Governance Health KPI Report v1.0](https://gov-health.intersectmbo.org/).

A previous implementation (ys-to-parquet) proved the Python + psycopg3 + pyarrow + click stack works well. We restart from scratch in `/Users/piz/git/bloxbean/gov-health/` taking the best ideas from that project.

---

## Project Structure

```
gov-health/
├── ANALYSIS.md                     # Already created — KPI + schema reference
├── pyproject.toml                  # Python 3.11+, dependencies, CLI entry point
├── .env.example                    # Database connection template
├── gov_health/
│   ├── __init__.py
│   ├── cli.py                      # Click CLI: extract, create-views
│   ├── config.py                   # DB config from env vars
│   ├── db.py                       # Connection management, epoch discovery
│   ├── extract.py                  # Orchestration loop
│   ├── views.py                    # DuckDB views creation
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
└── output/                         # Generated parquet files (gitignored)
```

---

## Parquet Files (8 datasets)

### Storage Strategies

- **Epoch-partitioned**: One file per epoch (`dataset/epoch=N.parquet`). Settled epochs are immutable; only the current unsettled epoch is rewritten. DuckDB reads via hive partitioning.
- **Single-file**: One file per dataset with read-merge-write incremental updates (load existing, append new epochs, replace updated epochs).

Settlement rule: an epoch is settled when `epoch.end_time < now() - 86400` (1 day buffer).

---

### 1. `drep_epoch_stats/` (epoch-partitioned)

Per-DRep per-epoch delegation and voting stats. Core building block for KPIs: 1.3, 1.8, 2.1–2.3, 2.8, 2.10, 2.11.

| Column | Type | Source |
|--------|------|--------|
| epoch | INT32 | partition key |
| drep_hash | STRING | drep_dist.drep_hash |
| drep_type | STRING | drep_dist.drep_type |
| drep_id | STRING | drep_dist.drep_id |
| delegated_amount | INT64 | drep_dist.amount |
| active_until | INT32 | drep_dist.active_until |
| expiry | INT32 | drep_dist.expiry |
| status | STRING | drep.status (latest for that epoch) |
| registration_epoch | INT32 | MIN(drep_registration.epoch) for this drep |
| has_metadata | BOOL | latest drep_registration.anchor_url IS NOT NULL |
| votes_cast | INT32 | COUNT from voting_procedure (DREP types) |
| votes_with_rationale | INT32 | COUNT where anchor_url IS NOT NULL |

**SQL:**
```sql
SELECT
  dd.epoch, dd.drep_hash, dd.drep_type, dd.drep_id,
  dd.amount AS delegated_amount,
  dd.active_until, dd.expiry,
  d.status,
  reg.registration_epoch,
  reg.has_metadata,
  COALESCE(v.votes_cast, 0) AS votes_cast,
  COALESCE(v.votes_with_rationale, 0) AS votes_with_rationale
FROM drep_dist dd
LEFT JOIN LATERAL (
  SELECT status FROM drep
  WHERE drep_hash = dd.drep_hash AND epoch <= dd.epoch
  ORDER BY slot DESC LIMIT 1
) d ON true
LEFT JOIN LATERAL (
  SELECT
    MIN(epoch) AS registration_epoch,
    (SELECT anchor_url IS NOT NULL FROM drep_registration
     WHERE drep_hash = dd.drep_hash ORDER BY slot DESC LIMIT 1) AS has_metadata
  FROM drep_registration WHERE drep_hash = dd.drep_hash
) reg ON true
LEFT JOIN (
  SELECT voter_hash, epoch,
    COUNT(*) AS votes_cast,
    COUNT(anchor_url) AS votes_with_rationale
  FROM voting_procedure
  WHERE voter_type IN ('DREP_KEY_HASH', 'DREP_SCRIPT_HASH')
  GROUP BY voter_hash, epoch
) v ON v.voter_hash = dd.drep_hash AND v.epoch = dd.epoch
WHERE dd.epoch = %(epoch)s
ORDER BY dd.drep_hash
```

---

### 2. `pool_epoch_stats/` (epoch-partitioned)

Per-pool per-epoch stats. Core for KPIs: 3.1–3.5.

| Column | Type | Source |
|--------|------|--------|
| epoch | INT32 | partition key |
| pool_id | STRING | pool.pool_id |
| status | STRING | pool.status |
| stake_amount | INT64 | pool.amount |
| votes_cast | INT32 | COUNT from voting_procedure |
| votes_yes | INT32 | |
| votes_no | INT32 | |
| votes_abstain | INT32 | |
| default_stance | STRING | delegation_vote drep_type for pool operator |

**SQL:**
```sql
SELECT
  p.epoch, p.pool_id, p.status, p.amount AS stake_amount,
  COALESCE(v.votes_cast, 0) AS votes_cast,
  COALESCE(v.votes_yes, 0) AS votes_yes,
  COALESCE(v.votes_no, 0) AS votes_no,
  COALESCE(v.votes_abstain, 0) AS votes_abstain,
  ds.default_stance
FROM pool p
LEFT JOIN (
  SELECT voter_hash, epoch,
    COUNT(*) AS votes_cast,
    COUNT(*) FILTER (WHERE vote = 'YES') AS votes_yes,
    COUNT(*) FILTER (WHERE vote = 'NO') AS votes_no,
    COUNT(*) FILTER (WHERE vote = 'ABSTAIN') AS votes_abstain
  FROM voting_procedure
  WHERE voter_type = 'STAKING_POOL_KEY_HASH'
  GROUP BY voter_hash, epoch
) v ON v.voter_hash = p.pool_id AND v.epoch = p.epoch
LEFT JOIN LATERAL (
  SELECT CASE
    WHEN dv.drep_type = 'ABSTAIN' THEN 'ABSTAIN'
    WHEN dv.drep_type = 'NO_CONFIDENCE' THEN 'NO_CONFIDENCE'
    ELSE NULL
  END AS default_stance
  FROM delegation_vote dv
  WHERE dv.drep_type IN ('ABSTAIN', 'NO_CONFIDENCE')
    AND dv.credential IN (
      SELECT jsonb_array_elements_text(pr.pool_owners)
      FROM pool_registration pr
      WHERE pr.pool_id = p.pool_id
      ORDER BY pr.slot DESC LIMIT 1
    )
  ORDER BY dv.slot DESC LIMIT 1
) ds ON true
WHERE p.epoch = %(epoch)s
ORDER BY p.pool_id
```

---

### 3. `gov_action_votes/` (epoch-partitioned)

All individual votes. Core for KPIs: 2.4, 2.5, 2.7, 2.9, 2.12, 3.4, 3.6, 4.2.

| Column | Type | Source |
|--------|------|--------|
| gov_action_tx_hash | STRING | voting_procedure |
| gov_action_index | INT32 | |
| action_type | STRING | gov_action_proposal.type |
| action_epoch | INT32 | gov_action_proposal.epoch |
| action_block_time | INT64 | gov_action_proposal.block_time |
| voter_type | STRING | voting_procedure.voter_type |
| voter_hash | STRING | voting_procedure.voter_hash |
| vote | STRING | voting_procedure.vote |
| has_rationale | BOOL | anchor_url IS NOT NULL |
| vote_epoch | INT32 | voting_procedure.epoch (partition key) |
| vote_block_time | INT64 | voting_procedure.block_time |
| vote_slot | INT64 | voting_procedure.slot |

**SQL:**
```sql
SELECT
  vp.gov_action_tx_hash, vp.gov_action_index,
  gap.type AS action_type, gap.epoch AS action_epoch,
  gap.block_time AS action_block_time,
  vp.voter_type, vp.voter_hash, vp.vote,
  (vp.anchor_url IS NOT NULL) AS has_rationale,
  vp.epoch AS vote_epoch,
  vp.block_time AS vote_block_time,
  vp.slot AS vote_slot
FROM voting_procedure vp
JOIN gov_action_proposal gap
  ON gap.tx_hash = vp.gov_action_tx_hash AND gap.idx = vp.gov_action_index
WHERE vp.epoch = %(epoch)s
ORDER BY vp.gov_action_tx_hash, vp.gov_action_index, vp.voter_hash, vp.slot
```

---

### 4. `gov_action_lifecycle.parquet` (single-file)

Per-action lifecycle with full voting stats from the JSONB. Core for KPIs: 1.1, 3.7, 4.1, 4.2, 4.5, 4.7, 4.8.

| Column | Type | Source |
|--------|------|--------|
| gov_action_tx_hash | STRING | gov_action_proposal.tx_hash |
| gov_action_index | INT32 | gov_action_proposal.idx |
| type | STRING | gov_action_proposal.type |
| deposit | INT64 | gov_action_proposal.deposit |
| return_address | STRING | gov_action_proposal.return_address |
| anchor_url | STRING | gov_action_proposal.anchor_url |
| epoch_proposed | INT32 | gov_action_proposal.epoch |
| block_time_proposed | INT64 | gov_action_proposal.block_time |
| status | STRING | latest gov_action_proposal_status.status |
| epoch_resolved | INT32 | epoch when status became RATIFIED or EXPIRED |
| cc_yes | INT32 | voting_stats→cc_yes |
| cc_no | INT32 | voting_stats→cc_no |
| cc_abstain | INT32 | voting_stats→cc_abstain |
| cc_do_not_vote | INT32 | voting_stats→cc_do_not_vote |
| cc_approval_ratio | FLOAT64 | voting_stats→cc_approval_ratio |
| drep_yes_vote_stake | INT64 | voting_stats→drep_yes_vote_stake |
| drep_no_vote_stake | INT64 | voting_stats→drep_no_vote_stake |
| drep_abstain_vote_stake | INT64 | |
| drep_no_confidence_stake | INT64 | |
| drep_auto_abstain_stake | INT64 | |
| drep_do_not_vote_stake | INT64 | |
| drep_total_yes_stake | INT64 | |
| drep_total_no_stake | INT64 | |
| drep_total_abstain_stake | INT64 | |
| drep_approval_ratio | FLOAT64 | |
| spo_yes_vote_stake | INT64 | |
| spo_no_vote_stake | INT64 | |
| spo_abstain_vote_stake | INT64 | |
| spo_do_not_vote_stake | INT64 | |
| spo_total_yes_stake | INT64 | |
| spo_total_no_stake | INT64 | |
| spo_total_abstain_stake | INT64 | |
| spo_approval_ratio | FLOAT64 | |

**SQL:** Joins gov_action_proposal with latest gov_action_proposal_status per action via LATERAL subquery, extracting voting_stats JSONB fields. Re-fetches actions still ACTIVE (non-terminal status).

---

### 5. `epoch_summary.parquet` (single-file)

Per-epoch context and denominators. Core for KPIs: 1.1, 1.3, 2.2, 3.1, 4.3, 5.2.

| Column | Type | Source |
|--------|------|--------|
| epoch | INT32 | |
| treasury | INT64 | adapot.treasury |
| reserves | INT64 | adapot.reserves |
| circulation | INT64 | adapot.circulation |
| utxo | INT64 | adapot.utxo |
| fees | INT64 | adapot.fees |
| total_active_stake | INT64 | SUM(epoch_stake.amount) |
| total_drep_delegated | INT64 | SUM(drep_dist.amount) |
| total_drep_delegated_non_abstain | INT64 | SUM where drep_type != 'ABSTAIN' |
| total_drep_delegated_no_confidence | INT64 | SUM where drep_type = 'NO_CONFIDENCE' |
| active_drep_count | INT32 | COUNT DISTINCT from drep_dist (non-special) |
| active_pool_count | INT32 | COUNT from pool where not retired |
| cc_member_count | INT32 | COUNT from committee_member |
| cc_threshold | FLOAT64 | committee.threshold |
| block_count | INT32 | epoch.block_count |
| tx_count | INT64 | epoch.transaction_count |
| epoch_start_time | INT64 | epoch.start_time |
| epoch_end_time | INT64 | epoch.end_time |
| gov_actions_proposed | INT32 | COUNT(gov_action_proposal) |
| gov_actions_ratified | INT32 | COUNT status=RATIFIED |
| gov_actions_expired | INT32 | COUNT status=EXPIRED |
| drep_registrations | INT32 | COUNT drep_registration type=REG |
| drep_retirements | INT32 | COUNT drep_registration type=UNREG |
| treasury_withdrawal_total | INT64 | SUM(local_treasury_withdrawal.amount) |

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
| epoch | INT32 | |
| slot | INT64 | |
| block_time | INT64 | |

---

### 7. `cc_vote_details.parquet` (single-file)

CC member votes with timing. Core for KPIs: 5.1–5.4.

| Column | Type | Source |
|--------|------|--------|
| gov_action_tx_hash | STRING | voting_procedure |
| gov_action_index | INT32 | |
| action_type | STRING | gov_action_proposal.type |
| action_epoch | INT32 | |
| voter_hash | STRING | hot key from voting_procedure |
| cold_key | STRING | mapped via committee_registration |
| vote | STRING | |
| vote_epoch | INT32 | |
| vote_block_time | INT64 | |
| action_block_time | INT64 | gov_action_proposal.block_time |
| time_to_vote_seconds | INT64 | vote_block_time - action_block_time |

---

### 8. `governance_params.parquet` (single-file) — NEW

Governance-relevant protocol parameters per epoch, extracted from `epoch_param.params` JSONB.
Needed for KPIs: 4.9 (Min Attack Vector), contextual thresholds display.

| Column | Type | Source |
|--------|------|--------|
| epoch | INT32 | |
| drep_deposit | INT64 | params→drep_deposit |
| gov_action_deposit | INT64 | params→gov_action_deposit |
| drep_activity | INT32 | params→drep_activity |
| gov_action_lifetime | INT32 | params→gov_action_lifetime |
| committee_min_size | INT32 | params→committee_min_size |
| committee_max_term_length | INT32 | params→committee_max_term_length |
| pvt_motion_no_confidence | FLOAT64 | num/denom ratio |
| pvt_committee_normal | FLOAT64 | |
| pvt_committee_no_confidence | FLOAT64 | |
| pvt_hard_fork_initiation | FLOAT64 | |
| pvt_pp_security_group | FLOAT64 | |
| dvt_motion_no_confidence | FLOAT64 | |
| dvt_committee_normal | FLOAT64 | |
| dvt_committee_no_confidence | FLOAT64 | |
| dvt_update_to_constitution | FLOAT64 | |
| dvt_hard_fork_initiation | FLOAT64 | |
| dvt_pp_network_group | FLOAT64 | |
| dvt_pp_economic_group | FLOAT64 | |
| dvt_pp_technical_group | FLOAT64 | |
| dvt_pp_gov_group | FLOAT64 | |
| dvt_treasury_withdrawal | FLOAT64 | |

**SQL:**
```sql
SELECT
  epoch,
  (params->>'drep_deposit')::bigint AS drep_deposit,
  (params->>'gov_action_deposit')::bigint AS gov_action_deposit,
  (params->>'drep_activity')::int AS drep_activity,
  (params->>'gov_action_lifetime')::int AS gov_action_lifetime,
  (params->>'committee_min_size')::int AS committee_min_size,
  (params->>'committee_max_term_length')::int AS committee_max_term_length,
  -- Pool voting thresholds (numerator/denominator → ratio)
  (params->'pool_voting_thresholds'->'pvt_motion_no_confidence'->>'numerator')::float
    / NULLIF((params->'pool_voting_thresholds'->'pvt_motion_no_confidence'->>'denominator')::float, 0)
    AS pvt_motion_no_confidence,
  -- ... same pattern for all threshold fields
FROM epoch_param
ORDER BY epoch
```

---

## Implementation Steps

### Step 1: Project scaffolding
- Create `pyproject.toml` with dependencies (psycopg[binary]>=3.1, pyarrow>=18.0, click>=8.0, tqdm>=4.0, python-dotenv>=1.0, optional duckdb>=1.4)
- Create `.env.example`
- Create package structure with `__init__.py` files

### Step 2: Core infrastructure
- `config.py` — DB config from env vars (host, port, dbname, user, password, schema)
- `db.py` — Connection factory (psycopg3, dict_row, search_path=yaci_store), epoch discovery (settled vs unsettled), Conway-era detection

### Step 3: Base classes
- `datasets/base.py` — `EpochPartitionedDataset` and `SingleFileDataset` abstract classes with schema(), query methods, and parquet write logic

### Step 4: Implement all 8 datasets
One Python file per dataset, each implementing the SQL and schema.

### Step 5: Extraction orchestrator
- `extract.py` — Loop over datasets, determine needed epochs, extract with progress bars

### Step 6: CLI
- `cli.py` — `extract` command (--only, --output, --full), `create-views` command (--output, --db, --docker)

### Step 7: DuckDB views
- `views.py` — Create views over all 8 parquet files (hive partitioning for epoch-partitioned, direct read for single-file)

### Step 8: Verify
- Run extraction against live yaci-store DB
- Query DuckDB views for sample KPI computations
- Validate row counts and data integrity

---

## Key Design Decisions

1. **Network-agnostic**: No hardcoded thresholds. Governance thresholds are extracted from `epoch_param.params` into `governance_params.parquet`.
2. **Schema prefix**: Connection sets `search_path=yaci_store,public` (learned from previous project).
3. **JSONB extraction**: `voting_stats` and `epoch_param.params` JSONB fields are flattened into typed columns at extraction time (not at query time).
4. **CC cold_key mapping**: `cc_vote_details` joins `committee_registration` to resolve hot_key → cold_key.
5. **Incremental by default**: Only new/unsettled epochs are processed. `--full` flag for complete re-extraction.
6. **Empty epoch skip**: Epoch-partitioned datasets skip writing files for epochs with no data.

---

## Verification

```bash
# Install
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

---

## KPI Analytic Views

The second layer of the pipeline: DuckDB views that compute each KPI as a per-epoch time-series, ready for Superset charting. Views are `CREATE OR REPLACE VIEW` (idempotent) and are created automatically by the existing `create-views` CLI command.

### Architecture

- **Location**: `gov_health/kpis/` package, one module per category
- **Registry**: `kpis/__init__.py` collects `ALL_KPI_VIEWS` from all category modules (mirrors `datasets/__init__.py` pattern)
- **Format**: Each KPI is a `(view_name, create_sql)` tuple
- **Naming convention**: `kpi_<category>_<number>_<short_name>` (e.g. `kpi_1_1_voting_turnout`)
- **Integration**: `views.py` iterates `ALL_KPI_VIEWS` after creating base views — no CLI changes needed
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
| 1.4 New vs Returning Delegators | Needs per-address Ada balances (not in current extraction) |
| 1.6 Stake Registration Rate | Needs `stake_registration` events (not yet extracted) |
| 1.7 Future Delegation Status | Complex cross-correlation of delegation status with activity predictions |

---

## Superset Stack

Docker Compose stack providing Apache Superset with a DuckDB backend, mirroring the proven setup from `ys-to-parquet`.

### Architecture

```
superset/
├── docker-compose.yml         # 3 services: superset, superset-db, superset-redis
├── Dockerfile                 # apache/superset:6.0.0 + duckdb-engine
├── requirements-local.txt     # duckdb, duckdb-engine, psycopg2-binary
├── superset_config.py         # Metadata DB, cache, PREVENT_UNSAFE_DB_CONNECTIONS
├── bootstrap.sh               # Creates DuckDB views + registers database in Superset
└── .env                       # COMPOSE_PROJECT_NAME, SUPERSET_SECRET_KEY
```

### Services

| Service | Image | Purpose |
|---------|-------|---------|
| `superset` | Custom (apache/superset:6.0.0) | Superset web UI on port 8088 |
| `superset-db` | postgres:16-alpine | Superset metadata store |
| `superset-redis` | redis:7-alpine | Cache + Celery broker |

### Volumes

- `../output` → `/data` — parquet files + governance.duckdb
- `../gov_health` → `/app/gov_health` — KPI view SQL (single source of truth)

### Bootstrap (idempotent, runs on every container start)

1. **DuckDB views** — auto-discovers parquet files under `/data`, creates base views, then imports `ALL_KPI_VIEWS` from mounted `gov_health.kpis`
2. **Superset init** — creates admin user (admin/admin), runs migrations, syncs permissions
3. **Database registration** — upserts `Cardano Governance (DuckDB)` datasource with URI `duckdb:////data/governance.duckdb?access_mode=READ_ONLY`

### Usage

```bash
cd superset
docker compose up --build     # http://localhost:8088 (admin / admin)
```

---

## Next: KPI Data Debugging

**Priority task.** KPI 1.1 (voting turnout) returns 0 rows, indicating either:
- `gov_action_lifecycle.drep_yes_vote_stake` is NULL for all rows (voting_stats JSONB not populated in yaci-store)
- The extraction query for lifecycle voting stats has a bug
- yaci-store configuration issue (voting stats computation may need to be enabled)

### Debugging plan

1. Check raw `gov_action_lifecycle.parquet` — are drep stake columns all NULL?
2. If NULL: query yaci-store `gov_action_proposal.voting_stats` JSONB directly to see what's there
3. If JSONB is empty: yaci-store may not compute voting stats — check yaci-store config / version
4. If JSONB has data but extraction is wrong: fix the JSONB field path in `gov_action_lifecycle.py`
5. Validate remaining KPIs (1.2, 1.3, 1.5, 1.8) produce sensible values
