# Cardano Governance Health — Analysis

> Based on the [Governance Health KPI Report v1.0](https://gov-health.intersectmbo.org/) (Intersect Civics Committee, 3 Dec 2025) and the yaci-store PostgreSQL schema.

---

## Part 1: KPI Definitions

### Framework

Governance health is defined as:

> "A composite state where the system is accessible, decentralized, efficient, functioning and upholds principles of fairness, equality and protection from undue influence."

Five measurement buckets:
1. **Participation** — Are people showing up?
2. **Distribution of Power** — Is influence concentrated?
3. **Performance & Effectiveness** — Are decisions timely?
4. **Constitutional Alignment** — Are checks and balances working?
5. **Tooling, UX & Data Parity** — Is the barrier to entry low?

### The 7 Core KPIs

| # | KPI | Dimension | Formula |
|---|-----|-----------|---------|
| 1 | **Voting Turnout (% Ada)** | Participation | Total ADA voted / Circulating ADA |
| 2 | **DRep Activity Rate** | DRep Health | DReps voting on >= X% actions / Registered DReps |
| 3 | **Delegation Decentralization (Gini)** | Decentralization | Gini coefficient on ADA delegated per DRep |
| 4 | **SPO Voting Turnout** | SPO Engagement | SPOs who voted / Active SPOs (stake-weighted) |
| 5 | **Treasury Balance Rate** | Financial | ADA spent / ADA added per calendar year |
| 6 | **DRep Rationale Rate** | Transparency | Votes with rationale / Total DRep votes |
| 7 | **CC Voting Turnout** | CC Participation | CC members voting / Total CC members |

---

### Category 1: Ada Holder Participation (8 metrics)

| ID | Metric | Formula | Status |
|----|--------|---------|--------|
| 1.1 | **Voting Turnout (% Ada)** | Total ADA voted on a governance action / Circulating ADA | Core |
| 1.2 | **Active Stake Address Participation** | Count of unique stake addresses that voted or delegated to DReps | Included |
| 1.3 | **Delegation Rate (% Ada)** | ADA delegated to DReps / Circulating ADA | Included |
| 1.4 | **Delegation Distribution by Wallet Size** | Group wallets by ADA tier; compute % delegation per tier | Included |
| 1.5 | **Delegation Churn** | Wallets changing DRep / Total delegated wallets (per epoch) | Future |
| 1.6 | **New Wallet Delegation Rate** | New wallets delegating / New wallets × 100 | Included |
| 1.7 | **Post-Vote Delegation Flow** | Sum of ADA redelegated away from DReps voting X on action Y within Z epochs | Future |
| 1.8 | **Inactive Delegated Ada** | Total ADA delegated to inactive/retired DReps | Included |

### Category 2: DRep Insights & Activity (12 metrics)

| ID | Metric | Formula | Status |
|----|--------|---------|--------|
| 2.1 | **Delegation Decentralization (Gini)** | Gini coefficient on ADA delegated to active DReps | Core |
| 2.2 | **DRep Activity Rate** | DReps voting on >= X% actions / Registered DReps | Core |
| 2.3 | **DRep Rationale Rate** | Votes with rationale / Total DRep votes | Core |
| 2.4 | **Time-to-Vote** | Median time between action submission and DRep vote cast | Included |
| 2.5 | **DRep Voting Correlation** | Avg pairwise vote-direction correlation among top-N DReps | Future |
| 2.6 | **DRep Lifecycle Rate** | New registrations − de-registrations per epoch | Included |
| 2.7 | **DRep Vote Change Rate** | Count of vote changes (same voter, same action, different vote) | Included |
| 2.8 | **DRep Seniority Distribution** | Histogram of DRep registration dates | Included |
| 2.9 | **DRep-CC Vote Latency** | Median time between CC decision and DRep vote | Future |
| 2.10 | **DRep Metadata Completeness** | % DReps with complete minimal metadata (CIP-100/108) | Included |
| 2.11 | **Top-100 DRep Concentration Volatility** | StdDev over rolling W epochs of % ADA held by top-100 DReps | Future |
| 2.12 | **Seasonality of Voting** | Avg turnout per period; YoY same-period % change | Included |

### Category 3: SPO Governance Participation (7 metrics)

| ID | Metric | Formula | Status |
|----|--------|---------|--------|
| 3.1 | **SPO Voting Turnout** | SPOs who voted / Active SPOs (stake-weighted) | Core |
| 3.2 | **SPO Silent Stake Rate** | Non-voting active-pool stake / Active stake | Included |
| 3.3 | **Default Stance Adoption** | % pools using Abstain/No-Confidence defaults | Included |
| 3.4 | **Ratification Latency** | Median time from vote window open to SPO vote | Included |
| 3.5 | **Entity Voting Power Concentration** | HHI/Gini on entity-aggregated stake | Included |
| 3.6 | **SPO Vote Change Frequency** | Count of vote-change transactions per SPO | Future |
| 3.7 | **SPO vs DRep Vote Divergence** | Difference in voting outcomes between SPO and DRep bodies | Future |

### Category 4: Governance Action & Treasury Health (9 metrics)

| ID | Metric | Formula | Status |
|----|--------|---------|--------|
| 4.1 | **Gov Action Volume & Source** | Count per month by submitter type | Included |
| 4.2 | **Gov Action Contention Rate** | Distribution of Yes/No/Abstain; share of close outcomes | Included |
| 4.3 | **Treasury Balance Rate** | ADA spent / ADA added per calendar year | Core |
| 4.4 | **Gov Action Consequence Analysis** | % actions with standardized consequence section | Future |
| 4.5 | **Time-to-Enactment** | Median days from submission to enactment | Included |
| 4.6 | **Treasury Spend vs NCL** | Treasury spend rate / Net Change Limit | Included |
| 4.7 | **Constitutional Compliance Clarity** | % actions with zero "No" votes by CC | Included |
| 4.8 | **Success Rate by Source** | Approved actions / Submitted actions per source | Included |
| 4.9 | **Min Attack Vector** | Number of actors needed to collude to meet approval threshold | Future |

### Category 5: Constitutional Committee Activity (5 metrics)

| ID | Metric | Formula | Status |
|----|--------|---------|--------|
| 5.1 | **Time-to-Decision** | Median days from governance action submission to CC vote | Included |
| 5.2 | **CC Member Participation Rate** | Actions voted on by member / Total actions | Included |
| 5.3 | **CC Abstain Rate** | Count of 'Abstain' / Total CC votes | Included |
| 5.4 | **CC Vote Agreement Rate** | % of actions with non-unanimous CC vote | Included |
| 5.5 | **CC Off-Chain Election Turnout** | Total ADA voted in CC election / Circulating ADA | Included |

### Category 6: Tooling & UX (6 metrics)

| ID | Metric | Formula | Status |
|----|--------|---------|--------|
| 6.1 | **Submission Path Share** | % actions submitted via CLI vs GUI | Included |
| 6.2 | **Proposer Onboarding Rate** | Completed submissions / Initiated drafts | Included |
| 6.3 | **Deposit Cost Burden** | ADA deposit requirement indexed to FX basket | Included |
| 6.4 | **Access Friction Index** | Composite of steps, errors, time-to-submit | Future |
| 6.5 | **Info Availability** | % actions with human-readable vote pages | Included |
| 6.6 | **Governance Data Parity** | Data consistency delta between explorers | Included |

### Excluded Metrics

| Metric | Reason |
|--------|--------|
| DRep "Competence" Score | Too subjective |
| Social Media Sentiment | Prone to manipulation |
| Rational Ada Holder Index | Unclear causality |
| DeFi Governance Correlation | Insufficient entity tagging |

### Summary

| Category | Total | Core | Included | Future |
|----------|-------|------|----------|--------|
| 1. Ada Holder Participation | 8 | 1 | 5 | 2 |
| 2. DRep Insights & Activity | 12 | 3 | 5 | 4 |
| 3. SPO Governance Participation | 7 | 1 | 4 | 2 |
| 4. Gov Action & Treasury | 9 | 1 | 5 | 3 |
| 5. Constitutional Committee | 5 | 1 | 4 | 0 |
| 6. Tooling & UX | 6 | 0 | 5 | 1 |
| **Total** | **47** | **7** | **28** | **12** |

> **Note:** Category 6 (Tooling & UX) requires off-chain/portal analytics data not available in yaci-store. These metrics are out of scope for this project.

---

## Part 2: Yaci-Store Database Schema

### Overview

Yaci-store uses Flyway migrations to manage its PostgreSQL schema. Tables are organized across multiple modules:
- `stores/governance/` — core governance tables
- `aggregates/governance-aggr/` — aggregated governance state (drep_dist, proposal status, voting stats)
- `aggregates/adapot/` — ADA pot, rewards, epoch stake
- `aggregates/epoch-aggr/` — epoch summary
- `stores/epoch/` — protocol parameters
- `stores/staking/` — pools, delegations, stake registrations
- `stores/blocks/` — blocks
- `stores/transaction/` — transactions, UTXOs, balances

Partitioned tables (by epoch): `drep_dist`, `epoch_stake`, `reward`.

---

### Governance Tables

#### `gov_action_proposal`

Per governance action submitted on-chain.

| Column | Type | Notes |
|--------|------|-------|
| tx_hash | varchar(64) | PK part 1 |
| idx | int | PK part 2 |
| tx_index | int | |
| deposit | bigint | Deposit in lovelace |
| return_address | varchar(255) | |
| anchor_url | varchar | Proposal metadata URL |
| anchor_hash | varchar(64) | |
| type | varchar(50) | See enum below |
| details | **jsonb** | Structure varies by type (see below) |
| epoch | int | |
| slot | bigint | |
| block | bigint | |
| block_time | bigint | Unix timestamp |

**Indexes:** slot, tx_hash, return_address, type

**`type` enum values:**
- `PARAMETER_CHANGE_ACTION`
- `HARD_FORK_INITIATION_ACTION`
- `TREASURY_WITHDRAWALS_ACTION`
- `NO_CONFIDENCE`
- `UPDATE_COMMITTEE`
- `NEW_CONSTITUTION`
- `INFO_ACTION`

**`details` JSONB structure** — free-form `JsonNode`, varies by `type`:
- `PARAMETER_CHANGE_ACTION`: proposed parameter changes as key-value pairs
- `TREASURY_WITHDRAWALS_ACTION`: withdrawal addresses and amounts
- `UPDATE_COMMITTEE`: committee member additions/removals and threshold changes
- `HARD_FORK_INITIATION_ACTION`: target protocol version
- `NEW_CONSTITUTION`: anchor and optional script hash

---

#### `gov_action_proposal_status`

Per-action, per-epoch status snapshots with voting stats.

| Column | Type | Notes |
|--------|------|-------|
| gov_action_tx_hash | varchar(64) | PK part 1 |
| gov_action_index | int | PK part 2 |
| type | varchar(50) | GovActionType enum |
| status | varchar(20) | `ACTIVE`, `RATIFIED`, `EXPIRED` |
| voting_stats | **jsonb** | Detailed vote breakdown (see below) |
| epoch | int | PK part 3 |

**`voting_stats` JSONB structure** (from `ProposalVotingStats.java`):
```json
{
  "cc_yes": 5,
  "cc_no": 1,
  "cc_abstain": 0,
  "cc_do_not_vote": 1,
  "cc_approval_ratio": 0.833,

  "drep_yes_vote_stake": 1234567890,
  "drep_no_vote_stake": 234567890,
  "drep_abstain_vote_stake": 34567890,
  "drep_no_confidence_stake": 4567890,
  "drep_auto_abstain_stake": 567890,
  "drep_do_not_vote_stake": 67890,
  "drep_total_yes_stake": 1234567890,
  "drep_total_no_stake": 234567890,
  "drep_total_abstain_stake": 34567890,
  "drep_approval_ratio": 0.75,

  "spo_yes_vote_stake": 9876543210,
  "spo_no_vote_stake": 876543210,
  "spo_abstain_vote_stake": 76543210,
  "spo_do_not_vote_stake": 6543210,
  "spo_total_yes_stake": 9876543210,
  "spo_total_no_stake": 876543210,
  "spo_total_abstain_stake": 76543210,
  "spo_approval_ratio": 0.90
}
```

All stake values are in lovelace (BigInteger). Approval ratios are BigDecimal.

---

#### `voting_procedure`

Individual votes cast by DReps, SPOs, and CC members.

| Column | Type | Notes |
|--------|------|-------|
| id | uuid | |
| tx_hash | varchar(64) | PK part 1 |
| idx | int | |
| tx_index | int | |
| voter_type | varchar(50) | PK part 2 (see enum) |
| voter_hash | varchar(56) | PK part 3 |
| gov_action_tx_hash | varchar(64) | PK part 4 |
| gov_action_index | int | PK part 5 |
| vote | varchar(10) | `YES`, `NO`, `ABSTAIN` |
| anchor_url | varchar | Rationale URL (CIP-100/108) |
| anchor_hash | varchar(64) | |
| epoch | int | |
| slot | bigint | |
| block | bigint | |
| block_time | bigint | |

**`voter_type` enum:**
- `CONSTITUTIONAL_COMMITTEE_HOT_KEY_HASH`
- `CONSTITUTIONAL_COMMITTEE_HOT_SCRIPT_HASH`
- `DREP_KEY_HASH`
- `DREP_SCRIPT_HASH`
- `STAKING_POOL_KEY_HASH`

**Indexes:** slot, (voter_hash, voter_type), (gov_action_tx_hash, gov_action_index), tx_hash

---

#### `drep`

DRep registration lifecycle events.

| Column | Type | Notes |
|--------|------|-------|
| drep_id | varchar(255) | Bech32 DRep ID |
| drep_hash | varchar(56) | PK part 1 |
| tx_hash | varchar(64) | PK part 2 |
| cert_index | int | PK part 3 |
| tx_index | int | |
| cert_type | varchar(40) | CertificateType enum |
| status | varchar(50) | `REGISTERED`, `UPDATED`, `RETIRED` |
| deposit | bigint | |
| epoch | int | |
| registration_slot | bigint | |
| slot | bigint | PK part 4 |
| block | bigint | |
| block_time | bigint | |

**Indexes:** slot, drep_id, epoch

---

#### `drep_registration`

DRep registration/update/retirement certificate events.

| Column | Type | Notes |
|--------|------|-------|
| tx_hash | varchar(64) | PK part 1 |
| cert_index | int | PK part 2 |
| tx_index | int | |
| type | varchar(50) | CertificateType |
| deposit | bigint | |
| drep_hash | varchar(56) | |
| drep_id | varchar(255) | Bech32 |
| anchor_url | varchar | Metadata URL |
| anchor_hash | varchar(64) | |
| cred_type | varchar(40) | |
| epoch | int | |
| slot | bigint | |
| block | bigint | |
| block_time | bigint | |

**Indexes:** slot, type, epoch, drep_hash

---

#### `drep_dist` (partitioned by epoch)

DRep stake distribution snapshot per epoch. This is the key table for delegation analysis.

| Column | Type | Notes |
|--------|------|-------|
| drep_hash | varchar(56) | PK part 1 |
| drep_type | varchar(40) | PK part 2 |
| drep_id | varchar(255) | Bech32 |
| amount | bigint | Delegated stake in lovelace |
| epoch | int | PK part 3 |
| active_until | int | Epoch until which DRep remains active |
| expiry | int | Epoch at which DRep expires |

**`drep_type` enum:** `ADDR_KEYHASH`, `SCRIPTHASH`, `ABSTAIN`, `NO_CONFIDENCE`

**Indexes:** drep_hash, epoch

---

#### `delegation_vote`

Stake address → DRep delegation events.

| Column | Type | Notes |
|--------|------|-------|
| tx_hash | varchar(64) | PK part 1 |
| cert_index | int | PK part 2 |
| tx_index | int | |
| address | varchar(255) | Bech32 stake address |
| drep_hash | varchar(56) | |
| drep_id | varchar(255) | Bech32 |
| drep_type | varchar(40) | DrepType enum |
| epoch | int | |
| credential | varchar(56) | |
| cred_type | varchar(40) | |
| slot | bigint | |
| block | bigint | |
| block_time | bigint | |

**Indexes:** slot, epoch, address, drep_id

---

#### `committee`

Constitutional Committee state per epoch.

| Column | Type | Notes |
|--------|------|-------|
| gov_action_tx_hash | varchar(64) | Gov action that created/updated the committee |
| gov_action_index | int | |
| threshold_numerator | bigint | |
| threshold_denominator | bigint | |
| threshold | double | Computed threshold ratio |
| epoch | int | PK |
| slot | bigint | |

---

#### `committee_member`

CC member records.

| Column | Type | Notes |
|--------|------|-------|
| hash | varchar(56) | PK part 1 — credential hash |
| cred_type | varchar(40) | |
| start_epoch | int | |
| expired_epoch | int | |
| epoch | int | |
| slot | bigint | PK part 2 |

---

#### `committee_registration`

CC hot key authorization certificates (cold_key → hot_key mapping).

| Column | Type | Notes |
|--------|------|-------|
| tx_hash | varchar(64) | PK part 1 |
| cert_index | int | PK part 2 |
| tx_index | int | |
| cold_key | varchar | |
| hot_key | varchar | |
| cred_type | varchar(40) | |
| epoch | int | |
| slot | bigint | |
| block_time | bigint | |

---

#### `committee_deregistration`

CC resignation certificates.

| Column | Type | Notes |
|--------|------|-------|
| tx_hash | varchar(64) | PK part 1 |
| cert_index | int | PK part 2 |
| tx_index | int | |
| anchor_url | varchar | |
| anchor_hash | varchar(64) | |
| cold_key | varchar | |
| cred_type | varchar(40) | |
| epoch | int | |
| slot | bigint | |
| block_time | bigint | |

---

#### `committee_state`

CC state per epoch (normal or no-confidence).

| Column | Type | Notes |
|--------|------|-------|
| epoch | int | PK |
| state | varchar(20) | `NORMAL` or `NO_CONFIDENCE` |

---

#### `constitution`

Active constitution per epoch.

| Column | Type | Notes |
|--------|------|-------|
| active_epoch | int | PK |
| anchor_url | varchar | |
| anchor_hash | varchar(64) | |
| script | varchar(64) | Guardrails script hash |
| slot | bigint | |

---

#### `gov_epoch_activity`

Governance epoch dormancy tracking.

| Column | Type | Notes |
|--------|------|-------|
| epoch | int | PK |
| dormant | boolean | Whether epoch is dormant for governance |
| dormant_epoch_count | int | Consecutive dormant epoch count |

---

### Epoch & Protocol Tables

#### `epoch`

Epoch summary statistics.

| Column | Type | Notes |
|--------|------|-------|
| number | bigint | PK |
| block_count | int | |
| transaction_count | bigint | |
| total_output | numeric(38) | |
| total_fees | bigint | |
| start_time | bigint | Unix timestamp |
| end_time | bigint | Unix timestamp |
| max_slot | bigint | |

---

#### `epoch_param`

Protocol parameters per epoch.

| Column | Type | Notes |
|--------|------|-------|
| epoch | int | PK |
| params | **jsonb** | Full protocol parameters |
| cost_model_hash | varchar(64) | |
| slot | bigint | |
| block_time | bigint | |

**`params` JSONB** contains all Cardano protocol parameters including governance thresholds:
- `drep_deposit`, `gov_action_deposit`
- `committee_min_size`
- DRep voting thresholds per action type
- SPO voting thresholds per action type
- `drep_activity` (epoch count for DRep inactivity)
- Standard protocol params (min_fee_a/b, max_block_size, etc.)

---

### Ada Pot & Staking Tables

#### `adapot`

ADA distribution snapshot per epoch.

| Column | Type | Notes |
|--------|------|-------|
| epoch | int | PK |
| slot | bigint | |
| deposits_stake | numeric(38) | |
| fees | numeric(38) | |
| utxo | numeric(38) | |
| treasury | numeric(38) | |
| reserves | numeric(38) | |
| circulation | numeric(38) | |
| distributed_rewards | numeric(38) | |
| undistributed_rewards | numeric(38) | |
| rewards_pot | numeric(38) | |
| pool_rewards_pot | numeric(38) | |

---

#### `epoch_stake` (partitioned by epoch)

Per-address stake snapshot per epoch.

| Column | Type | Notes |
|--------|------|-------|
| epoch | int | PK part 1 |
| address | varchar(255) | PK part 2 — stake address |
| amount | numeric(38) | Staked lovelace |
| pool_id | varchar(56) | Pool hash |
| delegation_epoch | int | |
| active_epoch | int | |

**Indexes:** (epoch, pool_id)

---

### Pool Tables

#### `pool`

Pool lifecycle events.

| Column | Type | Notes |
|--------|------|-------|
| pool_id | varchar(56) | PK part 1 — pool hash |
| tx_hash | varchar(64) | PK part 2 |
| cert_index | int | PK part 3 |
| tx_index | int | |
| status | varchar(50) | `REGISTRATION`, `UPDATE`, `RETIRING`, `RETIRED` |
| amount | numeric(38) | |
| epoch | int | |
| active_epoch | int | |
| retire_epoch | int | |
| registration_slot | bigint | |
| slot | bigint | PK part 4 |
| block | bigint | |
| block_time | bigint | |

**Indexes:** slot, pool_id, epoch, retire_epoch

---

#### `pool_registration`

Pool registration certificates with metadata.

| Column | Type | Notes |
|--------|------|-------|
| tx_hash | varchar(64) | PK part 1 |
| cert_index | int | PK part 2 |
| tx_index | int | |
| pool_id | varchar(56) | |
| vrf_key | varchar(64) | |
| pledge | numeric(20,0) | |
| cost | numeric(20,0) | |
| margin | double | |
| reward_account | varchar(255) | |
| pool_owners | **jsonb** | Array of owner key hashes |
| relays | **jsonb** | Array of relay objects |
| metadata_url | text | |
| metadata_hash | varchar(64) | |
| epoch | int | |
| slot | bigint | |
| block_time | bigint | |

**`pool_owners` JSONB:** `["<key_hash_hex>", ...]`

**`relays` JSONB:**
```json
[
  {"port": 3001, "ipv4": "1.2.3.4", "ipv6": null, "dnsName": null},
  {"port": null, "ipv4": null, "ipv6": null, "dnsName": "relay.example.com"}
]
```

---

#### `delegation`

Stake delegation to pools (not governance — see `delegation_vote` for DRep delegation).

| Column | Type | Notes |
|--------|------|-------|
| tx_hash | varchar(64) | PK part 1 |
| cert_index | int | PK part 2 |
| credential | varchar(56) | |
| pool_id | varchar(56) | |
| address | varchar(255) | Bech32 stake address |
| epoch | int | |
| slot | bigint | |
| block_time | bigint | |

---

### Other Relevant Tables

#### `stake_registration`

Stake key registration/deregistration events.

| Column | Type | Notes |
|--------|------|-------|
| tx_hash | varchar(64) | PK part 1 |
| cert_index | int | PK part 2 |
| credential | varchar(56) | |
| type | varchar(50) | CertificateType |
| address | varchar(255) | Bech32 stake address |
| epoch | int | |
| slot | bigint | |
| block_time | bigint | |

---

#### `local_treasury_withdrawal`

Enacted treasury withdrawals.

| Column | Type | Notes |
|--------|------|-------|
| gov_action_tx_hash | varchar(64) | PK part 1 |
| gov_action_index | int | PK part 2 |
| address | varchar(255) | PK part 3 |
| amount | bigint | |
| epoch | int | |
| slot | bigint | |

---

#### `block`

Block data (for slot leader / SPO block production cross-reference).

| Column | Type | Notes |
|--------|------|-------|
| hash | varchar(64) | PK |
| number | bigint | |
| epoch | int | |
| slot | bigint | |
| slot_leader | varchar(56) | Pool hash of block producer |
| block_time | bigint | |
| no_of_txs | int | |
| total_fees | bigint | |

---

## Part 3: KPI → Table Mapping

### On-Chain Feasibility

Below maps each KPI to the yaci-store tables needed to compute it.

#### Category 1: Ada Holder Participation

| KPI | Tables Needed | Feasibility |
|-----|---------------|-------------|
| 1.1 Voting Turnout (% Ada) | `gov_action_proposal_status` (voting_stats JSONB → drep/spo stake), `adapot` (circulation) | **Full** — voting_stats has all the stake totals needed |
| 1.2 Active Stake Address Participation | `delegation_vote` (unique addresses), `voting_procedure` (indirect via DReps) | **Partial** — delegation addresses yes; individual voter addresses only visible through DRep delegation, not direct votes |
| 1.3 Delegation Rate (% Ada) | `drep_dist` (SUM amount), `adapot` (circulation) | **Full** |
| 1.4 Delegation Distribution by Wallet Size | `delegation_vote` + `stake_address_balance` or `epoch_stake` | **Full** — join delegation_vote addresses with their balances |
| 1.5 Delegation Churn | `delegation_vote` (self-join by address, ordered by slot) | **Full** |
| 1.6 New Wallet Delegation Rate | `stake_registration` + `delegation_vote` (match by epoch) | **Full** |
| 1.7 Post-Vote Delegation Flow | `delegation_vote` + `voting_procedure` + time correlation | **Full** (complex query) |
| 1.8 Inactive Delegated Ada | `drep_dist` (active_until < current epoch, or drep status=RETIRED) | **Full** |

#### Category 2: DRep Insights & Activity

| KPI | Tables Needed | Feasibility |
|-----|---------------|-------------|
| 2.1 Delegation Decentralization (Gini) | `drep_dist` (amount per drep per epoch) | **Full** |
| 2.2 DRep Activity Rate | `voting_procedure` (voter_type DREP), `drep` (status), `gov_action_proposal_status` (active actions per epoch) | **Full** |
| 2.3 DRep Rationale Rate | `voting_procedure` (anchor_url IS NOT NULL / total, voter_type DREP) | **Full** |
| 2.4 Time-to-Vote | `voting_procedure` (block_time) − `gov_action_proposal` (block_time) | **Full** |
| 2.5 DRep Voting Correlation | `voting_procedure` (voter_type DREP, build vote matrix) | **Full** (compute in Python/DuckDB) |
| 2.6 DRep Lifecycle Rate | `drep` or `drep_registration` (type=REG vs DEREG per epoch) | **Full** |
| 2.7 DRep Vote Change Rate | `voting_procedure` (multiple votes per voter+action, ordered by slot) | **Full** |
| 2.8 DRep Seniority Distribution | `drep_registration` (first registration block_time per drep) | **Full** |
| 2.9 DRep-CC Vote Latency | `voting_procedure` (compare CC vote time vs DRep vote time per action) | **Full** |
| 2.10 DRep Metadata Completeness | `drep_registration` (anchor_url IS NOT NULL) | **Full** |
| 2.11 Top-100 DRep Concentration Volatility | `drep_dist` (rolling window on top-100 by amount) | **Full** |
| 2.12 Seasonality of Voting | `voting_procedure` + `gov_action_proposal` (by calendar period) | **Full** |

#### Category 3: SPO Governance Participation

| KPI | Tables Needed | Feasibility |
|-----|---------------|-------------|
| 3.1 SPO Voting Turnout | `voting_procedure` (voter_type STAKING_POOL_KEY_HASH), `pool` (active), `epoch_stake` (stake amounts) | **Full** |
| 3.2 SPO Silent Stake Rate | Same as 3.1, inverted | **Full** |
| 3.3 Default Stance Adoption | `delegation_vote` (drep_type IN ABSTAIN, NO_CONFIDENCE) cross-ref with `pool_registration` (pool_owners) | **Full** (requires credential matching) |
| 3.4 Ratification Latency | `voting_procedure` (voter_type SPO, block_time) − `gov_action_proposal` (block_time) | **Full** |
| 3.5 Entity Voting Power Concentration | `epoch_stake` + external entity registry | **Partial** — no entity registry in yaci-store |
| 3.6 SPO Vote Change Frequency | `voting_procedure` (voter_type SPO, same pattern as 2.7) | **Full** |
| 3.7 SPO vs DRep Vote Divergence | `gov_action_proposal_status` (voting_stats: compare spo_approval_ratio vs drep_approval_ratio) | **Full** |

#### Category 4: Governance Action & Treasury Health

| KPI | Tables Needed | Feasibility |
|-----|---------------|-------------|
| 4.1 Gov Action Volume & Source | `gov_action_proposal` (type, epoch, return_address) | **Full** |
| 4.2 Gov Action Contention Rate | `gov_action_proposal_status` (voting_stats JSONB) | **Full** |
| 4.3 Treasury Balance Rate | `adapot` (treasury per epoch) + `local_treasury_withdrawal` | **Full** |
| 4.4 Gov Action Consequence Analysis | Requires off-chain metadata parsing (anchor_url content) | **Off-chain** |
| 4.5 Time-to-Enactment | `gov_action_proposal` (block_time) + `gov_action_proposal_status` (epoch when RATIFIED) | **Full** |
| 4.6 Treasury Spend vs NCL | `adapot` + `local_treasury_withdrawal` + NCL config | **Partial** — NCL not in DB, needs external config |
| 4.7 Constitutional Compliance Clarity | `gov_action_proposal_status` (voting_stats → cc_no = 0) | **Full** |
| 4.8 Success Rate by Source | `gov_action_proposal` (return_address) + `gov_action_proposal_status` (status=RATIFIED) | **Full** |
| 4.9 Min Attack Vector | `drep_dist` + `epoch_stake` + `epoch_param` (thresholds) | **Full** (complex computation) |

#### Category 5: Constitutional Committee Activity

| KPI | Tables Needed | Feasibility |
|-----|---------------|-------------|
| 5.1 Time-to-Decision | `voting_procedure` (CC voter_types, block_time) − `gov_action_proposal` (block_time) | **Full** |
| 5.2 CC Member Participation Rate | `voting_procedure` (CC types) + `committee_member` (active members) | **Full** — need hot_key→cold_key mapping from `committee_registration` |
| 5.3 CC Abstain Rate | `voting_procedure` (CC types, vote=ABSTAIN / total) | **Full** |
| 5.4 CC Vote Agreement Rate | `voting_procedure` (CC types, compare votes per action) | **Full** |
| 5.5 CC Off-Chain Election Turnout | Off-chain election data | **Not available** |

#### Category 6: Tooling & UX

All 6 metrics (6.1–6.6) require portal analytics, explorer data, or FX data — **not available** in yaci-store.

---

### Feasibility Summary

| Feasibility | Count | KPIs |
|-------------|-------|------|
| **Full** | 33 | 1.1–1.8, 2.1–2.12, 3.1–3.4, 3.6–3.7, 4.1–4.3, 4.5, 4.7–4.9, 5.1–5.4 |
| **Partial** | 2 | 3.5 (needs entity registry), 4.6 (needs NCL config) |
| **Off-chain only** | 1 | 4.4 (needs anchor content parsing) |
| **Not available** | 7 | 5.5, 6.1–6.6 |
| **Total on-chain feasible** | **35 / 47** | |

---

## Part 4: Key JSONB Columns Reference

These are the structured JSON columns most important for governance analytics:

### 1. `gov_action_proposal_status.voting_stats`

**Source class:** `ProposalVotingStats.java` in `aggregates/governance-aggr`

This is the single most valuable column for many KPIs. It contains pre-computed vote tallies for each governance body (CC, DReps, SPOs) per action per epoch. Fields:

| Field | Type | Description |
|-------|------|-------------|
| cc_yes | int | CC members voting YES |
| cc_no | int | CC members voting NO |
| cc_abstain | int | CC members voting ABSTAIN |
| cc_do_not_vote | int | CC members not voting |
| cc_approval_ratio | decimal | cc_yes / (cc_yes + cc_no) |
| drep_yes_vote_stake | bigint | Stake of DReps explicitly voting YES |
| drep_no_vote_stake | bigint | Stake of DReps explicitly voting NO |
| drep_abstain_vote_stake | bigint | Stake of DReps explicitly voting ABSTAIN |
| drep_no_confidence_stake | bigint | Stake delegated to no-confidence DRep |
| drep_auto_abstain_stake | bigint | Stake delegated to auto-abstain DRep |
| drep_do_not_vote_stake | bigint | Stake of active DReps not voting |
| drep_total_yes_stake | bigint | Total effective YES stake |
| drep_total_no_stake | bigint | Total effective NO stake |
| drep_total_abstain_stake | bigint | Total effective abstain stake |
| drep_approval_ratio | decimal | yes / (yes + no) |
| spo_yes_vote_stake | bigint | Stake of SPOs explicitly voting YES |
| spo_no_vote_stake | bigint | Stake of SPOs explicitly voting NO |
| spo_abstain_vote_stake | bigint | Stake of SPOs explicitly voting ABSTAIN |
| spo_do_not_vote_stake | bigint | Stake of active SPOs not voting |
| spo_total_yes_stake | bigint | Total effective YES stake |
| spo_total_no_stake | bigint | Total effective NO stake |
| spo_total_abstain_stake | bigint | Total effective abstain stake |
| spo_approval_ratio | decimal | yes / (yes + no) |

### 2. `epoch_param.params`

Full protocol parameters including governance-specific thresholds. Key governance fields:
- `drep_deposit` — deposit required for DRep registration
- `gov_action_deposit` — deposit required to submit a governance action
- `committee_min_size` — minimum CC size
- `drep_activity` — number of epochs before DRep is considered inactive
- DRep/SPO voting thresholds per governance action type

### 3. `gov_action_proposal.details`

Free-form JSON varying by proposal type. Structure is action-type-specific and would need type-aware parsing for detailed analysis.

### 4. `pool_registration.pool_owners`

Simple JSON array of hex key hashes: `["abc123...", "def456..."]`. Needed for SPO default stance computation (matching pool owner credentials to delegation_vote entries).
