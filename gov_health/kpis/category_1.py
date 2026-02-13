"""Category 1 — Ada Holder Participation KPI views.

Each entry is a (view_name, create_sql) tuple.  The SQL runs against the
base DuckDB views created from Parquet files (epoch_summary, delegation_events,
drep_epoch_stats, gov_action_lifecycle).

Deferred KPIs (data gaps):
  1.4 — needs per-address Ada balances (not in current extraction)
  1.6 — needs stake_registration events (not yet extracted)
  1.7 — Future delegation status, complex cross-correlation
"""

# ---------------------------------------------------------------------------
# KPI 1.1  Voting Turnout (% Ada)
# ---------------------------------------------------------------------------
# For each epoch, average DRep participating stake across governance actions
# divided by circulating Ada supply.  Only considers actions with DRep vote
# data (non-null drep stakes).
# ---------------------------------------------------------------------------
KPI_1_1 = (
    "kpi_1_1_voting_turnout",
    """
    CREATE OR REPLACE VIEW kpi_1_1_voting_turnout AS
    SELECT
        l.epoch_proposed                                          AS epoch,
        COUNT(*)                                                  AS actions_count,
        AVG(l.drep_yes_vote_stake
            + l.drep_no_vote_stake
            + l.drep_abstain_vote_stake)                          AS avg_participating_stake,
        e.circulation,
        ROUND(
            AVG(l.drep_yes_vote_stake
                + l.drep_no_vote_stake
                + l.drep_abstain_vote_stake)
            * 100.0 / NULLIF(e.circulation, 0),
            4
        )                                                         AS turnout_pct
    FROM gov_action_lifecycle l
    JOIN epoch_summary e ON e.epoch = l.epoch_proposed
    WHERE l.drep_yes_vote_stake IS NOT NULL
    GROUP BY l.epoch_proposed, e.circulation
    ORDER BY l.epoch_proposed
    """,
)

# ---------------------------------------------------------------------------
# KPI 1.2  Active Stake Address Participation
# ---------------------------------------------------------------------------
# Per-epoch count of distinct addresses making delegation events, plus a
# cumulative count of unique addresses that have ever delegated.
# ---------------------------------------------------------------------------
KPI_1_2 = (
    "kpi_1_2_active_stake_participation",
    """
    CREATE OR REPLACE VIEW kpi_1_2_active_stake_participation AS
    WITH first_delegation AS (
        SELECT address, MIN(epoch) AS first_epoch
        FROM delegation_events
        GROUP BY address
    ),
    per_epoch AS (
        SELECT epoch, COUNT(DISTINCT address) AS active_addresses
        FROM delegation_events
        GROUP BY epoch
    ),
    new_per_epoch AS (
        SELECT first_epoch AS epoch, COUNT(*) AS new_addresses
        FROM first_delegation
        GROUP BY first_epoch
    )
    SELECT
        p.epoch,
        p.active_addresses,
        COALESCE(n.new_addresses, 0)                              AS new_addresses,
        SUM(COALESCE(n.new_addresses, 0))
            OVER (ORDER BY p.epoch)                               AS cumulative_unique_addresses
    FROM per_epoch p
    LEFT JOIN new_per_epoch n ON n.epoch = p.epoch
    ORDER BY p.epoch
    """,
)

# ---------------------------------------------------------------------------
# KPI 1.3  Delegation Rate (% Ada)
# ---------------------------------------------------------------------------
# Trivial ratio: total Ada delegated to DReps / circulating supply.
# ---------------------------------------------------------------------------
KPI_1_3 = (
    "kpi_1_3_delegation_rate",
    """
    CREATE OR REPLACE VIEW kpi_1_3_delegation_rate AS
    SELECT
        epoch,
        total_drep_delegated,
        circulation,
        ROUND(
            total_drep_delegated * 100.0 / NULLIF(circulation, 0),
            4
        )                                                         AS delegation_rate_pct
    FROM epoch_summary
    ORDER BY epoch
    """,
)

# ---------------------------------------------------------------------------
# KPI 1.5  Delegation Churn
# ---------------------------------------------------------------------------
# Detects DRep changes per stake address using LAG() over delegation_events
# ordered by slot.  A "change" is when the same address re-delegates to a
# different DRep (hash or type differs from previous delegation).
# ---------------------------------------------------------------------------
KPI_1_5 = (
    "kpi_1_5_delegation_churn",
    """
    CREATE OR REPLACE VIEW kpi_1_5_delegation_churn AS
    WITH ordered AS (
        SELECT
            address,
            epoch,
            drep_hash,
            drep_type,
            LAG(drep_hash) OVER (PARTITION BY address ORDER BY slot)  AS prev_drep_hash,
            LAG(drep_type) OVER (PARTITION BY address ORDER BY slot)  AS prev_drep_type
        FROM delegation_events
    ),
    changes AS (
        SELECT epoch, address
        FROM ordered
        WHERE prev_drep_hash IS NOT NULL
          AND (drep_hash != prev_drep_hash OR drep_type != prev_drep_type)
    )
    SELECT
        epoch,
        COUNT(*)                AS delegation_changes,
        COUNT(DISTINCT address) AS addresses_changed
    FROM changes
    GROUP BY epoch
    ORDER BY epoch
    """,
)

# ---------------------------------------------------------------------------
# KPI 1.8  Inactive Delegated Ada
# ---------------------------------------------------------------------------
# Ada delegated to DReps that are either retired or past their active_until
# epoch.  Summed per epoch from drep_epoch_stats.
# ---------------------------------------------------------------------------
KPI_1_8 = (
    "kpi_1_8_inactive_delegated_ada",
    """
    CREATE OR REPLACE VIEW kpi_1_8_inactive_delegated_ada AS
    SELECT
        epoch,
        SUM(delegated_amount)   AS inactive_delegated_ada,
        COUNT(*)                AS inactive_drep_count
    FROM drep_epoch_stats
    WHERE status = 'RETIRED'
       OR epoch > active_until
    GROUP BY epoch
    ORDER BY epoch
    """,
)

# ---------------------------------------------------------------------------
# Collected list for registry import
# ---------------------------------------------------------------------------
CATEGORY_1_VIEWS = [
    KPI_1_1,
    KPI_1_2,
    KPI_1_3,
    KPI_1_5,
    KPI_1_8,
]
