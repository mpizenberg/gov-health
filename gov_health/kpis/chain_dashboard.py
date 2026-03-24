"""Chain Dashboard — general Cardano chain health views.

Each entry is a (view_name, create_sql) tuple. These views use source
Parquet tables directly (block, transaction, adapot, epoch_stake, etc.)
registered by db.py, NOT the extracted output datasets.
"""

# ---------------------------------------------------------------------------
# 1. Chain Activity — blocks, transactions, fees per epoch
# ---------------------------------------------------------------------------
CHAIN_ACTIVITY = (
    "chain_activity",
    """
    CREATE OR REPLACE VIEW chain_activity AS
    SELECT
        epoch,
        COUNT(*)::INT                           AS blocks,
        SUM(no_of_txs)::BIGINT                  AS transactions,
        SUM(total_fees)::BIGINT                  AS total_fees_lovelace,
        ROUND(SUM(total_fees) / 1e6, 2)         AS total_fees_ada,
        ROUND(AVG(no_of_txs), 2)                AS avg_txs_per_block,
        ROUND(AVG(total_output) / 1e6, 2)       AS avg_output_per_block_ada
    FROM block
    WHERE epoch >= 208
    GROUP BY epoch
    ORDER BY epoch
    """,
)

# ---------------------------------------------------------------------------
# 2. Chain Economics — treasury, reserves, circulation from adapot
# ---------------------------------------------------------------------------
CHAIN_ECONOMICS = (
    "chain_economics",
    """
    CREATE OR REPLACE VIEW chain_economics AS
    SELECT
        epoch,
        ROUND(treasury / 1e6, 0)::BIGINT        AS treasury_ada,
        ROUND(reserves / 1e6, 0)::BIGINT         AS reserves_ada,
        ROUND(circulation / 1e6, 0)::BIGINT      AS circulation_ada,
        ROUND(utxo / 1e6, 0)::BIGINT             AS utxo_ada,
        ROUND(fees / 1e6, 2)                      AS epoch_fees_ada,
        ROUND(distributed_rewards / 1e6, 0)::BIGINT AS distributed_rewards_ada,
        ROUND(
            (circulation - LAG(circulation) OVER (ORDER BY epoch)) / 1e6, 2
        )                                          AS circulation_delta_ada,
        ROUND(
            (treasury - LAG(treasury) OVER (ORDER BY epoch)) / 1e6, 2
        )                                          AS treasury_delta_ada
    FROM adapot
    ORDER BY epoch
    """,
)

# ---------------------------------------------------------------------------
# 3. Staking Summary — total staked, staker count, pool count per epoch
# ---------------------------------------------------------------------------
# epoch_stake is large so this aggregates per epoch. Only available from
# epoch 208+ (Shelley era).
# ---------------------------------------------------------------------------
STAKING_SUMMARY = (
    "staking_summary",
    """
    CREATE OR REPLACE VIEW staking_summary AS
    WITH stake_agg AS (
        SELECT
            epoch,
            COUNT(DISTINCT address)::INT         AS unique_stakers,
            COUNT(DISTINCT pool_id)::INT         AS active_pools,
            SUM(amount)::BIGINT                  AS total_staked_lovelace,
            ROUND(SUM(amount) / 1e6, 0)::BIGINT AS total_staked_ada
        FROM epoch_stake
        GROUP BY epoch
    ),
    circ AS (
        SELECT epoch, circulation FROM adapot
    )
    SELECT
        s.epoch,
        s.unique_stakers,
        s.active_pools,
        s.total_staked_lovelace,
        s.total_staked_ada,
        c.circulation AS circulation_lovelace,
        ROUND(s.total_staked_lovelace * 100.0 / NULLIF(c.circulation, 0), 2)
            AS staking_rate_pct,
        s.unique_stakers - LAG(s.unique_stakers) OVER (ORDER BY s.epoch)
            AS staker_delta,
        s.active_pools - LAG(s.active_pools) OVER (ORDER BY s.epoch)
            AS pool_delta
    FROM stake_agg s
    LEFT JOIN circ c ON c.epoch = s.epoch
    ORDER BY s.epoch
    """,
)

# ---------------------------------------------------------------------------
# 4. Stake Flow — registrations vs deregistrations per epoch
# ---------------------------------------------------------------------------
STAKE_FLOW = (
    "stake_flow",
    """
    CREATE OR REPLACE VIEW stake_flow AS
    SELECT
        epoch,
        COUNT(*) FILTER (WHERE type = 'STAKE_REGISTRATION')::INT
            AS registrations,
        COUNT(*) FILTER (WHERE type = 'STAKE_DEREGISTRATION')::INT
            AS deregistrations,
        (COUNT(*) FILTER (WHERE type = 'STAKE_REGISTRATION')
         - COUNT(*) FILTER (WHERE type = 'STAKE_DEREGISTRATION'))::INT
            AS net_flow
    FROM stake_registration
    GROUP BY epoch
    ORDER BY epoch
    """,
)

# ---------------------------------------------------------------------------
# 5. Pool Landscape — active pools, avg stake, saturation metrics
# ---------------------------------------------------------------------------
POOL_LANDSCAPE = (
    "pool_landscape",
    """
    CREATE OR REPLACE VIEW pool_landscape AS
    WITH pool_stakes AS (
        SELECT
            epoch,
            pool_id,
            SUM(amount)::BIGINT AS pool_stake
        FROM epoch_stake
        GROUP BY epoch, pool_id
    ),
    per_epoch AS (
        SELECT
            epoch,
            COUNT(*)::INT                              AS pool_count,
            ROUND(AVG(pool_stake) / 1e6, 0)::BIGINT   AS avg_pool_stake_ada,
            ROUND(MEDIAN(pool_stake) / 1e6, 0)::BIGINT AS median_pool_stake_ada,
            ROUND(MAX(pool_stake) / 1e6, 0)::BIGINT   AS max_pool_stake_ada,
            ROUND(MIN(pool_stake) / 1e6, 0)::BIGINT   AS min_pool_stake_ada,
            -- Top-10 pool concentration (% of total stake)
            ROUND(
                SUM(pool_stake) FILTER (
                    WHERE pool_stake >= (
                        SELECT APPROX_QUANTILE(ps2.pool_stake, 0.99)
                        FROM pool_stakes ps2 WHERE ps2.epoch = pool_stakes.epoch
                    )
                ) * 100.0 / NULLIF(SUM(pool_stake), 0),
                2
            ) AS top_1pct_stake_share
        FROM pool_stakes
        GROUP BY epoch
    )
    SELECT * FROM per_epoch
    ORDER BY epoch
    """,
)

# ---------------------------------------------------------------------------
# 6. Reward Summary — total rewards per epoch by type
# ---------------------------------------------------------------------------
REWARD_SUMMARY = (
    "reward_summary",
    """
    CREATE OR REPLACE VIEW reward_summary AS
    SELECT
        epoch,
        ROUND(SUM(amount) / 1e6, 0)::BIGINT AS total_rewards_ada,
        ROUND(SUM(amount) FILTER (WHERE type = 'member') / 1e6, 0)::BIGINT
            AS member_rewards_ada,
        ROUND(SUM(amount) FILTER (WHERE type = 'leader') / 1e6, 0)::BIGINT
            AS leader_rewards_ada,
        COUNT(DISTINCT address)::INT AS rewarded_addresses
    FROM reward
    GROUP BY epoch
    ORDER BY epoch
    """,
)

# ---------------------------------------------------------------------------
# 7. Transaction Volume — total output volume per epoch
# ---------------------------------------------------------------------------
TX_VOLUME = (
    "tx_volume",
    """
    CREATE OR REPLACE VIEW tx_volume AS
    SELECT
        epoch,
        COUNT(*)::BIGINT                         AS tx_count,
        ROUND(SUM(total_output) / 1e6, 0)::BIGINT AS total_output_ada,
        ROUND(AVG(total_output) / 1e6, 2)         AS avg_output_per_block_ada,
        ROUND(SUM(total_fees) / 1e6, 2)           AS total_fees_ada,
        ROUND(AVG(total_fees) / 1e6, 4)            AS avg_fee_per_block_ada
    FROM block
    WHERE epoch >= 208
    GROUP BY epoch
    ORDER BY epoch
    """,
)


# ---------------------------------------------------------------------------
# Collected list for registry import
# ---------------------------------------------------------------------------
CHAIN_DASHBOARD_VIEWS = [
    CHAIN_ACTIVITY,
    CHAIN_ECONOMICS,
    STAKING_SUMMARY,
    STAKE_FLOW,
    POOL_LANDSCAPE,
    REWARD_SUMMARY,
    TX_VOLUME,
]
