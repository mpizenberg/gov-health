from gov_health.datasets.base import EpochPartitionedDataset


class PoolEpochStats(EpochPartitionedDataset):
    name = "pool_epoch_stats"

    def query_epoch(self, epoch: int) -> str:
        return f"""
            WITH vote_counts AS (
                SELECT voter_hash,
                    COUNT(*) AS votes_cast,
                    COUNT(*) FILTER (WHERE vote = 'YES') AS votes_yes,
                    COUNT(*) FILTER (WHERE vote = 'NO') AS votes_no,
                    COUNT(*) FILTER (WHERE vote = 'ABSTAIN') AS votes_abstain
                FROM voting_procedure
                WHERE voter_type = 'STAKING_POOL_KEY_HASH'
                  AND epoch = {epoch}
                GROUP BY voter_hash
            ),
            latest_pool_reg AS (
                SELECT pool_id, pool_owners,
                       ROW_NUMBER() OVER (PARTITION BY pool_id ORDER BY slot DESC) AS rn
                FROM pool_registration
            ),
            pool_owner_creds AS (
                SELECT pr.pool_id, unnest(CAST(pr.pool_owners AS VARCHAR[])) AS owner_cred
                FROM latest_pool_reg pr
                WHERE pr.rn = 1
            ),
            latest_delegation AS (
                SELECT dv.credential, dv.drep_type,
                       ROW_NUMBER() OVER (PARTITION BY dv.credential ORDER BY dv.slot DESC) AS rn
                FROM delegation_vote dv
                WHERE dv.drep_type IN ('ABSTAIN', 'NO_CONFIDENCE')
            ),
            default_stances AS (
                SELECT DISTINCT poc.pool_id,
                       FIRST_VALUE(ld.drep_type) OVER (
                           PARTITION BY poc.pool_id ORDER BY ld.drep_type
                       ) AS default_stance
                FROM pool_owner_creds poc
                JOIN latest_delegation ld ON ld.credential = poc.owner_cred AND ld.rn = 1
            )
            SELECT
                p.epoch::INT AS epoch,
                p.pool_id,
                p.status,
                p.amount AS stake_amount,
                COALESCE(vc.votes_cast, 0)::INT AS votes_cast,
                COALESCE(vc.votes_yes, 0)::INT AS votes_yes,
                COALESCE(vc.votes_no, 0)::INT AS votes_no,
                COALESCE(vc.votes_abstain, 0)::INT AS votes_abstain,
                ds.default_stance
            FROM pool p
            LEFT JOIN vote_counts vc ON vc.voter_hash = p.pool_id
            LEFT JOIN default_stances ds ON ds.pool_id = p.pool_id
            WHERE p.epoch = {epoch}
            ORDER BY p.pool_id
        """
