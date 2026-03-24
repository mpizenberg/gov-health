from gov_health.datasets.base import EpochPartitionedDataset


class DRepEpochStats(EpochPartitionedDataset):
    name = "drep_epoch_stats"

    def query_epoch(self, epoch: int) -> str:
        return f"""
            WITH latest_status AS (
                SELECT drep_hash, status,
                       ROW_NUMBER() OVER (PARTITION BY drep_hash ORDER BY slot DESC) AS rn
                FROM drep
                WHERE epoch <= {epoch}
            ),
            reg_info AS (
                SELECT
                    drep_hash,
                    MIN(epoch) AS registration_epoch,
                    BOOL_OR(anchor_url IS NOT NULL) FILTER (
                        WHERE (drep_hash, slot) IN (
                            SELECT drep_hash, MAX(slot) FROM drep_registration GROUP BY drep_hash
                        )
                    ) AS has_metadata
                FROM drep_registration
                GROUP BY drep_hash
            ),
            vote_counts AS (
                SELECT voter_hash,
                    COUNT(*) AS votes_cast,
                    COUNT(anchor_url) AS votes_with_rationale
                FROM voting_procedure
                WHERE voter_type IN ('DREP_KEY_HASH', 'DREP_SCRIPT_HASH')
                  AND epoch = {epoch}
                GROUP BY voter_hash
            )
            SELECT
                dd.epoch::INT AS epoch,
                dd.drep_hash,
                dd.drep_type,
                dd.drep_id,
                dd.amount AS delegated_amount,
                dd.active_until::INT AS active_until,
                dd.expiry::INT AS expiry,
                ls.status,
                ri.registration_epoch::INT AS registration_epoch,
                ri.has_metadata,
                COALESCE(vc.votes_cast, 0)::INT AS votes_cast,
                COALESCE(vc.votes_with_rationale, 0)::INT AS votes_with_rationale
            FROM drep_dist dd
            LEFT JOIN latest_status ls ON ls.drep_hash = dd.drep_hash AND ls.rn = 1
            LEFT JOIN reg_info ri ON ri.drep_hash = dd.drep_hash
            LEFT JOIN vote_counts vc ON vc.voter_hash = dd.drep_hash
            WHERE dd.epoch = {epoch}
            ORDER BY dd.drep_hash
        """
