import pyarrow as pa

from gov_health.datasets.base import EpochPartitionedDataset


class PoolEpochStats(EpochPartitionedDataset):
    name = "pool_epoch_stats"

    def schema(self) -> pa.Schema:
        return pa.schema([
            ("epoch", pa.int32()),
            ("pool_id", pa.string()),
            ("status", pa.string()),
            ("stake_amount", pa.int64()),
            ("votes_cast", pa.int32()),
            ("votes_yes", pa.int32()),
            ("votes_no", pa.int32()),
            ("votes_abstain", pa.int32()),
            ("default_stance", pa.string()),
        ])

    def query_epoch(self, epoch: int) -> tuple[str, list]:
        sql = """
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
        """
        return sql, {"epoch": epoch}
