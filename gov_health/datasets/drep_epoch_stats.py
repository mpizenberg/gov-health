import pyarrow as pa

from gov_health.datasets.base import EpochPartitionedDataset


class DRepEpochStats(EpochPartitionedDataset):
    name = "drep_epoch_stats"

    def schema(self) -> pa.Schema:
        return pa.schema([
            ("epoch", pa.int32()),
            ("drep_hash", pa.string()),
            ("drep_type", pa.string()),
            ("drep_id", pa.string()),
            ("delegated_amount", pa.int64()),
            ("active_until", pa.int32()),
            ("expiry", pa.int32()),
            ("status", pa.string()),
            ("registration_epoch", pa.int32()),
            ("has_metadata", pa.bool_()),
            ("votes_cast", pa.int32()),
            ("votes_with_rationale", pa.int32()),
        ])

    def query_epoch(self, epoch: int) -> tuple[str, list]:
        sql = """
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
        """
        return sql, {"epoch": epoch}
