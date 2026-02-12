import pyarrow as pa

from gov_health.datasets.base import SingleFileDataset


class CCVoteDetails(SingleFileDataset):
    name = "cc_vote_details"

    def schema(self) -> pa.Schema:
        return pa.schema([
            ("gov_action_tx_hash", pa.string()),
            ("gov_action_index", pa.int32()),
            ("action_type", pa.string()),
            ("action_epoch", pa.int32()),
            ("voter_hash", pa.string()),
            ("cold_key", pa.string()),
            ("vote", pa.string()),
            ("vote_epoch", pa.int32()),
            ("vote_block_time", pa.int64()),
            ("action_block_time", pa.int64()),
            ("time_to_vote_seconds", pa.int64()),
            # epoch for incremental tracking
            ("epoch", pa.int32()),
        ])

    def query_epochs(self, epochs: list[int]) -> tuple[str, list]:
        sql = """
            SELECT
              vp.gov_action_tx_hash,
              vp.gov_action_index,
              gap.type AS action_type,
              gap.epoch AS action_epoch,
              vp.voter_hash,
              cr.cold_key,
              vp.vote,
              vp.epoch AS vote_epoch,
              vp.block_time AS vote_block_time,
              gap.block_time AS action_block_time,
              (vp.block_time - gap.block_time) AS time_to_vote_seconds,
              vp.epoch AS epoch
            FROM voting_procedure vp
            JOIN gov_action_proposal gap
              ON gap.tx_hash = vp.gov_action_tx_hash AND gap.idx = vp.gov_action_index
            LEFT JOIN LATERAL (
              SELECT cold_key FROM committee_registration
              WHERE hot_key = vp.voter_hash
              ORDER BY slot DESC LIMIT 1
            ) cr ON true
            WHERE vp.voter_type IN ('CONSTITUTIONAL_COMMITTEE_HOT_KEY_HASH', 'CONSTITUTIONAL_COMMITTEE_HOT_SCRIPT_HASH')
              AND vp.epoch = ANY(%(epochs)s)
            ORDER BY vp.epoch, vp.gov_action_tx_hash, vp.gov_action_index, vp.voter_hash
        """
        return sql, {"epochs": epochs}
