import pyarrow as pa

from gov_health.datasets.base import EpochPartitionedDataset


class GovActionVotes(EpochPartitionedDataset):
    name = "gov_action_votes"

    def schema(self) -> pa.Schema:
        return pa.schema([
            ("gov_action_tx_hash", pa.string()),
            ("gov_action_index", pa.int32()),
            ("action_type", pa.string()),
            ("action_epoch", pa.int32()),
            ("action_block_time", pa.int64()),
            ("voter_type", pa.string()),
            ("voter_hash", pa.string()),
            ("vote", pa.string()),
            ("has_rationale", pa.bool_()),
            ("vote_epoch", pa.int32()),
            ("vote_block_time", pa.int64()),
            ("vote_slot", pa.int64()),
        ])

    def query_epoch(self, epoch: int) -> tuple[str, list]:
        sql = """
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
        """
        return sql, {"epoch": epoch}
