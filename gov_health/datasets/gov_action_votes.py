from gov_health.datasets.base import EpochPartitionedDataset


class GovActionVotes(EpochPartitionedDataset):
    name = "gov_action_votes"

    def query_epoch(self, epoch: int) -> str:
        return f"""
            SELECT
                vp.gov_action_tx_hash,
                vp.gov_action_index::INT AS gov_action_index,
                gap.type AS action_type,
                gap.epoch::INT AS action_epoch,
                epoch(gap.block_time)::BIGINT AS action_block_time,
                vp.voter_type,
                vp.voter_hash,
                vp.vote,
                (vp.anchor_url IS NOT NULL) AS has_rationale,
                vp.epoch::INT AS vote_epoch,
                epoch(vp.block_time)::BIGINT AS vote_block_time,
                vp.slot AS vote_slot
            FROM voting_procedure vp
            JOIN gov_action_proposal gap
              ON gap.tx_hash = vp.gov_action_tx_hash AND gap.idx = vp.gov_action_index
            WHERE vp.epoch = {epoch}
            ORDER BY vp.gov_action_tx_hash, vp.gov_action_index, vp.voter_hash, vp.slot
        """
