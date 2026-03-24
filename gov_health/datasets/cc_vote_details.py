from gov_health.datasets.base import SingleFileDataset


class CCVoteDetails(SingleFileDataset):
    name = "cc_vote_details"

    def query_epochs(self, epochs: list[int]) -> str:
        epoch_list = ",".join(str(e) for e in epochs)
        return f"""
            WITH latest_cc_reg AS (
                SELECT hot_key, cold_key,
                       ROW_NUMBER() OVER (PARTITION BY hot_key ORDER BY slot DESC) AS rn
                FROM committee_registration
            )
            SELECT
                vp.gov_action_tx_hash,
                vp.gov_action_index::INT AS gov_action_index,
                gap.type AS action_type,
                gap.epoch::INT AS action_epoch,
                vp.voter_hash,
                cr.cold_key,
                vp.vote,
                vp.epoch::INT AS vote_epoch,
                epoch(vp.block_time)::BIGINT AS vote_block_time,
                epoch(gap.block_time)::BIGINT AS action_block_time,
                (epoch(vp.block_time) - epoch(gap.block_time))::BIGINT AS time_to_vote_seconds,
                vp.epoch::INT AS epoch
            FROM voting_procedure vp
            JOIN gov_action_proposal gap
              ON gap.tx_hash = vp.gov_action_tx_hash AND gap.idx = vp.gov_action_index
            LEFT JOIN latest_cc_reg cr ON cr.hot_key = vp.voter_hash AND cr.rn = 1
            WHERE vp.voter_type IN ('CONSTITUTIONAL_COMMITTEE_HOT_KEY_HASH', 'CONSTITUTIONAL_COMMITTEE_HOT_SCRIPT_HASH')
              AND vp.epoch IN ({epoch_list})
            ORDER BY vp.epoch, vp.gov_action_tx_hash, vp.gov_action_index, vp.voter_hash
        """
