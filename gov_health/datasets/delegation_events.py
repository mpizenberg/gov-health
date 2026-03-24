from gov_health.datasets.base import SingleFileDataset


class DelegationEvents(SingleFileDataset):
    name = "delegation_events"

    def query_epochs(self, epochs: list[int]) -> str:
        epoch_list = ",".join(str(e) for e in epochs)
        return f"""
            SELECT
                dv.address,
                dv.drep_hash,
                dv.drep_id,
                dv.drep_type,
                dv.credential,
                dv.epoch::INT AS epoch,
                dv.slot,
                epoch(dv.block_time)::BIGINT AS block_time
            FROM delegation_vote dv
            WHERE dv.epoch IN ({epoch_list})
            ORDER BY dv.epoch, dv.slot
        """
