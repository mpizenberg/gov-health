import pyarrow as pa

from gov_health.datasets.base import SingleFileDataset


class DelegationEvents(SingleFileDataset):
    name = "delegation_events"

    def schema(self) -> pa.Schema:
        return pa.schema([
            ("address", pa.string()),
            ("drep_hash", pa.string()),
            ("drep_id", pa.string()),
            ("drep_type", pa.string()),
            ("credential", pa.string()),
            ("epoch", pa.int32()),
            ("slot", pa.int64()),
            ("block_time", pa.int64()),
        ])

    def query_epochs(self, epochs: list[int]) -> tuple[str, list]:
        sql = """
            SELECT
              dv.address,
              dv.drep_hash,
              dv.drep_id,
              dv.drep_type,
              dv.credential,
              dv.epoch,
              dv.slot,
              dv.block_time
            FROM delegation_vote dv
            WHERE dv.epoch = ANY(%(epochs)s)
            ORDER BY dv.epoch, dv.slot
        """
        return sql, {"epochs": epochs}
