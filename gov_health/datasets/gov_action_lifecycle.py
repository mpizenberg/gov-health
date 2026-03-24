from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from gov_health.datasets.base import SingleFileDataset


class GovActionLifecycle(SingleFileDataset):
    name = "gov_action_lifecycle"

    def query_epochs(self, epochs: list[int]) -> str:
        epoch_list = ",".join(str(e) for e in epochs)
        return f"""
            WITH latest_status AS (
                SELECT
                    gov_action_tx_hash,
                    gov_action_index,
                    status,
                    voting_stats,
                    epoch,
                    ROW_NUMBER() OVER (
                        PARTITION BY gov_action_tx_hash, gov_action_index
                        ORDER BY epoch DESC
                    ) AS rn
                FROM gov_action_proposal_status
            ),
            resolved AS (
                SELECT
                    gov_action_tx_hash,
                    gov_action_index,
                    status,
                    voting_stats,
                    CASE WHEN status IN ('RATIFIED', 'EXPIRED', 'ENACTED', 'DROPPED')
                         THEN epoch ELSE NULL END AS epoch_resolved
                FROM latest_status
                WHERE rn = 1
            )
            SELECT
                gap.tx_hash AS gov_action_tx_hash,
                gap.idx::INT AS gov_action_index,
                gap.type,
                gap.deposit,
                gap.return_address,
                gap.anchor_url,
                gap.epoch::INT AS epoch_proposed,
                epoch(gap.block_time)::BIGINT AS block_time_proposed,
                r.status,
                r.epoch_resolved::INT AS epoch_resolved,
                -- CC voting stats (flat JSON keys)
                CAST(json_extract_string(r.voting_stats, '$.cc_yes') AS INT) AS cc_yes,
                CAST(json_extract_string(r.voting_stats, '$.cc_no') AS INT) AS cc_no,
                CAST(json_extract_string(r.voting_stats, '$.cc_abstain') AS INT) AS cc_abstain,
                CAST(json_extract_string(r.voting_stats, '$.cc_do_not_vote') AS INT) AS cc_do_not_vote,
                CAST(json_extract_string(r.voting_stats, '$.cc_approval_ratio') AS DOUBLE) AS cc_approval_ratio,
                -- DRep voting stats
                CAST(json_extract_string(r.voting_stats, '$.drep_yes_vote_stake') AS BIGINT) AS drep_yes_vote_stake,
                CAST(json_extract_string(r.voting_stats, '$.drep_no_vote_stake') AS BIGINT) AS drep_no_vote_stake,
                CAST(json_extract_string(r.voting_stats, '$.drep_abstain_vote_stake') AS BIGINT) AS drep_abstain_vote_stake,
                CAST(json_extract_string(r.voting_stats, '$.drep_no_confidence_stake') AS BIGINT) AS drep_no_confidence_stake,
                CAST(json_extract_string(r.voting_stats, '$.drep_auto_abstain_stake') AS BIGINT) AS drep_auto_abstain_stake,
                CAST(json_extract_string(r.voting_stats, '$.drep_do_not_vote_stake') AS BIGINT) AS drep_do_not_vote_stake,
                CAST(json_extract_string(r.voting_stats, '$.drep_total_yes_stake') AS BIGINT) AS drep_total_yes_stake,
                CAST(json_extract_string(r.voting_stats, '$.drep_total_no_stake') AS BIGINT) AS drep_total_no_stake,
                CAST(json_extract_string(r.voting_stats, '$.drep_total_abstain_stake') AS BIGINT) AS drep_total_abstain_stake,
                CAST(json_extract_string(r.voting_stats, '$.drep_approval_ratio') AS DOUBLE) AS drep_approval_ratio,
                -- SPO voting stats
                CAST(json_extract_string(r.voting_stats, '$.spo_yes_vote_stake') AS BIGINT) AS spo_yes_vote_stake,
                CAST(json_extract_string(r.voting_stats, '$.spo_no_vote_stake') AS BIGINT) AS spo_no_vote_stake,
                CAST(json_extract_string(r.voting_stats, '$.spo_abstain_vote_stake') AS BIGINT) AS spo_abstain_vote_stake,
                CAST(json_extract_string(r.voting_stats, '$.spo_do_not_vote_stake') AS BIGINT) AS spo_do_not_vote_stake,
                CAST(json_extract_string(r.voting_stats, '$.spo_total_yes_stake') AS BIGINT) AS spo_total_yes_stake,
                CAST(json_extract_string(r.voting_stats, '$.spo_total_no_stake') AS BIGINT) AS spo_total_no_stake,
                CAST(json_extract_string(r.voting_stats, '$.spo_total_abstain_stake') AS BIGINT) AS spo_total_abstain_stake,
                CAST(json_extract_string(r.voting_stats, '$.spo_approval_ratio') AS DOUBLE) AS spo_approval_ratio,
                COALESCE(r.epoch_resolved, gap.epoch)::INT AS epoch
            FROM gov_action_proposal gap
            LEFT JOIN resolved r
              ON r.gov_action_tx_hash = gap.tx_hash AND r.gov_action_index = gap.idx
            WHERE gap.epoch IN ({epoch_list})
               OR (r.status IS NULL OR r.status NOT IN ('RATIFIED', 'EXPIRED', 'ENACTED', 'DROPPED'))
        """

    def extract(self, conn, settled: list[int], max_epoch: int, output_dir):
        """Override: for lifecycle, always re-fetch actions with non-terminal status."""
        output_dir = Path(output_dir)
        existing = self.existing_epochs(output_dir)
        new_epochs = sorted(set(settled) - existing)
        unsettled = max_epoch if max_epoch not in settled else None

        epochs_to_fetch = new_epochs[:]
        if unsettled is not None:
            epochs_to_fetch.append(unsettled)

        # For lifecycle, we pass epochs but the query also re-fetches ACTIVE actions
        if not epochs_to_fetch:
            epochs_to_fetch = [max_epoch]

        sql = self.query_epochs(epochs_to_fetch)
        new_table = conn.execute(sql).fetch_arrow_table()
        if new_table.num_rows == 0:
            return

        path = self.file_path(output_dir)

        if path.exists():
            old_table = pq.read_table(path)
            # Deduplicate by (gov_action_tx_hash, gov_action_index)
            new_keys = set()
            tx_col = new_table.column("gov_action_tx_hash").to_pylist()
            idx_col = new_table.column("gov_action_index").to_pylist()
            for tx, idx in zip(tx_col, idx_col):
                new_keys.add((tx, idx))

            old_tx = old_table.column("gov_action_tx_hash").to_pylist()
            old_idx = old_table.column("gov_action_index").to_pylist()
            keep = [i for i, (tx, idx) in enumerate(zip(old_tx, old_idx))
                    if (tx, idx) not in new_keys]
            old_table = old_table.take(keep)
            combined = pa.concat_tables([old_table, new_table])
        else:
            combined = new_table

        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(combined, path)
