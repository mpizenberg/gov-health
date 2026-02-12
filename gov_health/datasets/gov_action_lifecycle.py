import pyarrow as pa

from gov_health.datasets.base import SingleFileDataset


class GovActionLifecycle(SingleFileDataset):
    name = "gov_action_lifecycle"

    def schema(self) -> pa.Schema:
        return pa.schema([
            ("gov_action_tx_hash", pa.string()),
            ("gov_action_index", pa.int32()),
            ("type", pa.string()),
            ("deposit", pa.int64()),
            ("return_address", pa.string()),
            ("anchor_url", pa.string()),
            ("epoch_proposed", pa.int32()),
            ("block_time_proposed", pa.int64()),
            ("status", pa.string()),
            ("epoch_resolved", pa.int32()),
            ("cc_yes", pa.int32()),
            ("cc_no", pa.int32()),
            ("cc_abstain", pa.int32()),
            ("cc_do_not_vote", pa.int32()),
            ("cc_approval_ratio", pa.float64()),
            ("drep_yes_vote_stake", pa.int64()),
            ("drep_no_vote_stake", pa.int64()),
            ("drep_abstain_vote_stake", pa.int64()),
            ("drep_no_confidence_stake", pa.int64()),
            ("drep_auto_abstain_stake", pa.int64()),
            ("drep_do_not_vote_stake", pa.int64()),
            ("drep_total_yes_stake", pa.int64()),
            ("drep_total_no_stake", pa.int64()),
            ("drep_total_abstain_stake", pa.int64()),
            ("drep_approval_ratio", pa.float64()),
            ("spo_yes_vote_stake", pa.int64()),
            ("spo_no_vote_stake", pa.int64()),
            ("spo_abstain_vote_stake", pa.int64()),
            ("spo_do_not_vote_stake", pa.int64()),
            ("spo_total_yes_stake", pa.int64()),
            ("spo_total_no_stake", pa.int64()),
            ("spo_total_abstain_stake", pa.int64()),
            ("spo_approval_ratio", pa.float64()),
            # epoch column used for incremental tracking
            ("epoch", pa.int32()),
        ])

    def query_epochs(self, epochs: list[int]) -> tuple[str, list]:
        sql = """
            SELECT
              gap.tx_hash AS gov_action_tx_hash,
              gap.idx AS gov_action_index,
              gap.type,
              gap.deposit,
              gap.return_address,
              gap.anchor_url,
              gap.epoch AS epoch_proposed,
              gap.block_time AS block_time_proposed,
              s.status,
              s.epoch_resolved,
              -- CC voting stats
              (gap.voting_stats->'cc'->>'yes')::int AS cc_yes,
              (gap.voting_stats->'cc'->>'no')::int AS cc_no,
              (gap.voting_stats->'cc'->>'abstain')::int AS cc_abstain,
              (gap.voting_stats->'cc'->>'do_not_vote')::int AS cc_do_not_vote,
              (gap.voting_stats->'cc'->>'approval_ratio')::float8 AS cc_approval_ratio,
              -- DRep voting stats
              (gap.voting_stats->'drep'->>'yes_vote_stake')::bigint AS drep_yes_vote_stake,
              (gap.voting_stats->'drep'->>'no_vote_stake')::bigint AS drep_no_vote_stake,
              (gap.voting_stats->'drep'->>'abstain_vote_stake')::bigint AS drep_abstain_vote_stake,
              (gap.voting_stats->'drep'->>'no_confidence_stake')::bigint AS drep_no_confidence_stake,
              (gap.voting_stats->'drep'->>'auto_abstain_stake')::bigint AS drep_auto_abstain_stake,
              (gap.voting_stats->'drep'->>'do_not_vote_stake')::bigint AS drep_do_not_vote_stake,
              (gap.voting_stats->'drep'->>'total_yes_stake')::bigint AS drep_total_yes_stake,
              (gap.voting_stats->'drep'->>'total_no_stake')::bigint AS drep_total_no_stake,
              (gap.voting_stats->'drep'->>'total_abstain_stake')::bigint AS drep_total_abstain_stake,
              (gap.voting_stats->'drep'->>'approval_ratio')::float8 AS drep_approval_ratio,
              -- SPO voting stats
              (gap.voting_stats->'spo'->>'yes_vote_stake')::bigint AS spo_yes_vote_stake,
              (gap.voting_stats->'spo'->>'no_vote_stake')::bigint AS spo_no_vote_stake,
              (gap.voting_stats->'spo'->>'abstain_vote_stake')::bigint AS spo_abstain_vote_stake,
              (gap.voting_stats->'spo'->>'do_not_vote_stake')::bigint AS spo_do_not_vote_stake,
              (gap.voting_stats->'spo'->>'total_yes_stake')::bigint AS spo_total_yes_stake,
              (gap.voting_stats->'spo'->>'total_no_stake')::bigint AS spo_total_no_stake,
              (gap.voting_stats->'spo'->>'total_abstain_stake')::bigint AS spo_total_abstain_stake,
              (gap.voting_stats->'spo'->>'approval_ratio')::float8 AS spo_approval_ratio,
              -- epoch for incremental tracking (use latest status epoch, or proposed epoch)
              COALESCE(s.epoch_resolved, gap.epoch) AS epoch
            FROM gov_action_proposal gap
            LEFT JOIN LATERAL (
              SELECT
                gps.status,
                CASE WHEN gps.status IN ('RATIFIED', 'EXPIRED', 'ENACTED', 'DROPPED')
                     THEN gps.epoch ELSE NULL END AS epoch_resolved
              FROM gov_action_proposal_status gps
              WHERE gps.gov_action_tx_hash = gap.tx_hash
                AND gps.gov_action_index = gap.idx
              ORDER BY gps.epoch DESC, gps.id DESC LIMIT 1
            ) s ON true
            WHERE gap.epoch = ANY(%(epochs)s)
               OR (s.status IS NULL OR s.status NOT IN ('RATIFIED', 'EXPIRED', 'ENACTED', 'DROPPED'))
        """
        return sql, {"epochs": epochs}

    def extract(self, conn, settled: list[int], max_epoch: int, output_dir):
        """Override: for lifecycle, always re-fetch actions with non-terminal status."""
        from pathlib import Path
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        output_dir = Path(output_dir)
        existing = self.existing_epochs(output_dir)
        new_epochs = sorted(set(settled) - existing)
        unsettled = max_epoch if max_epoch not in settled else None

        epochs_to_fetch = new_epochs[:]
        if unsettled is not None:
            epochs_to_fetch.append(unsettled)

        # For lifecycle, we pass epochs but the query also re-fetches ACTIVE actions
        if not epochs_to_fetch:
            epochs_to_fetch = [max_epoch]  # still re-fetch active actions

        sql, params = self.query_epochs(epochs_to_fetch)
        rows = conn.execute(sql, params).fetchall()
        if not rows:
            return

        new_table = pa.Table.from_pylist(rows, schema=self.schema())
        path = self.file_path(output_dir)

        if path.exists():
            old_table = pq.read_table(path, schema=self.schema())
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
