import pyarrow as pa

from gov_health.datasets.base import SingleFileDataset


class EpochSummary(SingleFileDataset):
    name = "epoch_summary"

    def schema(self) -> pa.Schema:
        return pa.schema([
            ("epoch", pa.int32()),
            ("treasury", pa.int64()),
            ("reserves", pa.int64()),
            ("circulation", pa.int64()),
            ("utxo", pa.int64()),
            ("fees", pa.int64()),
            ("total_active_stake", pa.int64()),
            ("total_drep_delegated", pa.int64()),
            ("total_drep_delegated_non_abstain", pa.int64()),
            ("total_drep_delegated_no_confidence", pa.int64()),
            ("active_drep_count", pa.int32()),
            ("active_pool_count", pa.int32()),
            ("cc_member_count", pa.int32()),
            ("cc_threshold", pa.float64()),
            ("block_count", pa.int32()),
            ("tx_count", pa.int64()),
            ("epoch_start_time", pa.int64()),
            ("epoch_end_time", pa.int64()),
            ("gov_actions_proposed", pa.int32()),
            ("gov_actions_ratified", pa.int32()),
            ("gov_actions_expired", pa.int32()),
            ("drep_registrations", pa.int32()),
            ("drep_retirements", pa.int32()),
            ("treasury_withdrawal_total", pa.int64()),
        ])

    def query_epochs(self, epochs: list[int]) -> tuple[str, list]:
        sql = """
            SELECT
              e.number AS epoch,
              ap.treasury, ap.reserves, ap.circulation, ap.utxo, ap.fees,
              stk.total_active_stake,
              dd_agg.total_drep_delegated,
              dd_agg.total_drep_delegated_non_abstain,
              dd_agg.total_drep_delegated_no_confidence,
              dd_agg.active_drep_count,
              pool_agg.active_pool_count,
              cm_agg.cc_member_count,
              cm_agg.cc_threshold,
              e.block_count,
              e.transaction_count AS tx_count,
              e.start_time AS epoch_start_time,
              e.end_time AS epoch_end_time,
              COALESCE(ga_agg.gov_actions_proposed, 0) AS gov_actions_proposed,
              COALESCE(ga_agg.gov_actions_ratified, 0) AS gov_actions_ratified,
              COALESCE(ga_agg.gov_actions_expired, 0) AS gov_actions_expired,
              COALESCE(drep_reg.drep_registrations, 0) AS drep_registrations,
              COALESCE(drep_reg.drep_retirements, 0) AS drep_retirements,
              COALESCE(tw.treasury_withdrawal_total, 0) AS treasury_withdrawal_total
            FROM epoch e
            LEFT JOIN adapot ap ON ap.epoch = e.number
            LEFT JOIN (
              SELECT epoch, SUM(amount) AS total_active_stake
              FROM epoch_stake GROUP BY epoch
            ) stk ON stk.epoch = e.number
            LEFT JOIN (
              SELECT epoch,
                SUM(amount) AS total_drep_delegated,
                SUM(amount) FILTER (WHERE drep_type NOT IN ('ABSTAIN')) AS total_drep_delegated_non_abstain,
                SUM(amount) FILTER (WHERE drep_type = 'NO_CONFIDENCE') AS total_drep_delegated_no_confidence,
                COUNT(DISTINCT drep_hash) FILTER (WHERE drep_type NOT IN ('ABSTAIN','NO_CONFIDENCE')) AS active_drep_count
              FROM drep_dist GROUP BY epoch
            ) dd_agg ON dd_agg.epoch = e.number
            LEFT JOIN (
              SELECT epoch, COUNT(*) AS active_pool_count
              FROM pool WHERE status != 'RETIRED'
              GROUP BY epoch
            ) pool_agg ON pool_agg.epoch = e.number
            LEFT JOIN LATERAL (
              SELECT
                COUNT(*) AS cc_member_count,
                c.threshold AS cc_threshold
              FROM committee_member cmm
              CROSS JOIN LATERAL (
                SELECT (threshold->>'numerator')::float
                  / NULLIF((threshold->>'denominator')::float, 0) AS threshold
                FROM committee ORDER BY epoch DESC LIMIT 1
              ) c
              WHERE cmm.start_epoch <= e.number
                AND (cmm.expired_epoch IS NULL OR cmm.expired_epoch > e.number)
              GROUP BY c.threshold
            ) cm_agg ON true
            LEFT JOIN (
              SELECT epoch,
                COUNT(*) AS gov_actions_proposed,
                COUNT(*) FILTER (WHERE tx_hash IN (
                  SELECT gov_action_tx_hash FROM gov_action_proposal_status
                  WHERE status = 'RATIFIED' AND epoch = gap_inner.epoch
                )) AS gov_actions_ratified,
                COUNT(*) FILTER (WHERE tx_hash IN (
                  SELECT gov_action_tx_hash FROM gov_action_proposal_status
                  WHERE status = 'EXPIRED' AND epoch = gap_inner.epoch
                )) AS gov_actions_expired
              FROM gov_action_proposal gap_inner
              GROUP BY epoch
            ) ga_agg ON ga_agg.epoch = e.number
            LEFT JOIN (
              SELECT epoch,
                COUNT(*) FILTER (WHERE type = 'REG') AS drep_registrations,
                COUNT(*) FILTER (WHERE type = 'UNREG') AS drep_retirements
              FROM drep_registration GROUP BY epoch
            ) drep_reg ON drep_reg.epoch = e.number
            LEFT JOIN (
              SELECT epoch, SUM(amount) AS treasury_withdrawal_total
              FROM local_treasury_withdrawal GROUP BY epoch
            ) tw ON tw.epoch = e.number
            WHERE e.number = ANY(%(epochs)s)
            ORDER BY e.number
        """
        return sql, {"epochs": epochs}
