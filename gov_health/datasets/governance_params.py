import pyarrow as pa

from gov_health.datasets.base import SingleFileDataset


class GovernanceParams(SingleFileDataset):
    name = "governance_params"

    def schema(self) -> pa.Schema:
        return pa.schema([
            ("epoch", pa.int32()),
            ("drep_deposit", pa.int64()),
            ("gov_action_deposit", pa.int64()),
            ("drep_activity", pa.int32()),
            ("gov_action_lifetime", pa.int32()),
            ("committee_min_size", pa.int32()),
            ("committee_max_term_length", pa.int32()),
            ("pvt_motion_no_confidence", pa.float64()),
            ("pvt_committee_normal", pa.float64()),
            ("pvt_committee_no_confidence", pa.float64()),
            ("pvt_hard_fork_initiation", pa.float64()),
            ("pvt_pp_security_group", pa.float64()),
            ("dvt_motion_no_confidence", pa.float64()),
            ("dvt_committee_normal", pa.float64()),
            ("dvt_committee_no_confidence", pa.float64()),
            ("dvt_update_to_constitution", pa.float64()),
            ("dvt_hard_fork_initiation", pa.float64()),
            ("dvt_pp_network_group", pa.float64()),
            ("dvt_pp_economic_group", pa.float64()),
            ("dvt_pp_technical_group", pa.float64()),
            ("dvt_pp_gov_group", pa.float64()),
            ("dvt_treasury_withdrawal", pa.float64()),
        ])

    def query_epochs(self, epochs: list[int]) -> tuple[str, list]:
        sql = """
            SELECT
              epoch,
              (params->>'drep_deposit')::bigint AS drep_deposit,
              (params->>'gov_action_deposit')::bigint AS gov_action_deposit,
              (params->>'drep_activity')::int AS drep_activity,
              (params->>'gov_action_lifetime')::int AS gov_action_lifetime,
              (params->>'committee_min_size')::int AS committee_min_size,
              (params->>'committee_max_term_length')::int AS committee_max_term_length,
              -- Pool voting thresholds
              (params->'pool_voting_thresholds'->'pvt_motion_no_confidence'->>'numerator')::float
                / NULLIF((params->'pool_voting_thresholds'->'pvt_motion_no_confidence'->>'denominator')::float, 0)
                AS pvt_motion_no_confidence,
              (params->'pool_voting_thresholds'->'pvt_committee_normal'->>'numerator')::float
                / NULLIF((params->'pool_voting_thresholds'->'pvt_committee_normal'->>'denominator')::float, 0)
                AS pvt_committee_normal,
              (params->'pool_voting_thresholds'->'pvt_committee_no_confidence'->>'numerator')::float
                / NULLIF((params->'pool_voting_thresholds'->'pvt_committee_no_confidence'->>'denominator')::float, 0)
                AS pvt_committee_no_confidence,
              (params->'pool_voting_thresholds'->'pvt_hard_fork_initiation'->>'numerator')::float
                / NULLIF((params->'pool_voting_thresholds'->'pvt_hard_fork_initiation'->>'denominator')::float, 0)
                AS pvt_hard_fork_initiation,
              (params->'pool_voting_thresholds'->'pvt_pp_security_group'->>'numerator')::float
                / NULLIF((params->'pool_voting_thresholds'->'pvt_pp_security_group'->>'denominator')::float, 0)
                AS pvt_pp_security_group,
              -- DRep voting thresholds
              (params->'d_rep_voting_thresholds'->'dvt_motion_no_confidence'->>'numerator')::float
                / NULLIF((params->'d_rep_voting_thresholds'->'dvt_motion_no_confidence'->>'denominator')::float, 0)
                AS dvt_motion_no_confidence,
              (params->'d_rep_voting_thresholds'->'dvt_committee_normal'->>'numerator')::float
                / NULLIF((params->'d_rep_voting_thresholds'->'dvt_committee_normal'->>'denominator')::float, 0)
                AS dvt_committee_normal,
              (params->'d_rep_voting_thresholds'->'dvt_committee_no_confidence'->>'numerator')::float
                / NULLIF((params->'d_rep_voting_thresholds'->'dvt_committee_no_confidence'->>'denominator')::float, 0)
                AS dvt_committee_no_confidence,
              (params->'d_rep_voting_thresholds'->'dvt_update_to_constitution'->>'numerator')::float
                / NULLIF((params->'d_rep_voting_thresholds'->'dvt_update_to_constitution'->>'denominator')::float, 0)
                AS dvt_update_to_constitution,
              (params->'d_rep_voting_thresholds'->'dvt_hard_fork_initiation'->>'numerator')::float
                / NULLIF((params->'d_rep_voting_thresholds'->'dvt_hard_fork_initiation'->>'denominator')::float, 0)
                AS dvt_hard_fork_initiation,
              (params->'d_rep_voting_thresholds'->'dvt_p_p_network_group'->>'numerator')::float
                / NULLIF((params->'d_rep_voting_thresholds'->'dvt_p_p_network_group'->>'denominator')::float, 0)
                AS dvt_pp_network_group,
              (params->'d_rep_voting_thresholds'->'dvt_p_p_economic_group'->>'numerator')::float
                / NULLIF((params->'d_rep_voting_thresholds'->'dvt_p_p_economic_group'->>'denominator')::float, 0)
                AS dvt_pp_economic_group,
              (params->'d_rep_voting_thresholds'->'dvt_p_p_technical_group'->>'numerator')::float
                / NULLIF((params->'d_rep_voting_thresholds'->'dvt_p_p_technical_group'->>'denominator')::float, 0)
                AS dvt_pp_technical_group,
              (params->'d_rep_voting_thresholds'->'dvt_p_p_gov_group'->>'numerator')::float
                / NULLIF((params->'d_rep_voting_thresholds'->'dvt_p_p_gov_group'->>'denominator')::float, 0)
                AS dvt_pp_gov_group,
              (params->'d_rep_voting_thresholds'->'dvt_treasury_withdrawal'->>'numerator')::float
                / NULLIF((params->'d_rep_voting_thresholds'->'dvt_treasury_withdrawal'->>'denominator')::float, 0)
                AS dvt_treasury_withdrawal
            FROM epoch_param
            WHERE epoch = ANY(%(epochs)s)
            ORDER BY epoch
        """
        return sql, {"epochs": epochs}
