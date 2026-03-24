from gov_health.datasets.base import SingleFileDataset


class GovernanceParams(SingleFileDataset):
    name = "governance_params"

    def query_epochs(self, epochs: list[int]) -> str:
        epoch_list = ",".join(str(e) for e in epochs)
        return f"""
            SELECT
                epoch::INT AS epoch,
                CAST(json_extract_string(params, '$.drep_deposit') AS BIGINT) AS drep_deposit,
                CAST(json_extract_string(params, '$.gov_action_deposit') AS BIGINT) AS gov_action_deposit,
                CAST(json_extract_string(params, '$.drep_activity') AS INT) AS drep_activity,
                CAST(json_extract_string(params, '$.gov_action_lifetime') AS INT) AS gov_action_lifetime,
                CAST(json_extract_string(params, '$.committee_min_size') AS INT) AS committee_min_size,
                CAST(json_extract_string(params, '$.committee_max_term_length') AS INT) AS committee_max_term_length,
                -- Pool voting thresholds
                CAST(json_extract_string(params, '$.pool_voting_thresholds.pvt_motion_no_confidence.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.pool_voting_thresholds.pvt_motion_no_confidence.denominator') AS DOUBLE), 0)
                  AS pvt_motion_no_confidence,
                CAST(json_extract_string(params, '$.pool_voting_thresholds.pvt_committee_normal.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.pool_voting_thresholds.pvt_committee_normal.denominator') AS DOUBLE), 0)
                  AS pvt_committee_normal,
                CAST(json_extract_string(params, '$.pool_voting_thresholds.pvt_committee_no_confidence.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.pool_voting_thresholds.pvt_committee_no_confidence.denominator') AS DOUBLE), 0)
                  AS pvt_committee_no_confidence,
                CAST(json_extract_string(params, '$.pool_voting_thresholds.pvt_hard_fork_initiation.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.pool_voting_thresholds.pvt_hard_fork_initiation.denominator') AS DOUBLE), 0)
                  AS pvt_hard_fork_initiation,
                CAST(json_extract_string(params, '$.pool_voting_thresholds.pvt_ppsecurity_group.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.pool_voting_thresholds.pvt_ppsecurity_group.denominator') AS DOUBLE), 0)
                  AS pvt_pp_security_group,
                -- DRep voting thresholds
                CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_motion_no_confidence.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_motion_no_confidence.denominator') AS DOUBLE), 0)
                  AS dvt_motion_no_confidence,
                CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_committee_normal.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_committee_normal.denominator') AS DOUBLE), 0)
                  AS dvt_committee_normal,
                CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_committee_no_confidence.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_committee_no_confidence.denominator') AS DOUBLE), 0)
                  AS dvt_committee_no_confidence,
                CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_update_to_constitution.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_update_to_constitution.denominator') AS DOUBLE), 0)
                  AS dvt_update_to_constitution,
                CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_hard_fork_initiation.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_hard_fork_initiation.denominator') AS DOUBLE), 0)
                  AS dvt_hard_fork_initiation,
                CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_ppnetwork_group.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_ppnetwork_group.denominator') AS DOUBLE), 0)
                  AS dvt_pp_network_group,
                CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_ppeconomic_group.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_ppeconomic_group.denominator') AS DOUBLE), 0)
                  AS dvt_pp_economic_group,
                CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_pptechnical_group.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_pptechnical_group.denominator') AS DOUBLE), 0)
                  AS dvt_pp_technical_group,
                CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_ppgov_group.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_ppgov_group.denominator') AS DOUBLE), 0)
                  AS dvt_pp_gov_group,
                CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_treasury_withdrawal.numerator') AS DOUBLE)
                  / NULLIF(CAST(json_extract_string(params, '$.drep_voting_thresholds.dvt_treasury_withdrawal.denominator') AS DOUBLE), 0)
                  AS dvt_treasury_withdrawal
            FROM epoch_param
            WHERE epoch IN ({epoch_list})
            ORDER BY epoch
        """
