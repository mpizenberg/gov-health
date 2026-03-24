from gov_health.datasets.base import SingleFileDataset


class EpochSummary(SingleFileDataset):
    name = "epoch_summary"

    def query_epochs(self, epochs: list[int]) -> str:
        epoch_list = ",".join(str(e) for e in epochs)
        return f"""
            WITH epoch_base AS (
                SELECT epoch FROM adapot WHERE epoch IN ({epoch_list})
            ),
            block_stats AS (
                SELECT epoch,
                    COUNT(*) AS block_count,
                    MIN(epoch(block_time))::BIGINT AS epoch_start_time,
                    MAX(epoch(block_time))::BIGINT AS epoch_end_time
                FROM block
                WHERE epoch IN ({epoch_list})
                GROUP BY epoch
            ),
            tx_stats AS (
                SELECT epoch, COUNT(*) AS tx_count
                FROM transaction
                WHERE epoch IN ({epoch_list})
                GROUP BY epoch
            ),
            stake_agg AS (
                SELECT epoch, SUM(amount)::BIGINT AS total_active_stake
                FROM epoch_stake
                WHERE epoch IN ({epoch_list})
                GROUP BY epoch
            ),
            dd_agg AS (
                SELECT epoch,
                    SUM(amount)::BIGINT AS total_drep_delegated,
                    (SUM(amount) FILTER (WHERE drep_type NOT IN ('ABSTAIN')))::BIGINT AS total_drep_delegated_non_abstain,
                    (SUM(amount) FILTER (WHERE drep_type = 'NO_CONFIDENCE'))::BIGINT AS total_drep_delegated_no_confidence,
                    (COUNT(DISTINCT drep_hash) FILTER (WHERE drep_type NOT IN ('ABSTAIN', 'NO_CONFIDENCE')))::INT AS active_drep_count
                FROM drep_dist
                WHERE epoch IN ({epoch_list})
                GROUP BY epoch
            ),
            pool_agg AS (
                SELECT epoch, COUNT(*)::INT AS active_pool_count
                FROM pool
                WHERE status != 'RETIRED' AND epoch IN ({epoch_list})
                GROUP BY epoch
            ),
            cc_info AS (
                SELECT
                    eb.epoch,
                    COUNT(cmm.hash)::INT AS cc_member_count,
                    c.threshold AS cc_threshold
                FROM epoch_base eb
                LEFT JOIN committee_member cmm
                    ON cmm.start_epoch <= eb.epoch
                    AND (cmm.expired_epoch IS NULL OR cmm.expired_epoch > eb.epoch)
                LEFT JOIN (
                    SELECT threshold, epoch,
                           ROW_NUMBER() OVER (ORDER BY epoch DESC) AS rn
                    FROM committee
                ) c ON c.rn = 1
                GROUP BY eb.epoch, c.threshold
            ),
            ga_proposed AS (
                SELECT epoch, COUNT(*)::INT AS gov_actions_proposed
                FROM gov_action_proposal
                WHERE epoch IN ({epoch_list})
                GROUP BY epoch
            ),
            ga_status_agg AS (
                SELECT epoch,
                    (COUNT(*) FILTER (WHERE status = 'RATIFIED'))::INT AS gov_actions_ratified,
                    (COUNT(*) FILTER (WHERE status = 'EXPIRED'))::INT AS gov_actions_expired
                FROM gov_action_proposal_status
                WHERE epoch IN ({epoch_list})
                GROUP BY epoch
            ),
            drep_reg AS (
                SELECT epoch,
                    (COUNT(*) FILTER (WHERE type = 'REG_DREP_CERT'))::INT AS drep_registrations,
                    (COUNT(*) FILTER (WHERE type = 'UNREG_DREP_CERT'))::INT AS drep_retirements
                FROM drep_registration
                WHERE epoch IN ({epoch_list})
                GROUP BY epoch
            ),
            tw AS (
                SELECT gap.epoch,
                    COUNT(*)::BIGINT AS treasury_withdrawal_total
                FROM gov_action_proposal gap
                JOIN gov_action_proposal_status gps
                    ON gps.gov_action_tx_hash = gap.tx_hash
                    AND gps.gov_action_index = gap.idx
                    AND gps.status = 'ENACTED'
                WHERE gap.type = 'TREASURY_WITHDRAWALS_ACTION'
                  AND gps.epoch IN ({epoch_list})
                GROUP BY gap.epoch
            )
            SELECT
                eb.epoch::INT AS epoch,
                ap.treasury::BIGINT AS treasury,
                ap.reserves::BIGINT AS reserves,
                ap.circulation::BIGINT AS circulation,
                ap.utxo::BIGINT AS utxo,
                ap.fees::BIGINT AS fees,
                stk.total_active_stake,
                dd.total_drep_delegated,
                dd.total_drep_delegated_non_abstain,
                dd.total_drep_delegated_no_confidence,
                dd.active_drep_count,
                pa.active_pool_count,
                ci.cc_member_count,
                ci.cc_threshold,
                bs.block_count::INT AS block_count,
                ts.tx_count::BIGINT AS tx_count,
                bs.epoch_start_time,
                bs.epoch_end_time,
                COALESCE(gp.gov_actions_proposed, 0)::INT AS gov_actions_proposed,
                COALESCE(gs.gov_actions_ratified, 0)::INT AS gov_actions_ratified,
                COALESCE(gs.gov_actions_expired, 0)::INT AS gov_actions_expired,
                COALESCE(dr.drep_registrations, 0)::INT AS drep_registrations,
                COALESCE(dr.drep_retirements, 0)::INT AS drep_retirements,
                COALESCE(tw.treasury_withdrawal_total, 0)::BIGINT AS treasury_withdrawal_total
            FROM epoch_base eb
            LEFT JOIN adapot ap ON ap.epoch = eb.epoch
            LEFT JOIN stake_agg stk ON stk.epoch = eb.epoch
            LEFT JOIN dd_agg dd ON dd.epoch = eb.epoch
            LEFT JOIN pool_agg pa ON pa.epoch = eb.epoch
            LEFT JOIN cc_info ci ON ci.epoch = eb.epoch
            LEFT JOIN block_stats bs ON bs.epoch = eb.epoch
            LEFT JOIN tx_stats ts ON ts.epoch = eb.epoch
            LEFT JOIN ga_proposed gp ON gp.epoch = eb.epoch
            LEFT JOIN ga_status_agg gs ON gs.epoch = eb.epoch
            LEFT JOIN drep_reg dr ON dr.epoch = eb.epoch
            LEFT JOIN tw ON tw.epoch = eb.epoch
            ORDER BY eb.epoch
        """
