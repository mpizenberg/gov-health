from gov_health.datasets.drep_epoch_stats import DRepEpochStats
from gov_health.datasets.pool_epoch_stats import PoolEpochStats
from gov_health.datasets.gov_action_votes import GovActionVotes
from gov_health.datasets.gov_action_lifecycle import GovActionLifecycle
from gov_health.datasets.epoch_summary import EpochSummary
from gov_health.datasets.delegation_events import DelegationEvents
from gov_health.datasets.cc_vote_details import CCVoteDetails
from gov_health.datasets.governance_params import GovernanceParams

ALL_DATASETS = [
    DRepEpochStats(),
    PoolEpochStats(),
    GovActionVotes(),
    GovActionLifecycle(),
    EpochSummary(),
    DelegationEvents(),
    CCVoteDetails(),
    GovernanceParams(),
]
