use std::time::Duration;

const REOPT_RTT_MARGIN: f64 = 0.25;
const REOPT_LOSS_MARGIN: f64 = 0.10;
const REOPT_LOSS_TOLERANCE: f64 = 0.02;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PathScore {
    pub rtt: Duration,
    pub loss: f64,
}

impl PathScore {
    pub(crate) fn cost(&self) -> f64 {
        self.rtt.as_secs_f64() / (1.0 - self.loss).max(0.05)
    }
    pub(crate) fn beats_by_margin(&self, active: &PathScore) -> Option<MigrationRule> {
        let rtt_wins = self.rtt.as_secs_f64()
            <= active.rtt.as_secs_f64() * (1.0 - REOPT_RTT_MARGIN)
            && self.loss <= active.loss + REOPT_LOSS_TOLERANCE;
        let loss_wins = self.loss + REOPT_LOSS_MARGIN <= active.loss
            && self.rtt.as_secs_f64() <= active.rtt.as_secs_f64() * (1.0 + REOPT_RTT_MARGIN);
        match (rtt_wins, loss_wins) {
            (true, _) => Some(MigrationRule::Rtt),
            (false, true) => Some(MigrationRule::Loss),
            (false, false) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationRule {
    Rtt,
    Loss,
}

impl MigrationRule {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Rtt => "rtt_margin",
            Self::Loss => "loss_margin",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MigrationVerdict {
    Migrate {
        rule: MigrationRule,
        active: PathScore,
        best: PathScore,
    },
    ActiveUnmeasured,
    NoLiveCandidate {
        active: PathScore,
    },
    WithinMargin {
        active: PathScore,
        best: PathScore,
    },
}

impl MigrationVerdict {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Migrate { .. } => "margin_win",
            Self::ActiveUnmeasured => "active_unmeasured",
            Self::NoLiveCandidate { .. } => "no_live_candidate",
            Self::WithinMargin { .. } => "within_margin",
        }
    }
    pub fn wants_migration(&self) -> bool {
        matches!(self, Self::Migrate { .. })
    }
    pub fn active(&self) -> Option<PathScore> {
        match self {
            Self::Migrate { active, .. }
            | Self::NoLiveCandidate { active }
            | Self::WithinMargin { active, .. } => Some(*active),
            Self::ActiveUnmeasured => None,
        }
    }
    pub fn best(&self) -> Option<PathScore> {
        match self {
            Self::Migrate { best, .. } | Self::WithinMargin { best, .. } => Some(*best),
            Self::ActiveUnmeasured | Self::NoLiveCandidate { .. } => None,
        }
    }
    pub fn rule(&self) -> Option<MigrationRule> {
        match self {
            Self::Migrate { rule, .. } => Some(*rule),
            _ => None,
        }
    }
}
