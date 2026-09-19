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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cost_penalizes_loss_instead_of_ranking_by_rtt_alone() {
        let fast_but_lossy = PathScore {
            rtt: Duration::from_millis(100),
            loss: 0.8,
        };
        let slow_but_clean = PathScore {
            rtt: Duration::from_millis(200),
            loss: 0.0,
        };
        assert!(
            fast_but_lossy.cost() > slow_but_clean.cost(),
            "a heavily lossy fast path outranked a clean slower one: {:?} vs {:?}",
            fast_but_lossy,
            slow_but_clean,
        );
    }

    #[test]
    fn every_migration_rule_reports_its_documented_label() {
        let documented = [
            (MigrationRule::Rtt, "rtt_margin"),
            (MigrationRule::Loss, "loss_margin"),
        ];
        for (rule, label) in documented {
            assert_eq!(
                rule.as_str(),
                label,
                "{rule:?} reports {label:?} as its migration label",
            );
        }
    }

    #[test]
    fn every_migration_verdict_reports_its_documented_label_and_accessors() {
        let active = PathScore {
            rtt: Duration::from_millis(100),
            loss: 0.01,
        };
        let best = PathScore {
            rtt: Duration::from_millis(50),
            loss: 0.0,
        };
        let cases = [
            (
                MigrationVerdict::Migrate {
                    rule: MigrationRule::Rtt,
                    active,
                    best,
                },
                "margin_win",
                true,
                Some(active),
                Some(best),
                Some(MigrationRule::Rtt),
            ),
            (
                MigrationVerdict::Migrate {
                    rule: MigrationRule::Loss,
                    active,
                    best,
                },
                "margin_win",
                true,
                Some(active),
                Some(best),
                Some(MigrationRule::Loss),
            ),
            (
                MigrationVerdict::ActiveUnmeasured,
                "active_unmeasured",
                false,
                None,
                None,
                None,
            ),
            (
                MigrationVerdict::NoLiveCandidate { active },
                "no_live_candidate",
                false,
                Some(active),
                None,
                None,
            ),
            (
                MigrationVerdict::WithinMargin { active, best },
                "within_margin",
                false,
                Some(active),
                Some(best),
                None,
            ),
        ];
        for (verdict, label, wants_migration, active, best, rule) in cases {
            assert_eq!(
                verdict.as_str(),
                label,
                "{verdict:?} reports the wrong migration label",
            );
            assert_eq!(
                verdict.wants_migration(),
                wants_migration,
                "{verdict:?} disagrees on whether it wants a migration",
            );
            assert_eq!(
                verdict.active(),
                active,
                "{verdict:?} reports the wrong active path",
            );
            assert_eq!(
                verdict.best(),
                best,
                "{verdict:?} reports the wrong best path",
            );
            assert_eq!(
                verdict.rule(),
                rule,
                "{verdict:?} reports the wrong migration rule",
            );
        }
    }
}
