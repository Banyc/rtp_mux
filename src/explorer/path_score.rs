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

    fn score(secs: f64, loss: f64) -> PathScore {
        PathScore {
            rtt: Duration::from_secs_f64(secs),
            loss,
        }
    }

    /// Every migration margin is inclusive at its exact boundary, and no
    /// margin constant can be widened or narrowed unnoticed: each probe sits
    /// either exactly on the boundary (the win must still hold) or just
    /// outside it (the win must still be refused), with the arithmetic exact
    /// in `f64` so the comparison is decided by the operator, not by rounding.
    #[test]
    fn migration_margins_are_inclusive_at_their_exact_boundary() {
        // rtt margin: 0.5s * (1 - 0.25) == 0.375s exactly.
        assert_eq!(
            score(0.375, 0.0).beats_by_margin(&score(0.5, 0.0)),
            Some(MigrationRule::Rtt),
            "an rtt exactly at the 25% margin must still win",
        );
        assert_eq!(
            score(0.376, 0.0).beats_by_margin(&score(0.5, 0.0)),
            None,
            "an rtt above the 25% margin must not win",
        );
        // loss tolerance on an rtt win: best.loss <= active.loss + 0.02.
        assert_eq!(
            score(0.375, 0.02).beats_by_margin(&score(0.5, 0.0)),
            Some(MigrationRule::Rtt),
            "a loss exactly on the 2% tolerance must not veto an rtt win",
        );
        assert_eq!(
            score(0.375, 0.021).beats_by_margin(&score(0.5, 0.0)),
            None,
            "a loss just over the 2% tolerance must veto the rtt win",
        );
        // loss margin: best.loss + 0.10 <= active.loss (rtt equal).
        assert_eq!(
            score(0.5, 0.0).beats_by_margin(&score(0.5, 0.1)),
            Some(MigrationRule::Loss),
            "a 10% loss advantage exactly on the margin must still win",
        );
        assert_eq!(
            score(0.5, 0.01).beats_by_margin(&score(0.5, 0.1)),
            None,
            "a 9% loss advantage must not win",
        );
        // rtt veto on a loss win: best.rtt <= active.rtt * 1.25.
        assert_eq!(
            score(0.625, 0.0).beats_by_margin(&score(0.5, 0.1)),
            Some(MigrationRule::Loss),
            "an rtt exactly at 1.25x the active path must still allow a loss win",
        );
        assert_eq!(
            score(0.63, 0.0).beats_by_margin(&score(0.5, 0.1)),
            None,
            "an rtt above 1.25x the active path must refuse the loss win",
        );
    }

    /// A path that clears both margins at once reports the rtt rule: the rtt
    /// comparison is consulted first, so the logged migration reason is the
    /// one the path actually won on rather than an arbitrary tie-break.
    #[test]
    fn a_path_that_clears_both_margins_reports_the_rtt_rule() {
        let best = score(0.375, 0.0);
        let active = score(0.5, 0.1);
        assert!(
            best.rtt.as_secs_f64() <= active.rtt.as_secs_f64() * (1.0 - 0.25),
            "the probe must clear the rtt margin for this test to be about precedence",
        );
        assert!(
            best.loss + 0.10 <= active.loss,
            "the probe must clear the loss margin for this test to be about precedence",
        );
        assert_eq!(
            best.beats_by_margin(&active),
            Some(MigrationRule::Rtt),
            "a path winning on rtt and loss at once must report the rtt rule",
        );
    }

    /// `cost` divides the rtt by the unreliability but never by less than
    /// 0.05, so a fully lossy path still ranks at twenty times its rtt instead
    /// of at its raw rtt.
    #[test]
    fn cost_never_scales_by_more_than_twenty_times_the_rtt() {
        let close = |got: f64, want: f64| (got - want).abs() <= want * 1e-9;
        assert!(
            close(score(1.0, 1.0).cost(), 20.0),
            "a fully lossy path must cost 20x its rtt, not {:?}",
            score(1.0, 1.0).cost(),
        );
        assert!(
            close(score(1.0, 0.9).cost(), 10.0),
            "a 90% lossy path must cost 10x its rtt, not {:?}",
            score(1.0, 0.9).cost(),
        );
        assert!(
            close(score(1.0, 0.8).cost(), 5.0),
            "an 80% lossy path must cost 5x its rtt, not {:?}",
            score(1.0, 0.8).cost(),
        );
        assert!(
            close(score(1.0, 0.0).cost(), 1.0),
            "a clean path must cost exactly its rtt, not {:?}",
            score(1.0, 0.0).cost(),
        );
    }

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
