mod path_score;
mod tuple_stats;

use std::{
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};

pub use path_score::{PathScore, ReoptRule, ReoptVerdict};
pub use tuple_stats::TupleReport;
use tuple_stats::TupleStats;

#[derive(Debug, Clone)]
pub struct ExplorerConfig {
    pub enabled: bool,
    pub candidates: usize,
    pub probe_mean_interval: Duration,
    pub rotation_period: Duration,
}

impl Default for ExplorerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            candidates: 8,
            probe_mean_interval: Duration::from_secs(10),
            rotation_period: Duration::from_secs(120),
        }
    }
}

pub(crate) trait ProbeIo: Send {
    fn send_probe(&mut self, echo: rtp::probe::ProbeEcho) -> io::Result<()>;
    fn try_recv_echo(&mut self) -> Option<rtp::probe::ProbeEcho>;
}
impl ProbeIo for rtp::probe::ProbeTap {
    fn send_probe(&mut self, echo: rtp::probe::ProbeEcho) -> io::Result<()> {
        rtp::probe::ProbeTap::send_probe(self, echo)
    }
    fn try_recv_echo(&mut self) -> Option<rtp::probe::ProbeEcho> {
        rtp::probe::ProbeTap::try_recv_echo(self)
    }
}

#[derive(Debug)]
pub(crate) struct SocketCandidate {
    socket: tokio_udp::UdpSocket,
}

impl SocketCandidate {
    pub(crate) async fn mint(
        bind_ip: std::net::IpAddr,
        remote: SocketAddr,
    ) -> io::Result<(Self, SocketAddr)> {
        let socket = tokio_udp::UdpSocket::bind(SocketAddr::new(bind_ip, 0)).await?;
        socket.connect(remote).await?;
        let local = socket.local_addr()?;
        Ok((Self { socket }, local))
    }
    pub(crate) fn into_socket(self) -> tokio_udp::UdpSocket {
        self.socket
    }
}

impl ProbeIo for SocketCandidate {
    fn send_probe(&mut self, echo: rtp::probe::ProbeEcho) -> io::Result<()> {
        self.socket
            .try_send(&rtp::probe::encode_probe(echo))
            .map(drop)
    }
    fn try_recv_echo(&mut self) -> Option<rtp::probe::ProbeEcho> {
        let mut buf = [0u8; 64];
        loop {
            let n = self.socket.try_recv(&mut buf).ok()?;
            if let Some(echo) = rtp::probe::decode_echo(&buf[..n]) {
                return Some(echo);
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExplorerReport {
    pub candidates: Vec<TupleReport>,
    pub active: Option<TupleReport>,
}

struct Candidate<C> {
    io: C,
    local_addr: SocketAddr,
    stats: TupleStats,
}
pub(crate) struct Explorer<C> {
    config: ExplorerConfig,
    candidates: Vec<Candidate<C>>,
    active: Option<Candidate<Box<dyn ProbeIo>>>,
    next_rotation_at: Instant,
    refill_after: Instant,
    epoch: Instant,
}

impl<C: ProbeIo> Explorer<C> {
    pub(crate) fn new(config: ExplorerConfig, now: Instant) -> Self {
        let next_rotation_at = now + config.rotation_period;
        Self {
            config,
            candidates: Vec::new(),
            active: None,
            next_rotation_at,
            refill_after: now,
            epoch: now,
        }
    }
    pub(crate) fn deficit(&self) -> usize {
        self.config.candidates.saturating_sub(self.candidates.len())
    }
    pub(crate) fn wants_refill(&self, now: Instant) -> bool {
        self.deficit() > 0 && now >= self.refill_after
    }
    pub(crate) fn defer_refill(&mut self, until: Instant) {
        self.refill_after = until;
    }
    pub(crate) fn add_candidate(&mut self, io: C, local_addr: SocketAddr, now: Instant) {
        self.candidates.push(Candidate {
            io,
            local_addr,
            stats: TupleStats::new(now, self.config.probe_mean_interval),
        });
    }
    pub(crate) fn set_active(
        &mut self,
        active: Option<(Box<dyn ProbeIo>, SocketAddr)>,
        now: Instant,
    ) {
        self.active = active.map(|(io, local_addr)| Candidate {
            io,
            local_addr,
            stats: TupleStats::new(now, self.config.probe_mean_interval),
        });
    }
    pub(crate) fn tick(&mut self, now: Instant) {
        let mean = self.config.probe_mean_interval;
        for candidate in &mut self.candidates {
            candidate
                .stats
                .drive(&mut candidate.io, now, mean, self.epoch);
        }
        if let Some(active) = &mut self.active {
            active
                .stats
                .drive(active.io.as_mut(), now, mean, self.epoch);
        }
        if now >= self.next_rotation_at {
            self.next_rotation_at = now + self.config.rotation_period;
            self.rotate_worst();
        }
    }
    fn rotate_worst(&mut self) {
        if self.deficit() > 0 {
            return;
        }
        let worst = self
            .candidates
            .iter()
            .enumerate()
            .filter_map(|(index, c)| {
                let cost = if c.stats.dead() {
                    f64::INFINITY
                } else {
                    c.stats.score()?.cost()
                };
                Some((index, cost))
            })
            .max_by(|(_, a), (_, b)| a.total_cmp(b))
            .map(|(index, _)| index);
        if let Some(index) = worst {
            self.candidates.swap_remove(index);
        }
    }
    fn best_index(&self) -> Option<usize> {
        self.candidates
            .iter()
            .enumerate()
            .filter_map(|(index, c)| c.stats.score().map(|score| (index, score.cost())))
            .min_by(|(_, a), (_, b)| a.total_cmp(b))
            .map(|(index, _)| index)
    }
    pub(crate) fn best_score(&self) -> Option<PathScore> {
        self.best_index()
            .and_then(|index| self.candidates[index].stats.score())
    }
    pub(crate) fn take_best(&mut self) -> Option<(C, SocketAddr, PathScore)> {
        let index = self.best_index()?;
        let candidate = self.candidates.swap_remove(index);
        let score = candidate
            .stats
            .score()
            .expect("best_index only returns scored candidates");
        Some((candidate.io, candidate.local_addr, score))
    }
    pub(crate) fn reoptimize_verdict(&self) -> ReoptVerdict {
        let Some(active) = self.active.as_ref().and_then(|active| active.stats.score()) else {
            return ReoptVerdict::ActiveUnmeasured;
        };
        let Some(best) = self.best_score() else {
            return ReoptVerdict::NoLiveCandidate { active };
        };
        match best.beats_by_margin(&active) {
            Some(rule) => ReoptVerdict::Migrate { rule, active, best },
            None => ReoptVerdict::WithinMargin { active, best },
        }
    }
    pub(crate) fn next_wakeup(&self, now: Instant) -> Option<Instant> {
        let candidates = self
            .candidates
            .iter()
            .map(|c| c.stats.next_wakeup(now))
            .min();
        let active = self
            .active
            .as_ref()
            .map(|active| active.stats.next_wakeup(now));
        let refill = (self.deficit() > 0).then_some(self.refill_after);
        [candidates, active, refill, Some(self.next_rotation_at)]
            .into_iter()
            .flatten()
            .min()
    }
    pub(crate) fn report(&self) -> ExplorerReport {
        ExplorerReport {
            candidates: self
                .candidates
                .iter()
                .map(|c| c.stats.report(c.local_addr))
                .collect(),
            active: self
                .active
                .as_ref()
                .map(|active| active.stats.report(active.local_addr)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::tuple_stats::{
        DEAD_CONSECUTIVE_LOSSES, MIN_SAMPLES, PROBE_POLL_TICK, PROBE_TIMEOUT,
    };
    use super::*;

    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    #[derive(Debug, Clone, Default)]
    struct FakeIo {
        sent: Arc<Mutex<Vec<u64>>>,
        echoes: Arc<Mutex<VecDeque<u64>>>,
        send_fails: bool,
    }

    impl FakeIo {
        fn broken() -> Self {
            Self {
                send_fails: true,
                ..Self::default()
            }
        }
        // Echo every probe sent since the last call.
        fn echo_all(&self) {
            let mut sent = self.sent.lock().unwrap();
            self.echoes.lock().unwrap().extend(sent.drain(..));
        }
    }

    impl ProbeIo for FakeIo {
        fn send_probe(&mut self, echo: rtp::probe::ProbeEcho) -> io::Result<()> {
            if self.send_fails {
                return Err(io::Error::from(io::ErrorKind::NetworkUnreachable));
            }
            self.sent.lock().unwrap().push(echo.nonce);
            Ok(())
        }

        fn try_recv_echo(&mut self) -> Option<rtp::probe::ProbeEcho> {
            self.echoes
                .lock()
                .unwrap()
                .pop_front()
                .map(|nonce| rtp::probe::ProbeEcho {
                    nonce,
                    timestamp_micros: 0,
                })
        }
    }

    fn config() -> ExplorerConfig {
        ExplorerConfig {
            enabled: true,
            candidates: 2,
            probe_mean_interval: Duration::from_secs(8),
            rotation_period: Duration::from_secs(3600),
        }
    }

    fn local(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn probe_cycle(
        explorer: &mut Explorer<FakeIo>,
        now: &mut Instant,
        responders: &[(&FakeIo, Duration)],
    ) {
        *now += config().probe_mean_interval * 4;
        explorer.tick(*now);
        let mut receive_at = *now;
        for (io, rtt) in responders {
            io.echo_all();
            receive_at = receive_at.max(*now + *rtt);
            explorer.tick(*now + *rtt);
        }
        *now = receive_at;
        *now += PROBE_TIMEOUT;
        explorer.tick(*now);
    }

    fn warmed_pair(now: &mut Instant) -> (Explorer<FakeIo>, FakeIo, FakeIo) {
        let mut explorer = Explorer::new(config(), *now);
        let fast = FakeIo::default();
        let slow = FakeIo::default();
        explorer.add_candidate(fast.clone(), local(1000), *now);
        explorer.add_candidate(slow.clone(), local(2000), *now);
        for _ in 0..MIN_SAMPLES {
            probe_cycle(
                &mut explorer,
                now,
                &[
                    (&fast, Duration::from_millis(10)),
                    (&slow, Duration::from_millis(200)),
                ],
            );
        }
        (explorer, fast, slow)
    }

    #[test]
    fn take_best_prefers_the_lower_rtt_candidate_and_leaves_a_deficit() {
        let mut now = Instant::now();
        let (mut explorer, _fast, _slow) = warmed_pair(&mut now);
        let (_io, local_addr, score) = explorer.take_best().expect("warm explorer");
        assert_eq!(local_addr, local(1000));
        assert!(score.rtt < Duration::from_millis(50), "{score:?}");
        assert_eq!(explorer.deficit(), 1);
        explorer.add_candidate(FakeIo::default(), local(3000), now);
        assert_eq!(explorer.deficit(), 0);
        let (_io, local_addr, _score) = explorer.take_best().expect("slow is still alive");
        assert_eq!(local_addr, local(2000));
        assert!(
            explorer.take_best().is_none(),
            "fresh mint is not scored yet"
        );
    }
    #[test]
    fn cold_explorer_surrenders_nothing() {
        let mut explorer = Explorer::new(config(), Instant::now());
        explorer.add_candidate(FakeIo::default(), local(1000), Instant::now());
        assert!(explorer.take_best().is_none());
        assert_eq!(
            explorer.reoptimize_verdict(),
            ReoptVerdict::ActiveUnmeasured,
            "no active session means nothing to beat"
        );
    }
    #[test]
    fn loss_marks_a_candidate_dead_and_ineligible() {
        let mut now = Instant::now();
        let mut explorer = Explorer::new(config(), now);
        let mute = FakeIo::default();
        explorer.add_candidate(mute.clone(), local(1000), now);
        for _ in 0..DEAD_CONSECUTIVE_LOSSES + 1 {
            probe_cycle(&mut explorer, &mut now, &[]);
        }
        assert!(
            explorer.take_best().is_none(),
            "dead tuple must not be handed off"
        );
    }
    #[test]
    fn lossy_candidate_scores_worse_than_a_clean_one_at_equal_rtt() {
        let mut now = Instant::now();
        let mut explorer = Explorer::new(config(), now);
        let clean = FakeIo::default();
        let lossy = FakeIo::default();
        explorer.add_candidate(clean.clone(), local(1000), now);
        explorer.add_candidate(lossy.clone(), local(2000), now);
        let rtt = Duration::from_millis(50);
        for cycle in 0..6 {
            let mut responders = vec![(&clean, rtt)];
            if cycle % 2 == 0 {
                responders.push((&lossy, rtt));
            }
            probe_cycle(&mut explorer, &mut now, &responders);
        }
        let (_io, local_addr, score) = explorer.take_best().expect("clean candidate");
        assert_eq!(local_addr, local(1000));
        assert!(score.loss < 0.05, "{score:?}");
    }
    #[test]
    fn rotation_retires_the_worst_measured_candidate() {
        let mut now = Instant::now();
        let (mut explorer, _fast, _slow) = warmed_pair(&mut now);
        explorer.next_rotation_at = now;
        explorer.tick(now);
        assert_eq!(explorer.deficit(), 1);
        let report = explorer.report();
        assert_eq!(report.candidates.len(), 1);
        assert_eq!(report.candidates[0].local_addr, local(1000));
    }
    #[test]
    fn a_declined_reoptimize_distinguishes_its_two_no_op_cases() {
        let mut now = Instant::now();
        let mut explorer: Explorer<FakeIo> = Explorer::new(config(), now);
        let active = FakeIo::default();
        explorer.set_active(Some((Box::new(active.clone()), local(9000))), now);
        assert_eq!(
            explorer.reoptimize_verdict(),
            ReoptVerdict::ActiveUnmeasured,
            "the active tuple has not reached MIN_SAMPLES yet"
        );
        for _ in 0..MIN_SAMPLES {
            probe_cycle(
                &mut explorer,
                &mut now,
                &[(&active, Duration::from_millis(9))],
            );
        }
        let verdict = explorer.reoptimize_verdict();
        assert!(
            matches!(verdict, ReoptVerdict::NoLiveCandidate { .. }),
            "a measured active with no candidates: {verdict:?}"
        );
        assert!(verdict.active().is_some(), "the active score is reportable");
        assert!(verdict.best().is_none(), "there is no best to report");
    }
    #[test]
    fn a_report_names_the_active_tuple() {
        let now = Instant::now();
        let mut explorer: Explorer<FakeIo> = Explorer::new(config(), now);
        explorer.set_active(Some((Box::new(FakeIo::default()), local(3000))), now);
        let report = explorer.report();
        assert_eq!(
            report.active.expect("an active tuple was set").local_addr,
            local(3000),
        );
    }
    #[test]
    fn rotation_never_retires_unmeasured_candidates() {
        let now = Instant::now();
        let mut explorer = Explorer::new(config(), now);
        explorer.add_candidate(FakeIo::default(), local(1000), now);
        explorer.add_candidate(FakeIo::default(), local(2000), now);
        explorer.next_rotation_at = now;
        explorer.tick(now);
        assert_eq!(explorer.deficit(), 0);
    }
    #[test]
    fn rotation_retires_a_dead_tuple_before_a_warming_one() {
        let mut now = Instant::now();
        let mut explorer = Explorer::new(config(), now);
        let dead = FakeIo::default();
        explorer.add_candidate(dead.clone(), local(1000), now);
        for _ in 0..DEAD_CONSECUTIVE_LOSSES {
            probe_cycle(&mut explorer, &mut now, &[]);
        }
        let warming = FakeIo::default();
        explorer.add_candidate(warming.clone(), local(2000), now);
        probe_cycle(
            &mut explorer,
            &mut now,
            &[(&warming, Duration::from_millis(5))],
        );
        explorer.next_rotation_at = now;
        explorer.tick(now);
        let report = explorer.report();
        assert_eq!(report.candidates.len(), 1);
        assert_eq!(
            report.candidates[0].local_addr,
            local(2000),
            "rotation must retire the dead tuple, not the warming one",
        );
    }
    #[test]
    fn a_candidate_that_cannot_send_is_retired_like_a_lost_one() {
        let mut now = Instant::now();
        let mut explorer = Explorer::new(config(), now);
        let good = FakeIo::default();
        explorer.add_candidate(FakeIo::broken(), local(1000), now);
        explorer.add_candidate(good.clone(), local(2000), now);
        for _ in 0..DEAD_CONSECUTIVE_LOSSES {
            probe_cycle(
                &mut explorer,
                &mut now,
                &[(&good, Duration::from_millis(5))],
            );
        }
        explorer.next_rotation_at = now;
        explorer.tick(now);
        let report = explorer.report();
        assert_eq!(report.candidates.len(), 1);
        assert_eq!(
            report.candidates[0].local_addr,
            local(2000),
            "an unsendable candidate held its slot instead of being retired",
        );
    }
    #[test]
    fn reoptimize_requires_a_margin_win_over_the_active_tuple() {
        let mut now = Instant::now();
        let (mut explorer, fast, slow) = warmed_pair(&mut now);
        let ms = Duration::from_millis;
        assert!(!explorer.reoptimize_verdict().wants_migration());
        let active = FakeIo::default();
        explorer.set_active(Some((Box::new(active.clone()), local(9000))), now);
        for _ in 0..MIN_SAMPLES {
            probe_cycle(
                &mut explorer,
                &mut now,
                &[(&fast, ms(10)), (&active, ms(11)), (&slow, ms(200))],
            );
        }
        assert!(
            matches!(
                explorer.reoptimize_verdict(),
                ReoptVerdict::WithinMargin { .. }
            ),
            "11ms - 10ms is within margin: {:?}",
            explorer.reoptimize_verdict()
        );
        explorer.set_active(Some((Box::new(active.clone()), local(9000))), now);
        for _ in 0..MIN_SAMPLES {
            probe_cycle(
                &mut explorer,
                &mut now,
                &[(&fast, ms(10)), (&active, ms(100)), (&slow, ms(200))],
            );
        }
        let verdict = explorer.reoptimize_verdict();
        assert!(
            matches!(
                verdict,
                ReoptVerdict::Migrate {
                    rule: ReoptRule::Rtt,
                    ..
                }
            ),
            "100ms - 10ms clears the rtt margin: {verdict:?}"
        );
        let (active, best) = (verdict.active().unwrap(), verdict.best().unwrap());
        assert!(active.rtt > best.rtt * 2, "{active:?} vs {best:?}");
        explorer.set_active(Some((Box::new(FakeIo::default()), local(9000))), now);
        assert!(!explorer.reoptimize_verdict().wants_migration());
    }
    #[test]
    fn margin_math_matches_the_documented_constants() {
        let ms = |n: u64| Duration::from_millis(n);
        let score = |rtt, loss| PathScore { rtt, loss };
        assert_eq!(
            score(ms(75), 0.0).beats_by_margin(&score(ms(100), 0.0)),
            Some(ReoptRule::Rtt)
        );
        assert_eq!(
            score(ms(80), 0.0).beats_by_margin(&score(ms(100), 0.0)),
            None
        );
        assert_eq!(
            score(ms(70), 0.10).beats_by_margin(&score(ms(100), 0.0)),
            None
        );
        assert_eq!(
            score(ms(100), 0.0).beats_by_margin(&score(ms(100), 0.15)),
            Some(ReoptRule::Loss)
        );
        assert_eq!(
            score(ms(200), 0.0).beats_by_margin(&score(ms(100), 0.15)),
            None
        );
    }
    #[test]
    fn wakeups_poll_finely_only_while_a_probe_is_outstanding() {
        let now = Instant::now();
        let mut explorer = Explorer::new(config(), now);
        explorer.add_candidate(FakeIo::default(), local(1000), now);
        explorer.add_candidate(FakeIo::default(), local(2000), now);
        let idle = explorer.next_wakeup(now).unwrap();
        assert!(idle >= now + config().probe_mean_interval / 4);
        let sending = now + config().probe_mean_interval * 4;
        explorer.tick(sending);
        assert_eq!(
            explorer.next_wakeup(sending).unwrap(),
            sending + PROBE_POLL_TICK
        );
    }
}
