use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use super::ProbeIo;
use super::path_score::PathScore;

pub(crate) const PROBE_TIMEOUT: Duration = Duration::from_millis(1500);
pub(crate) const PROBE_POLL_TICK: Duration = Duration::from_millis(5);
pub(crate) const DEAD_CONSECUTIVE_LOSSES: u32 = 3;
const DEAD_CADENCE_FACTOR: u32 = 3;
pub(crate) const MIN_SAMPLES: u32 = 3;
const EWMA_ALPHA: f64 = 0.3;

#[derive(Debug)]
pub(crate) struct TupleStats {
    rtt: Option<Duration>,
    loss: Option<f64>,
    samples: u32,
    consecutive_losses: u32,
    outstanding: Option<(u64, Instant)>,
    next_probe_at: Instant,
}

impl TupleStats {
    pub(crate) fn new(now: Instant, mean: Duration) -> Self {
        let stagger = mean.mul_f64(0.5 + 0.5 * rand::random::<f64>());
        Self {
            rtt: None,
            loss: None,
            samples: 0,
            consecutive_losses: 0,
            outstanding: None,
            next_probe_at: now + stagger,
        }
    }
    pub(crate) fn dead(&self) -> bool {
        self.consecutive_losses >= DEAD_CONSECUTIVE_LOSSES
    }
    fn alive(&self) -> bool {
        self.samples >= MIN_SAMPLES && self.rtt.is_some() && !self.dead()
    }
    pub(crate) fn score(&self) -> Option<PathScore> {
        self.alive().then(|| PathScore {
            rtt: self.rtt.expect("alive implies a measured rtt"),
            loss: self.loss.unwrap_or(0.0),
        })
    }
    fn record(&mut self, rtt: Option<Duration>) {
        self.samples = self.samples.saturating_add(1);
        let loss_sample = if rtt.is_some() { 0.0 } else { 1.0 };
        self.loss = Some(match self.loss {
            None => loss_sample,
            Some(prev) => prev * (1.0 - EWMA_ALPHA) + loss_sample * EWMA_ALPHA,
        });
        match rtt {
            Some(sample) => {
                self.consecutive_losses = 0;
                self.rtt = Some(match self.rtt {
                    None => sample,
                    Some(prev) => prev.mul_f64(1.0 - EWMA_ALPHA) + sample.mul_f64(EWMA_ALPHA),
                });
            }
            None => self.consecutive_losses = self.consecutive_losses.saturating_add(1),
        }
    }
    pub(crate) fn tick(
        &mut self,
        io: &mut dyn ProbeIo,
        now: Instant,
        mean: Duration,
        epoch: Instant,
    ) {
        while let Some(echo) = io.try_recv_echo() {
            if let Some((expected, sent_at)) = self.outstanding
                && echo.nonce == expected
            {
                self.outstanding = None;
                self.record(Some(now.duration_since(sent_at)));
            }
        }
        if let Some((_, sent_at)) = self.outstanding
            && now.duration_since(sent_at) >= PROBE_TIMEOUT
        {
            self.outstanding = None;
            self.record(None);
        }
        if self.outstanding.is_none() && now >= self.next_probe_at {
            let nonce = rand::random();
            let echo = rtp::probe::ProbeEcho {
                nonce,
                timestamp_micros: now.duration_since(epoch).as_micros() as u64,
            };
            match io.send_probe(echo) {
                Ok(()) => self.outstanding = Some((nonce, now)),
                Err(_) => self.record(None),
            }
            let mean = if self.dead() {
                mean * DEAD_CADENCE_FACTOR
            } else {
                mean
            };
            self.next_probe_at = now + poisson_interval(mean);
        }
    }
    pub(crate) fn next_wakeup(&self, now: Instant) -> Instant {
        if self.outstanding.is_some() {
            now + PROBE_POLL_TICK
        } else {
            self.next_probe_at
        }
    }
    pub(crate) fn report(&self, local_addr: SocketAddr) -> TupleReport {
        TupleReport {
            local_addr,
            rtt: self.rtt,
            loss: self.loss,
            alive: self.alive(),
        }
    }
}

fn poisson_interval(mean: Duration) -> Duration {
    let u: f64 = rand::random::<f64>().clamp(f64::MIN_POSITIVE, 1.0);
    let interval = mean.mul_f64(-u.ln());
    interval.clamp(mean / 4, mean * 4)
}

#[derive(Debug, Clone)]
pub struct TupleReport {
    pub local_addr: SocketAddr,
    pub rtt: Option<Duration>,
    pub loss: Option<f64>,
    pub alive: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::explorer::ProbeIo;
    use rtp::probe::ProbeEcho;
    use std::{collections::VecDeque, io, net::SocketAddr};

    fn local() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 1))
    }

    #[derive(Debug, Default)]
    struct ScriptedIo {
        sent: Vec<u64>,
        echoes: VecDeque<u64>,
    }

    impl ProbeIo for ScriptedIo {
        fn send_probe(&mut self, echo: ProbeEcho) -> io::Result<()> {
            self.sent.push(echo.nonce);
            Ok(())
        }

        fn try_recv_echo(&mut self) -> Option<ProbeEcho> {
            self.echoes.pop_front().map(|nonce| ProbeEcho {
                nonce,
                timestamp_micros: 0,
            })
        }
    }

    #[test]
    fn an_echo_for_a_stale_probe_is_not_counted_as_the_outstanding_sample() {
        let epoch = Instant::now();
        let mean = Duration::from_secs(8);
        let mut stats = TupleStats::new(epoch, mean);
        let mut io = ScriptedIo::default();
        // The first probe is staggered within [mean/2, mean], so ticking a
        // full mean later guarantees it has been sent.
        stats.tick(&mut io, epoch + mean, mean, epoch);
        assert_eq!(io.sent.len(), 1, "no probe is outstanding to sample");
        // Deliver an echo whose nonce belongs to some other probe.
        io.echoes.push_back(io.sent[0].wrapping_add(1));
        stats.tick(
            &mut io,
            epoch + mean + Duration::from_millis(1),
            mean,
            epoch,
        );
        assert!(
            stats.report(local()).rtt.is_none(),
            "an echo for a different probe was counted as this probe's round trip",
        );
    }

    /// Exactly three consecutive losses retire a tuple: two are still a live
    /// tuple, so one streak boundary decides when a path stops being handed
    /// out without any protocol-side evidence.
    #[test]
    fn exactly_three_consecutive_losses_mark_a_tuple_dead() {
        let mut stats = TupleStats::new(Instant::now(), Duration::from_secs(8));
        stats.record(Some(Duration::from_millis(10)));
        for losses in 1..=2 {
            stats.record(None);
            assert!(
                !stats.dead(),
                "{losses} consecutive losses must not retire the tuple yet",
            );
        }
        stats.record(None);
        assert!(
            stats.dead(),
            "three consecutive losses must retire the tuple",
        );
        assert!(
            stats.score().is_none(),
            "a dead tuple must not be scored, so it is never selected",
        );
    }

    /// Exactly three samples make a tuple scorable; a loss sample counts
    /// toward the warm-up too, so a path that has answered nothing yet still
    /// needs the same three probes before it can be selected.
    #[test]
    fn exactly_three_samples_make_a_tuple_scorable() {
        let mut stats = TupleStats::new(Instant::now(), Duration::from_secs(8));
        stats.record(Some(Duration::from_millis(10)));
        stats.record(None);
        assert!(
            stats.score().is_none(),
            "two samples must not make a tuple selectable",
        );
        assert!(
            !stats.report(local()).alive,
            "a two-sample tuple must still report itself as warming up",
        );
        stats.record(Some(Duration::from_millis(10)));
        assert!(
            stats.score().is_some(),
            "three samples must make the tuple selectable",
        );
        assert!(
            stats.report(local()).alive,
            "a three-sample tuple must report itself as alive",
        );
    }

    /// A probe is charged as lost at exactly its timeout: one tick before the
    /// deadline the probe is still outstanding, and a tick landing exactly on
    /// the deadline records the loss (so the timeout is not deferred by a
    /// further poll interval each time).
    #[test]
    fn a_probe_is_recorded_lost_at_exactly_its_timeout() {
        let epoch = Instant::now();
        let mean = Duration::from_secs(8);
        let mut stats = TupleStats::new(epoch, mean);
        let mut io = ScriptedIo::default();
        let sent_at = epoch + mean;
        stats.tick(&mut io, sent_at, mean, epoch);
        assert_eq!(io.sent.len(), 1, "a probe must be outstanding to time out");
        stats.tick(
            &mut io,
            sent_at + PROBE_TIMEOUT - Duration::from_millis(1),
            mean,
            epoch,
        );
        assert_eq!(
            stats.report(local()).loss,
            None,
            "a probe must not be charged lost before its timeout",
        );
        stats.tick(&mut io, sent_at + PROBE_TIMEOUT, mean, epoch);
        assert_eq!(
            stats.report(local()).loss,
            Some(1.0),
            "a probe must be charged lost exactly at its timeout",
        );
        assert_eq!(
            io.sent.len(),
            1,
            "the timeout must not also send a new probe"
        );
    }

    #[test]
    fn the_rtt_ewma_weights_a_new_sample_less_than_the_history() {
        let mut stats = TupleStats::new(Instant::now(), Duration::from_secs(8));
        stats.record(Some(Duration::from_millis(10)));
        stats.record(Some(Duration::from_millis(20)));
        assert_eq!(
            stats.report(local()).rtt,
            Some(Duration::from_millis(13)),
            "the rtt EWMA inverted the weight of the newest sample",
        );
    }

    #[test]
    fn the_loss_ewma_weights_a_new_sample_less_than_the_history() {
        let mut stats = TupleStats::new(Instant::now(), Duration::from_secs(8));
        stats.record(None);
        stats.record(Some(Duration::from_millis(10)));
        let loss = stats.report(local()).loss.unwrap();
        assert!(
            (loss - 0.7).abs() < 1e-9,
            "the loss EWMA inverted the weight of the newest sample: {loss}",
        );
    }
}
