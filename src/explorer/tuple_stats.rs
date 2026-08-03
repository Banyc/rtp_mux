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
    pub(crate) fn drive(&mut self, io: &mut dyn ProbeIo, now: Instant, mean: Duration, epoch: Instant) {
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
