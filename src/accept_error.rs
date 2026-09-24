use std::{
    io,
    net::SocketAddr,
    time::{Duration, Instant},
};

const WARN_AFTER_CONSECUTIVE: u64 = 3;
const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(1);
const RETRY_BACKOFF_MAX: Duration = Duration::from_millis(100);

fn is_fatal(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::InvalidInput
            | io::ErrorKind::InvalidData
            | io::ErrorKind::PermissionDenied
            | io::ErrorKind::AddrNotAvailable
            | io::ErrorKind::NotConnected
            | io::ErrorKind::Unsupported
    )
}

#[derive(Debug, Default)]
pub(crate) struct AcceptErrorBackoff {
    error_count: u64,
    first_error: Option<String>,
    last_error: Option<String>,
    started_at: Option<Instant>,
    logged: bool,
}

impl AcceptErrorBackoff {
    pub(crate) fn failed_dispatching(
        &mut self,
        listener: &'static str,
        addr: SocketAddr,
        error: io::Error,
    ) -> io::Result<()> {
        let fatal = is_fatal(error.kind());
        let error_msg = error.to_string();
        self.error_count += 1;
        self.started_at.get_or_insert_with(Instant::now);
        self.first_error.get_or_insert_with(|| error_msg.clone());
        self.last_error = Some(error_msg);
        if !self.logged && (fatal || self.error_count >= WARN_AFTER_CONSECUTIVE) {
            self.logged = true;
            tracing::warn!(
                error_count = self.error_count,
                first_error = %self.first_error.as_deref().unwrap_or("?"),
                last_error = %self.last_error.as_deref().unwrap_or("?"),
                elapsed_ms = self.started_at.map(|t| t.elapsed().as_millis()).unwrap_or_default(),
                fatal,
                listener,
                %addr,
                "Listener accept errors"
            );
        }
        if fatal { Err(error) } else { Ok(()) }
    }

    fn retry_delay(&self) -> Duration {
        let Some(over) = self.error_count.checked_sub(WARN_AFTER_CONSECUTIVE) else {
            return Duration::ZERO;
        };
        let delay = RETRY_BACKOFF_BASE * 2u32.pow(over.min(16) as u32);
        delay.min(RETRY_BACKOFF_MAX)
    }

    pub(crate) async fn pause(&self) {
        let delay = self.retry_delay();
        match delay.is_zero() {
            true => tokio::task::yield_now().await,
            false => tokio::time::sleep(delay).await,
        }
    }

    pub(crate) fn accepted(&mut self, listener: &str, addr: SocketAddr) -> bool {
        let recovered = self.logged;
        if recovered {
            tracing::warn!(
                error_count = self.error_count,
                first_error = %self.first_error.as_deref().unwrap_or("?"),
                last_error = %self.last_error.as_deref().unwrap_or("?"),
                elapsed_ms = self.started_at.map(|t| t.elapsed().as_millis()).unwrap_or_default(),
                listener,
                %addr,
                "Listener accept recovered after error streak"
            );
        }
        *self = Self::default();
        recovered
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    fn addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1)
    }

    fn transient(backoff: &mut AcceptErrorBackoff) {
        backoff
            .failed_dispatching(
                "t",
                addr(),
                io::Error::from(io::ErrorKind::ConnectionAborted),
            )
            .expect("a non-fatal accept error is not returned to the caller");
    }

    /// The retry delay is exactly zero until the warning threshold, exactly
    /// doubles from the documented 1 ms base afterwards, and stops at the
    /// documented 100 ms cap: the delay values themselves are pinned, not just
    /// their ordering, so the accept loop cannot be turned into a spin or into
    /// a multi-second stall by a constant change.
    #[test]
    fn the_retry_backoff_doubles_from_the_warn_threshold_to_the_cap() {
        let mut backoff = AcceptErrorBackoff::default();
        let expected = [
            Duration::ZERO,
            Duration::ZERO,
            Duration::from_millis(1),
            Duration::from_millis(2),
            Duration::from_millis(4),
            Duration::from_millis(8),
            Duration::from_millis(16),
            Duration::from_millis(32),
            Duration::from_millis(64),
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(100),
        ];
        for (index, want) in expected.into_iter().enumerate() {
            transient(&mut backoff);
            let errors = index + 1;
            let got = backoff.retry_delay();
            assert_eq!(
                got, want,
                "after {errors} consecutive accept errors the retry delay was {got:?}, expected {want:?}",
            );
        }
    }

    #[test]
    fn a_warned_error_streak_logs_its_recovery() {
        let mut backoff = AcceptErrorBackoff::default();
        for _ in 0..WARN_AFTER_CONSECUTIVE {
            transient(&mut backoff);
        }
        assert!(
            backoff.accepted("t", addr()),
            "the listener recovered from a warned error streak without saying so, \
             leaving the warning open with nothing to retire it",
        );
    }

    #[test]
    fn an_unwarned_error_streak_recovers_quietly() {
        let mut backoff = AcceptErrorBackoff::default();
        transient(&mut backoff);
        assert!(
            !backoff.accepted("t", addr()),
            "a listener that hiccuped once warned on recovery, so a flapping listener warns on every accept",
        );
    }

    #[test]
    fn a_clean_listener_never_warns_on_accept() {
        let mut backoff = AcceptErrorBackoff::default();
        assert!(!backoff.accepted("t", addr()));
    }

    #[test]
    fn a_fatal_accept_error_is_returned_to_the_caller() {
        let mut backoff = AcceptErrorBackoff::default();
        let outcome =
            backoff.failed_dispatching("t", addr(), io::Error::from(io::ErrorKind::InvalidInput));
        let error = outcome
            .expect_err("a permanently fatal accept error was swallowed as if it were retryable");
        assert_eq!(
            error.kind(),
            io::ErrorKind::InvalidInput,
            "the fatal error's kind was lost on the way out",
        );
    }

    #[test]
    fn every_fatal_kind_is_returned_and_a_retryable_kind_is_not() {
        let fatal_kinds = [
            io::ErrorKind::InvalidInput,
            io::ErrorKind::InvalidData,
            io::ErrorKind::PermissionDenied,
            io::ErrorKind::AddrNotAvailable,
            io::ErrorKind::NotConnected,
            io::ErrorKind::Unsupported,
        ];
        for kind in fatal_kinds {
            let mut backoff = AcceptErrorBackoff::default();
            let error = backoff
                .failed_dispatching("t", addr(), io::Error::from(kind))
                .expect_err("a fatal accept-error kind was treated as retryable");
            assert_eq!(
                error.kind(),
                kind,
                "{kind:?} must be classified fatal and returned unchanged",
            );
        }
        let mut backoff = AcceptErrorBackoff::default();
        assert!(
            backoff
                .failed_dispatching(
                    "t",
                    addr(),
                    io::Error::from(io::ErrorKind::ConnectionAborted),
                )
                .is_ok(),
            "a retryable accept-error kind was classified fatal, so one flaky peer stops the listener",
        );
    }

    #[test]
    fn a_persistent_error_streak_stops_spinning() {
        let mut backoff = AcceptErrorBackoff::default();
        transient(&mut backoff);
        assert_eq!(
            backoff.retry_delay(),
            Duration::ZERO,
            "one bad peer delayed the next good one"
        );
        transient(&mut backoff);
        assert_eq!(
            backoff.retry_delay(),
            Duration::ZERO,
            "backoff began before the warn threshold was reached"
        );
        transient(&mut backoff);
        let first = backoff.retry_delay();
        assert!(
            first > Duration::ZERO,
            "a listener that keeps failing is retried with no delay at all",
        );
        for _ in 0..32 {
            transient(&mut backoff);
        }
        assert_eq!(
            backoff.retry_delay(),
            RETRY_BACKOFF_MAX,
            "the delay must climb to a cap and stay there",
        );
    }

    #[test]
    fn recovery_clears_the_backoff() {
        let mut backoff = AcceptErrorBackoff::default();
        for _ in 0..WARN_AFTER_CONSECUTIVE + 4 {
            transient(&mut backoff);
        }
        assert!(backoff.retry_delay() > Duration::ZERO);
        backoff.accepted("t", addr());
        assert_eq!(backoff.retry_delay(), Duration::ZERO);
    }

    /// A computed delay the accept loop never waits out is not backoff: the
    /// listener spins at accept speed through a persistent error streak, and
    /// the log says it is backing off while the CPU does not. The wait is
    /// applied only once the warn threshold is crossed; below it the retry
    /// must still not consume time (a zero-length sleep would still park the
    /// loop behind a timer). The clock is virtual, so the test observes the
    /// wait without paying it.
    #[tokio::test(start_paused = true)]
    async fn pause_waits_out_the_retry_delay_only_after_the_warn_threshold() {
        let mut backoff = AcceptErrorBackoff::default();
        transient(&mut backoff);
        let before = tokio::time::Instant::now();
        backoff.pause().await;
        assert_eq!(
            tokio::time::Instant::now() - before,
            Duration::ZERO,
            "a listener below the warn threshold delayed its next accept",
        );

        for _ in 0..WARN_AFTER_CONSECUTIVE {
            transient(&mut backoff);
        }
        let expected = backoff.retry_delay();
        assert!(
            expected > Duration::ZERO,
            "a warned error streak must compute a non-zero retry delay",
        );
        let before = tokio::time::Instant::now();
        backoff.pause().await;
        assert_eq!(
            tokio::time::Instant::now() - before,
            expected,
            "the accept loop did not wait out the retry delay it computed, so a persistent \
             error streak is retried at full speed",
        );
    }
}
