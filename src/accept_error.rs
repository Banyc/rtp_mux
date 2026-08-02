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
    fn a_persistent_error_streak_stops_spinning() {
        let mut backoff = AcceptErrorBackoff::default();
        transient(&mut backoff);
        assert_eq!(
            backoff.retry_delay(),
            Duration::ZERO,
            "one bad peer delayed the next good one"
        );
        for _ in 1..WARN_AFTER_CONSECUTIVE {
            transient(&mut backoff);
        }
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
}
