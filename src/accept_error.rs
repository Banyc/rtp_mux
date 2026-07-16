use std::{io, net::SocketAddr, time::Instant};

const WARN_AFTER_CONSECUTIVE: u64 = 3;

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

    pub(crate) fn accepted(&mut self, listener: &str, addr: SocketAddr) {
        if self.error_count > 0 && !self.logged {
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
    }
}
