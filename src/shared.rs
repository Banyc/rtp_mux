use std::{io, net::SocketAddr, time::Duration};

use mux::{Initiation, MuxConfig};

pub(crate) const PAIRING_DEADLINE: Duration = Duration::from_secs(10);
pub(crate) const HELLO_DEADLINE: Duration = Duration::from_secs(5);
pub(crate) const BIRTH_LIVENESS_DEADLINE: Duration = Duration::from_millis(2500);
pub(crate) const BIRTH_LIVENESS_GRACE: Duration = Duration::from_millis(250);
pub(crate) const MAX_DUAL_CONNECT_ATTEMPTS: usize = 3;
pub(crate) const MAX_PENDING_LANES: usize = 1024;
pub(crate) const MAX_PENDING_LANES_PER_PEER: usize = 32;
pub(crate) const ADMISSION_REJECTION_LOG_INTERVAL: Duration = Duration::from_secs(5);
pub(crate) const MAX_CONCURRENT_DUAL_DIALS: usize = 32;
pub(crate) const MAX_DIAL_WAITERS_PER_ADDR: usize = 256;

pub(crate) fn bulk_lane_addr(interactive: SocketAddr) -> io::Result<SocketAddr> {
    let port = interactive.port().checked_add(1).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "RTP mux bulk lane port overflows u16",
        )
    })?;
    let mut bulk = interactive;
    bulk.set_port(port);
    Ok(bulk)
}

pub(crate) fn interactive_server_mux_config() -> MuxConfig {
    MuxConfig {
        initiation: Initiation::Server,
        heartbeat_interval: Duration::from_secs(5),
        frame_reassembly: true,
    }
}

pub(crate) fn interactive_client_mux_config() -> MuxConfig {
    MuxConfig {
        initiation: Initiation::Client,
        heartbeat_interval: Duration::from_secs(5),
        frame_reassembly: true,
    }
}

pub(crate) fn bulk_server_mux_config() -> MuxConfig {
    MuxConfig {
        initiation: Initiation::Server,
        heartbeat_interval: Duration::from_secs(5),
        frame_reassembly: true,
    }
}

pub(crate) fn bulk_client_mux_config() -> MuxConfig {
    MuxConfig {
        initiation: Initiation::Client,
        heartbeat_interval: Duration::from_secs(5),
        frame_reassembly: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dual_lane_configs_enable_frame_reassembly() {
        assert!(interactive_client_mux_config().frame_reassembly);
        assert!(interactive_server_mux_config().frame_reassembly);
        assert!(bulk_client_mux_config().frame_reassembly);
        assert!(bulk_server_mux_config().frame_reassembly);
    }
}
