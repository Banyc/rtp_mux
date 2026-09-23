use std::{io, net::SocketAddr, time::Duration};

use mux::{Initiation, MuxConfig};

pub(crate) const PAIRING_DEADLINE: Duration = Duration::from_secs(10);
pub(crate) const HELLO_DEADLINE: Duration = Duration::from_secs(5);
/// Birth liveness: the dual-lane hello handshake and first receive must complete within these deadlines.
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

/// The interactive lane's default FEC policy, stated once for the whole
/// composition: the prompt-parity preset, with in-stream group FEC left at the
/// rtp transport default (environment-selected). The bulk lane's FEC-free
/// policy is owned by [`crate::lane_transport`], not here.
pub(crate) fn interactive_lane_fec_policy() -> (rtp::FecTuning, bool) {
    (
        rtp::FecTuning::interactive_prompt(),
        rtp::udp::AcceptConfig::default().instream_group_fec,
    )
}

pub(crate) fn lane_mux_config(initiation: Initiation) -> MuxConfig {
    MuxConfig {
        initiation,
        heartbeat_interval: Duration::from_secs(5),
        frame_reassembly: true,
    }
}
pub(crate) fn server_mux_config() -> MuxConfig {
    lane_mux_config(Initiation::Server)
}
pub(crate) fn client_mux_config() -> MuxConfig {
    lane_mux_config(Initiation::Client)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), port)
    }

    /// The bulk lane is addressed as the interactive lane's port plus one:
    /// the connector derives the bulk destination from the interactive one, so
    /// an offset of anything but one (or a silently reused port, which would
    /// collide with the interactive lane) misroutes the bulk lane.
    #[test]
    fn the_bulk_lane_address_is_the_interactive_port_plus_one() {
        assert_eq!(
            bulk_lane_addr(addr(1234)).expect("a non-final port has a bulk lane"),
            addr(1235),
            "the bulk lane must be the interactive port plus one",
        );
        assert_eq!(
            bulk_lane_addr(addr(65534)).expect("65534 + 1 is still a valid port"),
            addr(65535),
            "the last valid interactive/bulk port pair must be usable",
        );
        let overflow = bulk_lane_addr(addr(u16::MAX))
            .expect_err("there is no port above 65535, so the pair cannot exist");
        assert_eq!(
            overflow.kind(),
            io::ErrorKind::InvalidInput,
            "a bulk lane that cannot exist is a caller input error",
        );
    }

    /// The interactive lane's FEC policy has one authority. Both the connector
    /// default and the server default must be this exact pair; a second
    /// statement of either half (a literal preset, or a lane-local re-read of
    /// the transport default) is the divergence this pins.
    #[test]
    fn the_interactive_lane_fec_policy_is_the_prompt_preset_over_the_transport_default() {
        let (tuning, instream_group_fec) = interactive_lane_fec_policy();
        assert_eq!(
            tuning,
            rtp::FecTuning::interactive_prompt(),
            "the interactive lane must force-flush prompt parity by default",
        );
        assert_eq!(
            instream_group_fec,
            rtp::udp::AcceptConfig::default().instream_group_fec,
            "in-stream group FEC must stay at the transport default, not be re-decided here",
        );
        assert_eq!(
            instream_group_fec,
            rtp::udp::ConnectConfig::default().instream_group_fec,
            "the connect and accept transport defaults must agree, so one authority can serve both",
        );
    }

    #[test]
    fn the_two_lane_configs_declare_complementary_mux_roles() {
        let client = client_mux_config();
        let server = server_mux_config();
        assert_eq!(
            client.initiation,
            Initiation::Client,
            "the dialing lane config must initiate as the mux client",
        );
        assert_eq!(
            server.initiation,
            Initiation::Server,
            "the accepting lane config must initiate as the mux server",
        );
        assert!(
            client.frame_reassembly && server.frame_reassembly,
            "both lane configs must enable frame reassembly",
        );
        assert_eq!(client.heartbeat_interval, Duration::from_secs(5));
        assert_eq!(server.heartbeat_interval, Duration::from_secs(5));
    }
}
