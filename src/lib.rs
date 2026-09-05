#![warn(clippy::disallowed_methods, clippy::disallowed_types)]

mod accept_error;
mod admission;
mod bidirectional;
mod byte_count;
mod client_stream;
mod connector;
mod explorer;
mod group;
mod lane_rejection;
mod lane_transport;
mod migrating_write_half;
mod server;
mod session;
mod shared;
mod stream;
mod task_scope;

pub use bidirectional::{
    BidirectionalSession, BidirectionalSessionDriver, connect_bidirectional_session,
};
pub use byte_count::SessionStats;
pub use client_stream::ClientStream;
pub use connector::{
    BindSelector, BulkAddrSelector, OpenedStream, RtpMuxConnector, RtpMuxConnectorConfig,
    RtpMuxConnectorDriver, SessionView,
};
pub use explorer::{ExplorerConfig, ExplorerReport, PathScore, TupleReport};
pub use migrating_write_half::MigratingWriteHalf;
pub use mux::{LaneClass, MigratingStreamWriter, ResponseRouterHandle, StreamName, StreamReader};
pub use rtp::metrics::{MetricsFecCounters, MetricsObserver};
pub use rtp::{FecTuning, udp::Listener};

/// A 32-byte chacha20 key for datagram obfuscation.
///
/// When configured on a connector or server (via `with_obfuscation_key`),
/// every RTP datagram is prefixed with a 24-byte random nonce and
/// chacha20-encrypted with this key; both peers must use the same key.
/// Without a key (the default) datagrams travel in the clear.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObfuscationKey([u8; 32]);

impl ObfuscationKey {
    /// Wrap a 32-byte chacha20 key.
    pub const fn from_bytes(key: [u8; 32]) -> Self {
        Self(key)
    }

    /// The raw key bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Unwrap into the raw key bytes.
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}
pub use server::{RtpMuxServer, ServeError};
pub use session::SessionSpawner;
pub use stream::{ServerStream, SocketAddrPair};
