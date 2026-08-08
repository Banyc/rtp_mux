use std::{future::Future, io, net::SocketAddr, pin::Pin};

use mux::{DualStreamAccepter, DualStreamOpener, GroupToken, MuxError};
use tokio::task::JoinSet;

use crate::{
    connector::{RtpMuxConnectorConfig, dial::connect_dual_lane},
    stream::SocketAddrPair,
};

/// A fully paired RTP-mux session whose stream opener and accepter are both
/// available to the caller.
#[derive(Debug)]
pub struct BidirectionalSession {
    opener: DualStreamOpener,
    accepter: DualStreamAccepter,
    addr: SocketAddrPair,
    driver: BidirectionalSessionDriver,
}

impl BidirectionalSession {
    pub(crate) fn new(
        opener: DualStreamOpener,
        accepter: DualStreamAccepter,
        addr: SocketAddrPair,
        supervisor: JoinSet<MuxError>,
    ) -> Self {
        Self {
            opener,
            accepter,
            addr,
            driver: BidirectionalSessionDriver::new(supervisor),
        }
    }

    pub fn addr(&self) -> SocketAddrPair {
        self.addr
    }

    pub fn into_parts(
        self,
    ) -> (
        DualStreamOpener,
        DualStreamAccepter,
        SocketAddrPair,
        BidirectionalSessionDriver,
    ) {
        (self.opener, self.accepter, self.addr, self.driver)
    }
}

pub struct BidirectionalSessionDriver(Pin<Box<dyn Future<Output = MuxError> + Send + 'static>>);

impl BidirectionalSessionDriver {
    fn new(mut supervisor: JoinSet<MuxError>) -> Self {
        Self(Box::pin(async move {
            match supervisor.join_next().await {
                Some(result) => result.unwrap(),
                None => MuxError::TaskStopped { task: "dual_lane" },
            }
        }))
    }
}

impl std::fmt::Debug for BidirectionalSessionDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BidirectionalSessionDriver")
            .finish_non_exhaustive()
    }
}

impl Future for BidirectionalSessionDriver {
    type Output = MuxError;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}

/// Establish one dual-lane session without assigning fixed stream roles.
pub async fn connect_bidirectional_session(
    addr: SocketAddr,
    config: RtpMuxConnectorConfig,
) -> io::Result<BidirectionalSession> {
    let birth = connect_dual_lane(
        addr,
        config.bind,
        config.bulk_addr,
        config.fec,
        GroupToken::generate(),
        None,
    )
    .await?;
    Ok(BidirectionalSession::new(
        birth.opener,
        birth.accepter,
        SocketAddrPair {
            local_addr: birth.local_addr,
            peer_addr: addr,
        },
        birth.supervisor,
    ))
}
