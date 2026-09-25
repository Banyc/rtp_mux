use std::{io, net::SocketAddr, pin::Pin, task::Context};

use mux::{LaneClass, SplicedReader};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::migrating_write_half::MigratingWriteHalf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketAddrPair {
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
}

/// An accepted RTP mux stream.
///
/// The accepter built by `run_dual_mux_accepter` seeds the response opener and
/// turns plain-stream pass-through off, so `MigratingCapableAccepter::accept`
/// yields the duplex shape and nothing else.
#[derive(Debug)]
pub enum ServerStream {
    MigratingDuplex {
        reader: SplicedReader,
        writer: MigratingWriteHalf,
        addr: SocketAddrPair,
        source_lane: LaneClass,
    },
}

impl ServerStream {
    pub fn addr(&self) -> SocketAddrPair {
        match self {
            Self::MigratingDuplex { addr, .. } => *addr,
        }
    }

    pub fn source_lane(&self) -> LaneClass {
        match self {
            Self::MigratingDuplex { source_lane, .. } => *source_lane,
        }
    }

    pub fn set_name(&self, name: &str) {
        match self {
            Self::MigratingDuplex { writer, .. } => writer.name_handle().set(name),
        }
    }
}

impl AsyncRead for ServerStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            Self::MigratingDuplex { reader, .. } => Pin::new(reader).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ServerStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        match &mut *self {
            Self::MigratingDuplex { writer, .. } => Pin::new(writer).poll_write(cx, buf),
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> std::task::Poll<io::Result<usize>> {
        match &mut *self {
            Self::MigratingDuplex { writer, .. } => Pin::new(writer).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::MigratingDuplex { writer, .. } => writer.is_write_vectored(),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            Self::MigratingDuplex { writer, .. } => Pin::new(writer).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            Self::MigratingDuplex { writer, .. } => Pin::new(writer).poll_shutdown(cx),
        }
    }
}
