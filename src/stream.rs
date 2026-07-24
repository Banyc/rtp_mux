use std::{io, net::SocketAddr, pin::Pin, task::Context};

use mux::{LaneClass, SplicedReader, StreamReader, StreamWriter};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketAddrPair {
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
}

#[derive(Debug)]
pub enum ServerStream {
    Plain {
        reader: StreamReader,
        writer: StreamWriter,
        addr: SocketAddrPair,
        source_lane: LaneClass,
    },
    Migrating {
        reader: SplicedReader,
        writer: StreamWriter,
        addr: SocketAddrPair,
        source_lane: LaneClass,
    },
}

impl ServerStream {
    pub fn addr(&self) -> SocketAddrPair {
        match self {
            Self::Plain { addr, .. } | Self::Migrating { addr, .. } => *addr,
        }
    }

    pub fn source_lane(&self) -> LaneClass {
        match self {
            Self::Plain { source_lane, .. } | Self::Migrating { source_lane, .. } => *source_lane,
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
            Self::Plain { reader, .. } => Pin::new(reader).poll_read(cx, buf),
            Self::Migrating { reader, .. } => Pin::new(reader).poll_read(cx, buf),
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
            Self::Plain { writer, .. } | Self::Migrating { writer, .. } => {
                Pin::new(writer).poll_write(cx, buf)
            }
        }
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> std::task::Poll<io::Result<usize>> {
        match &mut *self {
            Self::Plain { writer, .. } | Self::Migrating { writer, .. } => {
                Pin::new(writer).poll_write_vectored(cx, bufs)
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain { writer, .. } | Self::Migrating { writer, .. } => {
                writer.is_write_vectored()
            }
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain { writer, .. } | Self::Migrating { writer, .. } => {
                Pin::new(writer).poll_flush(cx)
            }
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain { writer, .. } | Self::Migrating { writer, .. } => {
                Pin::new(writer).poll_shutdown(cx)
            }
        }
    }
}
