use std::{io, net::SocketAddr, pin::Pin, task::Context};

use mux::{LaneClass, SplicedReader, StreamReader, StreamWriter};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::migrating_write_half::MigratingWriteHalf;

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
            Self::Plain { addr, .. }
            | Self::Migrating { addr, .. }
            | Self::MigratingDuplex { addr, .. } => *addr,
        }
    }

    pub fn source_lane(&self) -> LaneClass {
        match self {
            Self::Plain { source_lane, .. }
            | Self::Migrating { source_lane, .. }
            | Self::MigratingDuplex { source_lane, .. } => *source_lane,
        }
    }

    pub fn set_name(&self, name: &str) {
        if let Self::MigratingDuplex { writer, .. } = self {
            writer.name_handle().set(name);
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
            Self::Plain { writer, .. } | Self::Migrating { writer, .. } => {
                Pin::new(writer).poll_write(cx, buf)
            }
            Self::MigratingDuplex { writer, .. } => Pin::new(writer).poll_write(cx, buf),
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
            Self::MigratingDuplex { writer, .. } => Pin::new(writer).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain { writer, .. } | Self::Migrating { writer, .. } => {
                writer.is_write_vectored()
            }
            Self::MigratingDuplex { writer, .. } => writer.is_write_vectored(),
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
            Self::MigratingDuplex { writer, .. } => Pin::new(writer).poll_flush(cx),
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
            Self::MigratingDuplex { writer, .. } => Pin::new(writer).poll_shutdown(cx),
        }
    }
}
