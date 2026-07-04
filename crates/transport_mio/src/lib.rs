//! Mio-based Transport backend. Wraps `mio::net::UdpSocket` and
//! `mio::net::TcpStream` behind the `transport_core` trait shape.
//!
//! Runtime-free: consumers drive [`MioTransport::poll_ready`] to advance the
//! `mio::Poll` state, then call `poll_event` / `next_frame` to observe I/O.

use std::task::{Context, Poll};
use std::time::Duration;
use transport_core::{
    AsPayload, BatchConfig, BindConfig, RecvBufConfig, RingConfig, SendBufConfig, Transport,
    TransportBind, TransportError,
};

pub mod pool;
pub mod tcp;
pub mod udp;

pub use pool::{SharedVecPool, VecPool, VecSlab};
pub use tcp::{TcpFrame, TcpTransport};
pub use udp::{UdpFrame, UdpTransport};

pub enum MioTransport {
    Udp(UdpTransport),
    Tcp(TcpTransport),
}

pub enum MioFrame<'a> {
    Udp(UdpFrame<'a>),
    Tcp(TcpFrame<'a>),
}

pub enum MioEvent {
    Udp(std::net::SocketAddr),
    Tcp(usize),
}

impl MioTransport {
    /// Drive the underlying `mio::Poll` until an event fires or `timeout`
    /// elapses. Returns immediately if events are already queued.
    pub fn poll_ready(&mut self, timeout: Option<Duration>) -> Result<(), TransportError> {
        match self {
            MioTransport::Udp(u) => u.poll_ready(timeout),
            MioTransport::Tcp(t) => t.poll_ready(timeout),
        }
    }
}

impl AsPayload for MioFrame<'_> {
    fn payload(&self) -> &[u8] {
        match self {
            MioFrame::Udp(f) => f.payload(),
            MioFrame::Tcp(f) => f.payload(),
        }
    }

    fn sequence(&self) -> u64 {
        match self {
            MioFrame::Udp(f) => f.sequence(),
            MioFrame::Tcp(f) => f.sequence(),
        }
    }

    fn stream_id(&self) -> u8 {
        match self {
            MioFrame::Udp(f) => f.stream_id(),
            MioFrame::Tcp(f) => f.stream_id(),
        }
    }
}

impl Transport for MioTransport {
    type Frame<'a>
        = MioFrame<'a>
    where
        Self: 'a;
    type Event = MioEvent;

    fn poll_event(&mut self, _cx: &mut Context<'_>) -> Poll<Result<Self::Event, TransportError>> {
        match self {
            MioTransport::Udp(u) => match u.try_recv() {
                Ok(Some(peer)) => Poll::Ready(Ok(MioEvent::Udp(peer))),
                Ok(None) => Poll::Pending,
                Err(e) => Poll::Ready(Err(e)),
            },
            MioTransport::Tcp(t) => match t.try_recv() {
                Ok(Some(n)) => Poll::Ready(Ok(MioEvent::Tcp(n))),
                Ok(None) => Poll::Pending,
                Err(e) => Poll::Ready(Err(e)),
            },
        }
    }

    fn next_frame(&self) -> Option<Self::Frame<'_>> {
        match self {
            MioTransport::Udp(u) => u.peek_frame().map(MioFrame::Udp),
            MioTransport::Tcp(t) => t.peek_frame().map(MioFrame::Tcp),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            MioTransport::Udp(_) => "mio-udp",
            MioTransport::Tcp(_) => "mio-tcp",
        }
    }

    async fn send(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        match self {
            MioTransport::Udp(u) => u.send(buf).map(|_| ()),
            MioTransport::Tcp(t) => t.send(buf),
        }
    }
}

impl TransportBind for MioTransport {
    async fn bind_udp(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        _ring: RingConfig,
        _batch: BatchConfig,
    ) -> Result<Self, TransportError> {
        let u = UdpTransport::bind(bind, rx, tx)?;
        Ok(MioTransport::Udp(u))
    }

    async fn connect_tcp(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        _ring: RingConfig,
    ) -> Result<Self, TransportError> {
        let t = TcpTransport::connect(bind, rx, tx)?;
        Ok(MioTransport::Tcp(t))
    }
}
