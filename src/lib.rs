//! Mio-based transport backend. Wraps `mio::net::UdpSocket` and
//! `mio::net::TcpStream` behind the `transport_core` recv seam: `DatagramSource`
//! for UDP, `StreamSource` for TCP, `AsyncReady` as the runtime-free readiness
//! adapter (it blocks the calling thread on `mio::Poll`), `PoolAccess` for the
//! shared slab pool. Runtime-free: no async runtime; the caller drives recv on
//! its own thread.

use std::mem::MaybeUninit;

use transport_core::{
    AffinityConfig, AsyncReady, BatchConfig, BindConfig, DatagramSource, FrameBatch, PoolAccess,
    RecvBufConfig, RingConfig, SendBufConfig, StreamSource, TransportBind, TransportCore,
    TransportError,
};

pub mod pool;
pub mod tcp;
pub mod udp;

pub use pool::{SharedVecPool, VecPool, VecSlab};
pub use tcp::TcpTransport;
pub use udp::{UdpFrame, UdpTransport};

/// Public backend enum consumers depend on. The `Udp` variant is the
/// `DatagramSource`; the `Tcp` variant is the `StreamSource`. Calling the wrong
/// recv shape for a variant returns `TransportError::Unsupported`.
pub enum MioTransport {
    Udp(UdpTransport),
    Tcp(TcpTransport),
}

impl TransportCore for MioTransport {
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

impl DatagramSource for MioTransport {
    type Frame = UdpFrame;

    fn recv_burst(
        &mut self,
        out: &mut FrameBatch<UdpFrame>,
        max: usize,
    ) -> Result<usize, TransportError> {
        match self {
            MioTransport::Udp(u) => u.recv_burst(out, max),
            MioTransport::Tcp(_) => Err(TransportError::Unsupported {
                name: "mio-tcp",
                reason: "recv_burst is datagram-only; use recv_into",
            }),
        }
    }
}

impl StreamSource for MioTransport {
    fn recv_into(&mut self, dst: &mut [MaybeUninit<u8>]) -> Result<usize, TransportError> {
        match self {
            MioTransport::Tcp(t) => t.recv_into(dst),
            MioTransport::Udp(_) => Err(TransportError::Unsupported {
                name: "mio-udp",
                reason: "recv_into is stream-only; use recv_burst",
            }),
        }
    }
}

impl AsyncReady for MioTransport {
    async fn ready(&mut self) -> Result<(), TransportError> {
        match self {
            MioTransport::Udp(u) => u.ready(),
            MioTransport::Tcp(t) => t.ready(),
        }
    }
}

impl PoolAccess for MioTransport {
    type Pool = SharedVecPool;

    fn pool(&self) -> &SharedVecPool {
        match self {
            MioTransport::Udp(u) => u.pool(),
            MioTransport::Tcp(t) => t.pool(),
        }
    }
}

impl TransportBind for MioTransport {
    async fn bind_udp(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        ring: RingConfig,
        batch: BatchConfig,
        affinity: AffinityConfig,
    ) -> Result<Self, TransportError> {
        let u = UdpTransport::bind(bind, rx, tx, ring, batch, affinity)?;
        Ok(MioTransport::Udp(u))
    }

    async fn connect_tcp(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        ring: RingConfig,
        affinity: AffinityConfig,
    ) -> Result<Self, TransportError> {
        let t = TcpTransport::connect(bind, rx, tx, ring, affinity)?;
        Ok(MioTransport::Tcp(t))
    }
}

// `transport_core::UdpTransport` (not the inner `crate::udp::UdpTransport`
// struct re-exported above). Multicast join + unconnected send for the Udp
// variant; the Tcp variant rejects both. `send_to` does sync non-blocking
// work under the async signature, matching the rest of the mio backend.
impl transport_core::UdpTransport for MioTransport {
    async fn join_multicast(
        &mut self,
        group: std::net::IpAddr,
        interface: transport_core::MulticastInterface,
    ) -> Result<(), TransportError> {
        match self {
            MioTransport::Udp(u) => u.join_multicast(group, interface),
            MioTransport::Tcp(_) => Err(TransportError::Unsupported {
                name: "mio-tcp",
                reason: "multicast join unsupported on TCP",
            }),
        }
    }

    async fn send_to(
        &mut self,
        buf: &[u8],
        addr: std::net::SocketAddr,
    ) -> Result<(), TransportError> {
        match self {
            MioTransport::Udp(u) => u.send_to(buf, addr).map(|_| ()),
            MioTransport::Tcp(_) => Err(TransportError::Unsupported {
                name: "mio-tcp",
                reason: "send_to unsupported on TCP",
            }),
        }
    }
}
