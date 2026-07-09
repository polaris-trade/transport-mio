//! UDP path built on `mio::net::UdpSocket`.
//!
//! Runtime-free. `recv_burst` is the sync, batch-first recv: it reaps ready
//! datagrams into pool-owned [`UdpFrame`]s and returns the count, `Ok(0)` when
//! the socket is drained, `PoolExhausted` under backpressure. `mio` is a thin
//! epoll/kqueue registration layer with no cached readiness, so recv hits
//! `recv_from` directly; `ready` blocks the calling thread on the owned
//! `mio::Poll` for a caller that prefers to wait rather than spin.

use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    thread,
    time::Duration,
};

use mio::{Events, Interest, Poll, Token, event::Source};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use transport_core::{
    AffinityConfig, AsPayload, BatchConfig, BindConfig, BufferPool, FrameBatch, MulticastInterface,
    RecvBufConfig, RingConfig, SendBufConfig, TransportError,
};

use crate::{
    pool::{SharedVecPool, VecSlab},
    stats::ReceiverStats,
};

const UDP_TOKEN: Token = Token(0);
const EVENTS_CAP: usize = 16;

pub struct UdpTransport {
    sock: mio::net::UdpSocket,
    poll: Poll,
    events: Events,
    pool: SharedVecPool,
    stats: Arc<ReceiverStats>,
    batch_recv_size: u32,
    last_peer: Option<SocketAddr>,
    local: SocketAddr,
}

impl UdpTransport {
    /// Bind a non-blocking UDP socket, register it on a fresh `mio::Poll`, and
    /// size the slab pool `recv_burst` lands into from `ring`. Fully sync (no
    /// runtime); `affinity` is honored where the backend can, warned otherwise.
    pub fn bind(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        ring: RingConfig,
        batch: BatchConfig,
        affinity: AffinityConfig,
    ) -> Result<Self, TransportError> {
        warn_affinity(&affinity, "mio-udp");
        let domain = Domain::for_address(bind.addr);
        let sock = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP)).map_err(|e| {
            TransportError::BindFailed {
                addr: bind.addr.to_string(),
                reason: e.to_string(),
            }
        })?;
        apply_socket_opts(&sock, &bind, &rx, &tx)?;
        sock.set_nonblocking(true).map_err(TransportError::Io)?;
        sock.bind(&SockAddr::from(bind.addr))
            .map_err(|e| TransportError::BindFailed {
                addr: bind.addr.to_string(),
                reason: e.to_string(),
            })?;
        let std_sock: std::net::UdpSocket = sock.into();
        let local = std_sock.local_addr().map_err(TransportError::Io)?;
        let mut mio_sock = mio::net::UdpSocket::from_std(std_sock);
        let poll = Poll::new().map_err(TransportError::Io)?;
        mio_sock
            .register(poll.registry(), UDP_TOKEN, Interest::READABLE)
            .map_err(TransportError::Io)?;
        Ok(Self {
            sock: mio_sock,
            poll,
            events: Events::with_capacity(EVENTS_CAP),
            pool: SharedVecPool::new(ring.slab_count, ring.slab_size),
            stats: Arc::new(ReceiverStats::default()),
            batch_recv_size: batch.recv_size,
            last_peer: None,
            local,
        })
    }

    /// Reap up to `max` ready datagrams into `out`, each frame owning a pool
    /// slab it wrote into (zero further copy). Returns the count. `Ok(0)` means
    /// the socket had nothing ready; `PoolExhausted` means no landing slab was
    /// free on acquire, regardless of whether the socket itself still has data
    /// queued (backpressure signal, let the kernel drop).
    pub fn recv_burst(
        &mut self,
        out: &mut FrameBatch<UdpFrame>,
        max: usize,
    ) -> Result<usize, TransportError> {
        let cap = max.min(out.spare());
        let mut n = 0;
        while n < cap {
            let mut slab = match self.pool.acquire(self.pool.slab_size()) {
                Some(slab) => slab,
                None => {
                    if n == 0 {
                        return Err(TransportError::PoolExhausted {
                            in_use: self.pool.in_use(),
                            capacity: self.pool.capacity(),
                        });
                    }
                    break;
                }
            };
            // mio does not cache readiness, so the recv is a direct nonblocking
            // syscall. The slab is zero-initialised, so a plain `&mut [u8]`
            // lands the datagram with no uninitialised-memory exposure.
            match self.sock.recv_from(slab.buf_mut()) {
                Ok((len, peer)) => {
                    slab.set_len(len);
                    self.stats.record_packet(len);
                    self.last_peer = Some(peer);
                    out.push(UdpFrame { slab, peer });
                    n += 1;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // Nothing more ready; `slab` drops here, back to the pool.
                    break;
                }
                Err(e) => return Err(TransportError::Io(e)),
            }
        }
        Ok(n)
    }

    /// Block the calling thread on the owned `mio::Poll` until the socket is
    /// readable, for callers that drive readiness instead of busy-polling
    /// `recv_burst`. Probes the fd via `poll(2)` first (see `probe_readable`):
    /// mio's epoll/kqueue readiness is edge-triggered and will not re-fire if a
    /// prior wake was only partially drained (bounded `max`, `PoolExhausted`),
    /// so the probe catches data still queued from an earlier edge. Callers
    /// should still drain `recv_burst` to `Ok(0)` after each wake rather than
    /// relying solely on this self-heal.
    /// NOTE: parks the OS thread; run it on a dedicated recv thread, not inside
    /// an async executor worker.
    pub fn ready(&mut self) -> Result<(), TransportError> {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            if probe_readable(self.sock.as_raw_fd()).map_err(TransportError::Io)? {
                return Ok(());
            }
        }
        loop {
            self.poll
                .poll(&mut self.events, None)
                .map_err(TransportError::Io)?;
            if self
                .events
                .iter()
                .any(|ev| ev.token() == UDP_TOKEN && ev.is_readable())
            {
                return Ok(());
            }
        }
    }

    pub fn pool(&self) -> &SharedVecPool {
        &self.pool
    }

    pub fn send(&mut self, buf: &[u8]) -> Result<usize, TransportError> {
        loop {
            match self.sock.send(buf) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(e) => return Err(TransportError::Io(e)),
            }
        }
    }

    pub fn send_to(&mut self, buf: &[u8], addr: SocketAddr) -> Result<usize, TransportError> {
        loop {
            match self.sock.send_to(buf, addr) {
                Ok(n) => return Ok(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(e) => return Err(TransportError::Io(e)),
            }
        }
    }

    /// Join an IPv4 or IPv6 multicast group. `interface.v4` picks the local
    /// v4 interface addr (defaults to `INADDR_ANY`); `interface.v6_scope_id`
    /// picks the v6 interface index (defaults to 0 = any). `mio` takes the v4
    /// addrs by reference.
    pub fn join_multicast(
        &self,
        group: IpAddr,
        interface: MulticastInterface,
    ) -> Result<(), TransportError> {
        match group {
            IpAddr::V4(m) => {
                let iface = interface.v4.unwrap_or(Ipv4Addr::UNSPECIFIED);
                self.sock
                    .join_multicast_v4(&m, &iface)
                    .map_err(TransportError::Io)
            }
            IpAddr::V6(m) => {
                let scope = interface.v6_scope_id.unwrap_or(0);
                self.sock
                    .join_multicast_v6(&m, scope)
                    .map_err(TransportError::Io)
            }
        }
    }

    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        Ok(self.local)
    }

    pub fn last_peer(&self) -> Option<SocketAddr> {
        self.last_peer
    }

    pub fn stats(&self) -> &Arc<ReceiverStats> {
        &self.stats
    }

    pub fn batch_recv_size(&self) -> u32 {
        self.batch_recv_size
    }
}

/// Non-blocking `poll(2)` probe for pending data, bypassing mio's owned
/// epoll/kqueue instance. That instance is edge-triggered internally: once it
/// delivers a readable edge it won't re-signal for data arriving while the
/// socket stays non-empty, so a partial `recv_burst` drain could otherwise
/// stall a later `ready()` call. `poll(2)` reads kernel socket state directly,
/// independent of mio's edge tracking.
#[cfg(unix)]
pub(crate) fn probe_readable(fd: std::os::fd::RawFd) -> io::Result<bool> {
    let mut fds = [libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    }];
    // SAFETY: fds points at one stack-local pollfd; len arg matches array size.
    let ret = unsafe { libc::poll(fds.as_mut_ptr(), 1, 0) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fds[0].revents & libc::POLLIN != 0)
}

/// Warn when CPU-affinity knobs are set on the mio backend, which owns no
/// thread to pin. Runtime-free: the caller drives recv on its own thread and
/// pins that thread itself, so `io_cpu`/`sqpoll_cpu` are informational here.
pub(crate) fn warn_affinity(affinity: &AffinityConfig, backend: &'static str) {
    if affinity.io_cpu.is_some() || affinity.sqpoll_cpu.is_some() {
        tracing::warn!(
            backend,
            "CPU affinity requested but the mio backend owns no thread to pin; \
             the caller pins its own recv loop"
        );
    }
}

fn apply_socket_opts(
    sock: &Socket,
    bind: &BindConfig,
    rx: &RecvBufConfig,
    tx: &SendBufConfig,
) -> Result<(), TransportError> {
    if bind.reuse_addr {
        sock.set_reuse_address(true).map_err(TransportError::Io)?;
    }
    #[cfg(unix)]
    if bind.reuse_port {
        sock.set_reuse_port(true).map_err(TransportError::Io)?;
    }
    if let Some(req) = rx.so_rcvbuf {
        sock.set_recv_buffer_size(req as usize)
            .map_err(TransportError::Io)?;
        let effective = sock.recv_buffer_size().map_err(TransportError::Io)?;
        if effective < req as usize {
            tracing::warn!(
                requested = req,
                effective,
                "kernel granted less SO_RCVBUF than requested on UDP"
            );
        }
    }
    if let Some(req) = tx.so_sndbuf {
        sock.set_send_buffer_size(req as usize)
            .map_err(TransportError::Io)?;
        let effective = sock.send_buffer_size().map_err(TransportError::Io)?;
        if effective < req as usize {
            tracing::warn!(
                requested = req,
                effective,
                "kernel granted less SO_SNDBUF than requested on UDP"
            );
        }
    }
    // busy-poll, RXQ_OVFL, and timestamping stay out of the mio backend; the
    // kernel-drop and busy-poll wire-up lives in the tokio and kernel-bypass
    // backends. see transport_tokio::udp for the Linux setsockopt path.
    Ok(())
}

/// Owned UDP datagram: carries the pool slab it landed in, so it is
/// `Send + 'static` and returns the slab to the pool on `Drop`. Raw UDP has no
/// sequencing, so `sequence`/`stream_id` are zero; protocol crates layer those.
pub struct UdpFrame {
    slab: VecSlab,
    peer: SocketAddr,
}

impl UdpFrame {
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }
}

impl AsPayload for UdpFrame {
    fn payload(&self) -> &[u8] {
        self.slab.as_ref()
    }

    fn sequence(&self) -> u64 {
        0
    }

    fn stream_id(&self) -> u8 {
        0
    }
}
