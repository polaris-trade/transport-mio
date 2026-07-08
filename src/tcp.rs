//! TCP path built on `mio::net::TcpStream`.
//!
//! Runtime-free. `connect` opens the stream, applies buffer sizes via
//! `socket2::SockRef`, registers on a fresh `mio::Poll`, then blocks in
//! `wait_connect` until the initial writable event lands. `recv_into` lands one
//! read directly into the caller's uninitialised buffer via `socket2` (std
//! `Read` needs an initialised buffer), the single copy in the stream path.

use std::{
    io::{self, Write},
    mem::MaybeUninit,
    net::SocketAddr,
    time::{Duration, Instant},
};

use mio::{Events, Interest, Poll, Token, event::Source};
use socket2::SockRef;
use transport_core::{
    AffinityConfig, BindConfig, RecvBufConfig, RingConfig, SendBufConfig, TransportError,
};

use crate::{pool::SharedVecPool, udp::warn_affinity};

const TCP_TOKEN: Token = Token(1);
const EVENTS_CAP: usize = 16;

pub struct TcpTransport {
    stream: mio::net::TcpStream,
    poll: Poll,
    events: Events,
    // Present for uniform `PoolAccess`; the stream path lands into caller-owned
    // buffers via `recv_into`, so it never draws slabs. Size it small via
    // `RingConfig` for a stream-only transport.
    pool: SharedVecPool,
    peer: SocketAddr,
}

impl TcpTransport {
    /// Connect to `BindConfig::addr` and block until the handshake completes.
    /// Fully sync (no runtime); `ring` sizes the (unused-on-recv) pool and
    /// `affinity` is warned since mio owns no thread to pin.
    pub fn connect(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
        ring: RingConfig,
        affinity: AffinityConfig,
    ) -> Result<Self, TransportError> {
        warn_affinity(&affinity, "mio-tcp");
        let mut stream =
            mio::net::TcpStream::connect(bind.addr).map_err(|e| TransportError::BindFailed {
                addr: bind.addr.to_string(),
                reason: e.to_string(),
            })?;
        apply_tcp_socket_opts(&stream, &rx, &tx)?;
        let poll = Poll::new().map_err(TransportError::Io)?;
        stream
            .register(
                poll.registry(),
                TCP_TOKEN,
                Interest::READABLE | Interest::WRITABLE,
            )
            .map_err(TransportError::Io)?;
        let mut inst = Self {
            stream,
            poll,
            events: Events::with_capacity(EVENTS_CAP),
            pool: SharedVecPool::new(ring.slab_count, ring.slab_size),
            peer: bind.addr,
        };
        // mio TCP connect returns immediately; real readiness arrives WRITABLE.
        inst.wait_connect()?;
        if let Ok(p) = inst.stream.peer_addr() {
            inst.peer = p;
        }
        Ok(inst)
    }

    fn wait_connect(&mut self) -> Result<(), TransportError> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            self.poll
                .poll(&mut self.events, Some(Duration::from_millis(100)))
                .map_err(TransportError::Io)?;
            let writable = self
                .events
                .iter()
                .any(|ev| ev.token() == TCP_TOKEN && ev.is_writable());
            if writable {
                if let Some(e) = self.stream.take_error().map_err(TransportError::Io)? {
                    return Err(TransportError::Io(e));
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(TransportError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "tcp connect timed out",
                )));
            }
        }
    }

    /// Land one read into `dst` (typically a decode buffer's spare capacity).
    /// Returns the byte count written; the caller marks exactly that many bytes
    /// initialised. `Ok(0)` means nothing was ready. A clean peer close surfaces
    /// as `UnexpectedEof` so callers can react rather than spin on empty reads.
    pub fn recv_into(&mut self, dst: &mut [MaybeUninit<u8>]) -> Result<usize, TransportError> {
        let sock = SockRef::from(&self.stream);
        match sock.recv(dst) {
            Ok(0) => Err(TransportError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "peer closed",
            ))),
            Ok(n) => Ok(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(e) => Err(TransportError::Io(e)),
        }
    }

    /// Block the calling thread on the owned `mio::Poll` until the stream is
    /// readable. NOTE: parks the OS thread; drive it on a dedicated recv thread,
    /// not inside an async executor worker.
    pub fn ready(&mut self) -> Result<(), TransportError> {
        loop {
            self.poll
                .poll(&mut self.events, None)
                .map_err(TransportError::Io)?;
            if self
                .events
                .iter()
                .any(|ev| ev.token() == TCP_TOKEN && ev.is_readable())
            {
                return Ok(());
            }
        }
    }

    pub fn pool(&self) -> &SharedVecPool {
        &self.pool
    }

    pub fn send(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        let mut off = 0;
        while off < buf.len() {
            match self.stream.write(&buf[off..]) {
                Ok(0) => {
                    return Err(TransportError::Io(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "write returned zero",
                    )));
                }
                Ok(n) => off += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    self.poll
                        .poll(&mut self.events, Some(Duration::from_millis(50)))
                        .map_err(TransportError::Io)?;
                }
                Err(e) => return Err(TransportError::Io(e)),
            }
        }
        Ok(())
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer
    }
}

fn apply_tcp_socket_opts(
    stream: &mio::net::TcpStream,
    rx: &RecvBufConfig,
    tx: &SendBufConfig,
) -> Result<(), TransportError> {
    let sock = SockRef::from(stream);
    if let Some(req) = rx.so_rcvbuf {
        let req_usize = req as usize;
        sock.set_recv_buffer_size(req_usize)
            .map_err(TransportError::Io)?;
        let effective = sock.recv_buffer_size().map_err(TransportError::Io)?;
        if effective < req_usize {
            tracing::warn!(
                requested = req,
                effective,
                "kernel granted less SO_RCVBUF than requested on TCP"
            );
        }
    }
    if let Some(req) = tx.so_sndbuf {
        let req_usize = req as usize;
        sock.set_send_buffer_size(req_usize)
            .map_err(TransportError::Io)?;
        let effective = sock.send_buffer_size().map_err(TransportError::Io)?;
        if effective < req_usize {
            tracing::warn!(
                requested = req,
                effective,
                "kernel granted less SO_SNDBUF than requested on TCP"
            );
        }
    }
    Ok(())
}
