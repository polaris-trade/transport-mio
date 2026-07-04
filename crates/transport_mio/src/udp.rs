//! UDP path built on `mio::net::UdpSocket`.
//!
//! Owns its own `mio::Poll`. `poll_ready(timeout)` drains readiness into an
//! internal flag; `try_recv` calls `recv_from` and stores the datagram for
//! `peek_frame`. Non-blocking send retries on `WouldBlock` after a short
//! `poll_ready` wait for `Interest::WRITABLE`.

use mio::event::Source;
use mio::{Events, Interest, Poll, Token};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::io;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;
use transport_core::{AsPayload, BindConfig, RecvBufConfig, SendBufConfig, TransportError};

const MAX_UDP_DATAGRAM: usize = 65_535;
const UDP_TOKEN: Token = Token(0);
const EVENTS_CAP: usize = 16;

pub struct UdpTransport {
    sock: mio::net::UdpSocket,
    poll: Poll,
    events: Events,
    buf: Vec<u8>,
    last_len: usize,
    last_peer: Option<SocketAddr>,
    has_frame: bool,
    readable: bool,
    local: SocketAddr,
}

impl UdpTransport {
    pub fn bind(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
    ) -> Result<Self, TransportError> {
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
            buf: vec![0u8; MAX_UDP_DATAGRAM],
            last_len: 0,
            last_peer: None,
            has_frame: false,
            readable: false,
            local,
        })
    }

    pub fn poll_ready(&mut self, timeout: Option<Duration>) -> Result<(), TransportError> {
        self.poll
            .poll(&mut self.events, timeout)
            .map_err(TransportError::Io)?;
        for ev in self.events.iter() {
            if ev.token() == UDP_TOKEN && ev.is_readable() {
                self.readable = true;
            }
        }
        Ok(())
    }

    pub fn try_recv(&mut self) -> Result<Option<SocketAddr>, TransportError> {
        if !self.readable {
            return Ok(None);
        }
        match self.sock.recv_from(&mut self.buf) {
            Ok((n, peer)) => {
                self.last_len = n;
                self.last_peer = Some(peer);
                self.has_frame = true;
                Ok(Some(peer))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.readable = false;
                Ok(None)
            }
            Err(e) => Err(TransportError::Io(e)),
        }
    }

    pub fn peek_frame(&self) -> Option<UdpFrame<'_>> {
        if !self.has_frame {
            return None;
        }
        Some(UdpFrame {
            bytes: &self.buf[..self.last_len],
            peer: self.last_peer?,
        })
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

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
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
    // busy-poll, RXQ_OVFL, timestamping intentionally omitted from mio backend
    // (see transport-tokio for kernel-drop and timestamping wire-up).
    Ok(())
}

pub struct UdpFrame<'a> {
    pub bytes: &'a [u8],
    pub peer: SocketAddr,
}

impl AsPayload for UdpFrame<'_> {
    fn payload(&self) -> &[u8] {
        self.bytes
    }

    fn sequence(&self) -> u64 {
        0
    }

    fn stream_id(&self) -> u8 {
        0
    }
}
