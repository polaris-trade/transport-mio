//! TCP path built on `mio::net::TcpStream`.
//!
//! Owns its own `mio::Poll`. `connect` opens the stream, applies buffer
//! sizes via `socket2::SockRef`, then registers for `READABLE | WRITABLE`.
//! `poll_ready(timeout)` drains readiness; `try_recv` reads one chunk per
//! call. Send loops on `WouldBlock` with a short `poll_ready` wait until
//! the whole buffer is written.

use mio::event::Source;
use mio::{Events, Interest, Poll, Token};
use socket2::SockRef;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::time::Duration;
use transport_core::{AsPayload, BindConfig, RecvBufConfig, SendBufConfig, TransportError};

const MAX_TCP_CHUNK: usize = 64 * 1024;
const TCP_TOKEN: Token = Token(1);
const EVENTS_CAP: usize = 16;

pub struct TcpTransport {
    stream: mio::net::TcpStream,
    poll: Poll,
    events: Events,
    buf: Vec<u8>,
    last_len: usize,
    has_frame: bool,
    readable: bool,
    writable: bool,
    peer: SocketAddr,
}

impl TcpTransport {
    pub fn connect(
        bind: BindConfig,
        rx: RecvBufConfig,
        tx: SendBufConfig,
    ) -> Result<Self, TransportError> {
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
            buf: vec![0u8; MAX_TCP_CHUNK],
            last_len: 0,
            has_frame: false,
            readable: false,
            writable: false,
            peer: bind.addr,
        };
        // Wait for connect to complete: mio TCP connect returns immediately;
        // real readiness arrives via WRITABLE.
        inst.wait_connect()?;
        if let Ok(p) = inst.stream.peer_addr() {
            inst.peer = p;
        }
        Ok(inst)
    }

    fn wait_connect(&mut self) -> Result<(), TransportError> {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            self.poll_ready(Some(Duration::from_millis(100)))?;
            if self.writable {
                if let Some(e) = self.stream.take_error().map_err(TransportError::Io)? {
                    return Err(TransportError::Io(e));
                }
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(TransportError::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "tcp connect timed out",
                )));
            }
        }
    }

    pub fn poll_ready(&mut self, timeout: Option<Duration>) -> Result<(), TransportError> {
        self.poll
            .poll(&mut self.events, timeout)
            .map_err(TransportError::Io)?;
        for ev in self.events.iter() {
            if ev.token() == TCP_TOKEN {
                if ev.is_readable() {
                    self.readable = true;
                }
                if ev.is_writable() {
                    self.writable = true;
                }
            }
        }
        Ok(())
    }

    pub fn try_recv(&mut self) -> Result<Option<usize>, TransportError> {
        if !self.readable {
            return Ok(None);
        }
        match self.stream.read(&mut self.buf) {
            Ok(0) => Err(TransportError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "peer closed",
            ))),
            Ok(n) => {
                self.last_len = n;
                self.has_frame = true;
                Ok(Some(n))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.readable = false;
                Ok(None)
            }
            Err(e) => Err(TransportError::Io(e)),
        }
    }

    pub fn peek_frame(&self) -> Option<TcpFrame<'_>> {
        if self.has_frame {
            Some(TcpFrame {
                bytes: &self.buf[..self.last_len],
            })
        } else {
            None
        }
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
                    self.writable = false;
                    self.poll_ready(Some(Duration::from_millis(50)))?;
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

pub struct TcpFrame<'a> {
    pub bytes: &'a [u8],
}

impl AsPayload for TcpFrame<'_> {
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
