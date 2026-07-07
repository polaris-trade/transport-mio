# transport-mio

Mio-based backend for `transport_core`. Runtime-free: consumer drives `MioTransport::poll_ready(timeout)` to advance a `mio::Poll`, then reads events + frames.

## Pool

Naive Vec-backed buffer pool. Free list of pre-sized `Vec<u8>` slabs; `Drop` on the slab returns it to the free list.

[[src/pool.rs#SharedVecPool]] is the `Arc`-wrapped handle backends and callers share; it wraps [[src/pool.rs#VecPool]] which owns the `parking_lot::Mutex<Vec<Vec<u8>>>` free list plus configured `slab_size`. `acquire(len)` returns `None` when `len > slab_size` or when the free list is drained, giving a natural backpressure signal.

[[src/pool.rs#VecSlab]] is the owned handle. It carries `Arc<VecPool>`, a length, and its own `Vec<u8>`. `Drop` clears + resizes the buffer, pushes it back onto the free list, and decrements the `in_use` counter. `AsRef<[u8]>` returns the filled slice up to `len`.

## UDP path

Wraps `mio::net::UdpSocket` with `socket2` for pre-bind option config, registered for `Interest::READABLE` on a per-socket `mio::Poll`.

[[src/udp.rs#UdpTransport]] builds a non-blocking `socket2::Socket`, applies reuse + `SO_RCVBUF`/`SO_SNDBUF` via [[src/udp.rs#apply_socket_opts]], binds, hands the fd to `mio::net::UdpSocket::from_std`, and registers it on a fresh `mio::Poll`. `poll_ready(timeout)` drains readiness into an internal `readable` flag; `try_recv` calls `recv_from` and stores the datagram for `peek_frame`.

[[src/udp.rs#UdpFrame]] is the borrowed view. Raw UDP has no sequencing so `sequence()` and `stream_id()` return zero; protocol crates (moldudp, custom framers) layer sequencing on top.

Sending is non-blocking with a short `thread::sleep(1ms)` retry on `WouldBlock`. UDP send rarely blocks in practice; a full retry-via-mio-writable path would add complexity for an edge case that the loopback and production paths both avoid.

## TCP path

Wraps `mio::net::TcpStream` registered for `READABLE | WRITABLE`; `WRITABLE` is used only during connect completion and the send retry loop.

[[src/tcp.rs#TcpTransport]] opens the stream, applies `SO_RCVBUF`/`SO_SNDBUF` via [[src/tcp.rs#apply_tcp_socket_opts]], registers on a fresh `mio::Poll`, then blocks in `wait_connect` until the initial writable event lands. `try_recv` reads one chunk per call; a zero-byte read is surfaced as `UnexpectedEof` so the caller sees graceful peer close. `send` loops on `WouldBlock` with a short `poll_ready` wait until the whole buffer is written.

[[src/tcp.rs#TcpFrame]] is the borrowed view. TCP is stream-oriented so both `sequence()` and `stream_id()` return zero; SoupBinTCP or any wire framer above handles record boundaries.

## MioTransport

Public enum unifying UDP and TCP under one `Transport` impl. Consumer owns the event loop via `poll_ready(timeout)`.

[[src/lib.rs#MioTransport]] is the enum consumers depend on. `impl Transport` and `impl TransportBind` dispatch across the `Udp` and `Tcp` variants uniformly. `poll_event(cx)` ignores the waker and returns from the internal event queue populated by the last `poll_ready` call, matching the mio "consumer drives the loop" model.

`impl transport_core::UdpTransport` adds multicast group join (`join_multicast`, dispatching IPv4 vs IPv6 to the inner socket's `join_multicast_v4`/`v6`, which `mio` takes by reference for v4) plus unconnected `send_to`. The `Tcp` variant rejects both with `TransportError::Unsupported`; the `send_to` body does sync non-blocking work under the async signature, like the rest of the backend.

[[src/lib.rs#MioFrame]] and [[src/lib.rs#MioEvent]] are the matching enums for the borrowed frame and per-poll event surface. `MioEvent::Udp(SocketAddr)` carries the sender addr; `MioEvent::Tcp(usize)` carries the byte count.

`bind_udp` and `connect_tcp` are `async fn` for `TransportBind` shape compliance but bodies do sync work and complete on first poll, so tokio callers and hand-rolled executor callers both work.
