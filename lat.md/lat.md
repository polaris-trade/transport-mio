# transport-mio

Mio-based backend for `transport_core`. Runtime-free: the caller drives recv on its own thread. Sync recv seam over `mio::net`: UDP as a `DatagramSource`, TCP as a `StreamSource`, with a blocking readiness adapter.

## Recv model

Recv is a sync, batch-first busy-poll. `mio` is a thin epoll/kqueue registration layer with no cached readiness, so recv hits the socket syscall directly; unlike an async runtime there is no reactor state to bypass.

`recv_burst` reaps ready datagrams into pool-owned owned frames and returns the count. A caller that prefers to wait rather than spin calls the blocking readiness adapter first, which parks the calling thread on the owned `mio::Poll` until the socket signals readable.

## Pool

Bounded slab pool with a fixed slot array and a `parking_lot` free list. Recv lands straight into a reused slab; the yielded frame owns that slab and returns it on `Drop`.

[[src/pool.rs#SharedVecPool]] is the `Arc`-wrapped handle backends and callers share; it wraps [[src/pool.rs#VecPool]], whose slabs sit behind `UnsafeCell` with the free list gating single-owner slot access. `acquire(len)` returns `None` when `len > slab_size` or the free list is drained, the natural backpressure signal.

[[src/pool.rs#VecSlab]] is the owned handle: an `Arc<VecPool>`, a slot index, and a length. [[src/pool.rs#VecSlab#buf_mut]] exposes the full-width slab for a recv to write into, then [[src/pool.rs#VecSlab#set_len]] records how many bytes landed, which bounds `AsRef<[u8]>`. `Drop` pushes the slot back onto the free list.

## Metrics

Recv throughput exports through `observability-core`'s gated hot path instead of a hand-rolled counter struct. `metrics` is a runtime dep of the lib crate; `observability` (the backend, tracing + Prometheus/OTLP) is dev-only, wired by [[examples/recv_metrics.rs]].

[[src/udp.rs#UdpTransport#recv_burst]] accumulates `bytes` across the reap loop, then behind one `observability_core::metrics_enabled()` check per burst increments `transport.recv.packets` and `transport.recv.bytes`, both labeled `backend => "mio-udp"`. Off-gate this is a single thread-local `Cell` read: no atomic, no allocation, so the `benches/recv.rs` steady-state zero-alloc assertion holds either way.

## UDP path

Wraps `mio::net::UdpSocket` with `socket2` for pre-bind option config, registered for `Interest::READABLE` on a per-socket `mio::Poll`.

[[src/udp.rs#UdpTransport]] builds a non-blocking `socket2::Socket`, applies reuse + `SO_RCVBUF`/`SO_SNDBUF` via [[src/udp.rs#apply_socket_opts]], binds, hands the fd to `mio::net::UdpSocket::from_std`, and registers it on a fresh `mio::Poll`. Bind is fully sync, so no runtime is needed to construct one.

[[src/udp.rs#UdpTransport#recv_burst]] acquires a slab per datagram, calls `recv_from` straight into it, records the length, and pushes an owned [[src/udp.rs#UdpFrame]]; it stops at `WouldBlock` (`Ok(0)` when nothing was ready) or returns `PoolExhausted` when no slab is free on acquire, regardless of whether the socket itself still has data queued. [[src/udp.rs#UdpTransport#ready]] blocks on the `mio::Poll` until readable, but probes the fd via [[src/udp.rs#probe_readable]] (`poll(2)`, Unix only) before parking: mio's owned epoll/kqueue instance is edge-triggered and won't re-signal for data left queued by a bounded or `PoolExhausted` `recv_burst` that stopped short of `WouldBlock`, so the probe reads kernel socket state directly and self-heals that gap. Callers should still drain `recv_burst`/`recv_into` to `Ok(0)` after each wake. [[src/tcp.rs#TcpTransport#ready]] shares the same probe via `udp::probe_readable`.

[[src/udp.rs#UdpFrame]] owns the pool slab it landed in, so it is `Send + 'static` and returns the slab on `Drop`. Raw UDP has no sequencing, so `sequence()` and `stream_id()` return zero; protocol crates (moldudp, custom framers) layer sequencing on top.

Sending is non-blocking with a short `thread::sleep(1ms)` retry on `WouldBlock`. UDP send rarely blocks in practice; a full retry-via-mio-writable path would add complexity for an edge case the loopback and production paths both avoid. `SO_BUSY_POLL`, `SO_RXQ_OVFL`, and timestamping stay out of this backend; that kernel-drop and busy-poll wire-up lives in the tokio and kernel-bypass backends.

### Recv benchmark

[[benches/recv.rs#bench_recv_burst]] reports `recv_burst` ns/msg and allocs/msg at batch depths 1/8/32/64 over a loopback UDP flood, mirroring the tokio backend bench. Allocs/msg asserts zero on the steady-state path per depth.

## TCP path

Wraps `mio::net::TcpStream` registered for `READABLE | WRITABLE`; `WRITABLE` drives connect completion and the send retry loop.

[[src/tcp.rs#TcpTransport]] opens the stream, applies `SO_RCVBUF`/`SO_SNDBUF` via [[src/tcp.rs#apply_tcp_socket_opts]], registers on a fresh `mio::Poll`, then blocks in `wait_connect` until the initial writable event lands.

[[src/tcp.rs#TcpTransport#recv_into]] lands one read directly into the caller's uninitialised buffer (a decode buffer's spare capacity) via `socket2` `recv`, since std `Read` needs an initialised buffer; that single copy is the only one in the stream path. A zero-byte read surfaces as `UnexpectedEof` so the caller sees graceful peer close. [[src/tcp.rs#TcpTransport#send]] loops on `WouldBlock` with a short poll wait until the whole buffer is written.

## MioTransport

Public enum unifying UDP and TCP under the `transport_core` recv seam. The caller owns the recv thread.

[[src/lib.rs#MioTransport]] is the enum consumers depend on. The `Udp` variant is the `DatagramSource` (`recv_burst`); the `Tcp` variant is the `StreamSource` (`recv_into`). It also implements `TransportCore` (`name` + async `send`), `AsyncReady` (the runtime-free readiness adapter blocks the calling thread on `mio::Poll`), `PoolAccess` (the shared `SharedVecPool`), and `TransportBind` (`bind_udp`/`connect_tcp`). Calling the wrong recv shape for a variant returns `TransportError::Unsupported`.

`impl transport_core::UdpTransport` adds multicast group join (`join_multicast`, dispatching IPv4 vs IPv6 to the inner socket's `join_multicast_v4`/`v6`, which `mio` takes by reference for v4) plus unconnected `send_to`. The `Tcp` variant rejects both with `TransportError::Unsupported`; the `send_to` body does sync non-blocking work under the async signature, like the rest of the backend.

`bind_udp` and `connect_tcp` carry the `TransportBind` async shape but their bodies do sync work, so a hand-rolled executor caller and a tokio caller both construct a transport without awaiting real I/O.
