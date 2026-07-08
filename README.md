# transport-mio

Runtime-free backend for the Polaris networking stack. Implements the
`transport_core` recv seam over `mio::net`: UDP as a `DatagramSource`, TCP as a
`StreamSource`, with a blocking readiness adapter over a per-socket `mio::Poll`
and a bounded slab pool (`SharedVecPool`) so recv lands zero-copy into reused
buffers. No async runtime; the caller drives recv on its own thread.

## What it provides

- `MioTransport`: the backend enum consumers hold. The `Udp` variant is the
  `DatagramSource` (`recv_burst`); the `Tcp` variant is the `StreamSource`
  (`recv_into`). It also implements `TransportCore`, `AsyncReady`, `PoolAccess`,
  `TransportBind`, and the multicast `UdpTransport` extension.
- `SharedVecPool`: bounded slab pool with `Drop`-based reclaim and `acquire`
  backpressure. Recv writes straight into a pooled slab; the yielded `UdpFrame`
  owns that slab (`Send + 'static`) and returns it on `Drop`.
- `ReceiverStats`: atomic packet and byte counters shared with observability.

## Recv model

Recv is a sync busy-poll. `mio` is a thin epoll/kqueue registration layer with
no cached readiness, so `recv_burst`/`recv_into` hit the socket syscall directly
(no reactor state to bypass). A caller that prefers to wait rather than spin
calls `AsyncReady::ready` first, which parks the calling thread on the owned
`mio::Poll` until the socket is readable. This is the runtime-free counterpart
to the tokio backend: identical recv seam, no executor.

## Usage

### UDP datagrams

Bind (fully sync, no runtime), then reap owned frames in a burst. Each frame
owns its pool slab and is `Send + 'static`, so it can be handed to another
thread:

```rust
use transport_core::{
    AffinityConfig, AsPayload, BatchConfig, BindConfig, DatagramSource, FrameBatch,
    RecvBufConfig, RingConfig, SendBufConfig,
};
use transport_mio::{MioTransport, UdpFrame, UdpTransport};

let mut bind = BindConfig::default();
bind.addr = "0.0.0.0:4242".parse().unwrap();
let mut transport = MioTransport::Udp(UdpTransport::bind(
    bind,
    RecvBufConfig::default(),
    SendBufConfig::default(),
    RingConfig::default(),
    BatchConfig::default(),
    AffinityConfig::default(),
)?);

let mut batch: FrameBatch<UdpFrame> = FrameBatch::with_capacity(64);
match transport.recv_burst(&mut batch, 64)? {
    0 => { /* nothing ready, spin again or block on ready() */ }
    _n => {
        for frame in batch.drain() {
            let _payload: &[u8] = frame.payload();
        }
    }
}
```

### TCP stream

`recv_into` lands one read directly into caller memory (a decode buffer's spare
capacity). That single copy is the only one in the stream path:

```rust
use std::mem::MaybeUninit;
use transport_core::StreamSource;

let mut landing = [MaybeUninit::<u8>::uninit(); 64 * 1024];
let n = transport.recv_into(&mut landing)?;
// exactly the first `n` bytes of `landing` are initialised
```

## Dependency

```toml
[dependencies]
transport_mio = { git = "https://github.com/polaris-trade/transport-mio", tag = "transport_mio-vX.Y.Z" }
```

Pulls `transport_core` by git tag. Distributed by git tag (`publish = false`).

## Dev commands

```bash
cargo nextest run
cargo clippy --all-targets -- -D warnings
lat check
```

MSRV `1.96.1` (pinned in `rust-toolchain.toml`).

## Docs

Architecture and design rationale live in [`lat.md/lat.md`](lat.md/lat.md).

## License

Dual-licensed under either [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
