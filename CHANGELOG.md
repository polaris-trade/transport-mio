# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).
## [0.2.1](https://github.com/polaris-trade/transport-mio/compare/transport_mio-v0.2.0...transport_mio-v0.2.1) - 2026-07-07

### Refactor

- *(lib)* Restructure into single-crate layout ([#8](https://github.com/polaris-trade/transport-mio/pull/8))


## [0.1.0](https://github.com/polaris-trade/transport-mio/releases/tag/transport_mio-v0.1.0) - 2026-07-04

### Features

- *(transport-mio)* Added VecPool, UDP + TCP transports, MioTransport enum with poll_ready(timeout) ([#3](https://github.com/polaris-trade/transport-mio/pull/3))


  Runtime-free backend for transport-core. Consumer drives mio::Poll via
MioTransport::poll_ready(timeout); poll_event and next_frame return from
the internal state populated by that call. UDP registers READABLE only
so the initial writable event does not spuriously wake poll_ready; TCP
registers READABLE|WRITABLE and drains the initial writable in
wait_connect so the same invariant holds post-connect. impl Transport
and impl TransportBind use async fn bodies that run sync work under the
covers, so tokio callers and hand-rolled executors both work.

