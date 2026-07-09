# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).
## [0.3.3](https://github.com/polaris-trade/transport-mio/compare/transport_mio-v0.3.2...transport_mio-v0.3.3) (2026-07-09)


### Bug fixes

* **mio:** probe socket before parking in ready() ([#17](https://github.com/polaris-trade/transport-mio/issues/17)) ([b6fe123](https://github.com/polaris-trade/transport-mio/commit/b6fe1237f06e63091933ffc2aafe915ce531a137))

## [0.3.2](https://github.com/polaris-trade/transport-mio/compare/transport_mio-v0.3.1...transport_mio-v0.3.2) (2026-07-09)


### Build

* **deps:** re-tag criterion bump ([df3ff4e](https://github.com/polaris-trade/transport-mio/commit/df3ff4edc001ce367f08a136f6be51446f88a43e))

## [0.3.1](https://github.com/polaris-trade/transport-mio/compare/transport_mio-v0.3.0...transport_mio-v0.3.1) (2026-07-09)


### Tests

* **bench:** add recv_burst throughput and allocation benchmark ([#13](https://github.com/polaris-trade/transport-mio/issues/13)) ([cf579a4](https://github.com/polaris-trade/transport-mio/commit/cf579a45dcf9ffd7ecf560b1ce79fd7ec7e4b009))

## [0.3.0](https://github.com/polaris-trade/transport-mio/compare/transport_mio-v0.2.1...transport_mio-v0.3.0) (2026-07-08)


### ⚠ BREAKING CHANGES

* **recv:** migrate mio backend to the owned-frame recv seam ([#10](https://github.com/polaris-trade/transport-mio/issues/10))

### Features

* **recv:** migrate mio backend to the owned-frame recv seam ([#10](https://github.com/polaris-trade/transport-mio/issues/10)) ([2250369](https://github.com/polaris-trade/transport-mio/commit/22503698e01872475b8846cc1b5ac0fb72b1d064))

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
