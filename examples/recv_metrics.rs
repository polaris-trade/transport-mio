//! Run the mio UDP recv path with metrics live; scrape http://127.0.0.1:9464/metrics.
//!
//! Mio is runtime-free, so this stays a plain `fn main`. `observability::init`'s
//! Prometheus install falls back to its own dedicated thread + current-thread runtime
//! when no tokio reactor is already running, so no `#[tokio::main]` is needed here.

use std::{
    net::{SocketAddr, UdpSocket as StdUdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use transport_core::{
    AffinityConfig, AsPayload, BatchConfig, BindConfig, DatagramSource, FrameBatch, RecvBufConfig,
    RingConfig, SendBufConfig, TransportError,
};
use transport_mio::{MioTransport, UdpFrame, UdpTransport};

const PAYLOAD_LEN: usize = 64;
const BATCH_DEPTH: usize = 64;
/// Bounded reap target so the example terminates in CI smoke without a live exchange.
const REAP_TARGET: u64 = 50_000;

/// Flood `addr` with fixed-size datagrams until `stop` flips true. Self-paced against
/// the kernel receive queue on loopback, no explicit backoff needed.
fn spawn_flooder(addr: SocketAddr, stop: Arc<AtomicBool>) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let sender = StdUdpSocket::bind("127.0.0.1:0").expect("flooder bind");
        let payload = [0u8; PAYLOAD_LEN];
        while !stop.load(Ordering::Relaxed) {
            let _ = sender.send_to(&payload, addr);
        }
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _obs = observability::init(observability::ObsConfig {
        pipeline: observability::PipelineConfig {
            service_name: "transport-mio-example".into(),
            level: "info".into(),
            otlp: None,
            logging: Some(observability::LoggingConfig {
                sinks: vec![observability::LogSink {
                    kind: observability::LogSinkKind::Stdout,
                    format: observability::LogFormat::Pretty,
                    level: None,
                }],
            }),
            tracing: None,
        },
        metrics: Some(observability::MetricsConfig {
            exporter: observability::MetricsExporter::Prometheus {
                bind: "127.0.0.1:9464".parse()?,
            },
        }),
    })?;
    // gate through transport-core's observability-core re-export, the same instance
    // the recv path reads. see the gate-instance note in the observability-integration spec.
    transport_core::observability_core::set_metrics_enabled(true);
    transport_core::observability_core::refresh_thread_gate();

    let mut bind = BindConfig::default();
    bind.addr = "127.0.0.1:0".parse()?;
    let udp = UdpTransport::bind(
        bind,
        RecvBufConfig::default(),
        SendBufConfig::default(),
        RingConfig::default(),
        BatchConfig::default(),
        AffinityConfig::default(),
    )?;
    let addr = udp.local_addr()?;
    let mut receiver = MioTransport::Udp(udp);

    let stop = Arc::new(AtomicBool::new(false));
    let flooder = spawn_flooder(addr, Arc::clone(&stop));

    let mut batch: FrameBatch<UdpFrame> = FrameBatch::with_capacity(BATCH_DEPTH);
    let mut reaped = 0u64;
    while reaped < REAP_TARGET {
        match receiver.recv_burst(&mut batch, BATCH_DEPTH) {
            Ok(n) => {
                reaped += n as u64;
                for frame in batch.drain() {
                    // touch the payload so the loop isn't optimized away
                    let _ = frame.payload().len();
                }
            }
            // pool full: caller lets the kernel drop and retries, same as production.
            Err(TransportError::PoolExhausted { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }

    stop.store(true, Ordering::Relaxed);
    flooder.join().expect("flooder thread");

    tracing::info!(
        reaped,
        "recv_metrics done, scrape http://127.0.0.1:9464/metrics before exit if desired"
    );
    Ok(())
}
