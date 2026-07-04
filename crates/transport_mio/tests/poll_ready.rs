//! Asserts `MioTransport::poll_ready(timeout)` returns after either an I/O
//! event or the timeout, whichever fires first.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::thread;
use std::time::{Duration, Instant};

use transport_core::{BindConfig, RecvBufConfig, SendBufConfig};
use transport_mio::UdpTransport;

fn loopback() -> BindConfig {
    let mut b = BindConfig::default();
    b.addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    b
}

#[test]
fn poll_ready_returns_before_timeout_when_datagram_arrives() {
    let mut receiver =
        UdpTransport::bind(loopback(), RecvBufConfig::default(), SendBufConfig::default())
            .expect("bind receiver");
    let recv_addr = receiver.local_addr().expect("local_addr");

    let sender = UdpSocket::bind("127.0.0.1:0").expect("sender bind");
    let payload = [0xABu8; 32];
    let handle = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        sender.send_to(&payload, recv_addr).expect("send_to");
    });

    let start = Instant::now();
    receiver
        .poll_ready(Some(Duration::from_secs(2)))
        .expect("poll_ready");
    let elapsed = start.elapsed();
    handle.join().expect("sender thread");
    assert!(
        elapsed < Duration::from_secs(1),
        "poll_ready blocked past event: {elapsed:?}"
    );

    let peer = receiver.try_recv().expect("try_recv").expect("frame ready");
    let frame = receiver.peek_frame().expect("peek_frame");
    assert_eq!(frame.bytes.len(), 32);
    assert_eq!(frame.peer, peer);
}

#[test]
fn poll_ready_returns_after_timeout_when_idle() {
    let mut receiver =
        UdpTransport::bind(loopback(), RecvBufConfig::default(), SendBufConfig::default())
            .expect("bind receiver");
    let start = Instant::now();
    receiver
        .poll_ready(Some(Duration::from_millis(50)))
        .expect("poll_ready");
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(40),
        "poll_ready returned before timeout: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "poll_ready blocked way past timeout: {elapsed:?}"
    );
    assert!(receiver.try_recv().expect("try_recv").is_none());
}
