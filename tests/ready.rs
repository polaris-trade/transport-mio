//! Exercises `UdpTransport::ready` over loopback: the `poll(2)` probe must
//! catch data left queued by a bounded `recv_burst` drain (partial-drain,
//! edge-triggered lost-wakeup risk), and `ready` must still block on a truly
//! idle socket until a sender fires a fresh datagram.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    sync::mpsc,
    thread,
    time::Duration,
};

use transport_core::{
    AffinityConfig, BatchConfig, BindConfig, FrameBatch, RecvBufConfig, RingConfig, SendBufConfig,
};
use transport_mio::{UdpFrame, UdpTransport};

const WATCHDOG: Duration = Duration::from_secs(2);

fn loopback() -> BindConfig {
    let mut b = BindConfig::default();
    b.addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    b
}

fn bind_receiver() -> UdpTransport {
    UdpTransport::bind(
        loopback(),
        RecvBufConfig::default(),
        SendBufConfig::default(),
        RingConfig::default(),
        BatchConfig::default(),
        AffinityConfig::default(),
    )
    .expect("bind receiver")
}

#[test]
fn ready_returns_immediately_on_partial_drain_probe() {
    let mut receiver = bind_receiver();
    let addr = receiver.local_addr().expect("local addr");

    let sender = UdpSocket::bind("127.0.0.1:0").expect("sender bind");
    sender.send_to(&[1u8; 8], addr).expect("send_to 1");
    sender.send_to(&[2u8; 8], addr).expect("send_to 2");
    thread::sleep(Duration::from_millis(20));

    // First wake: nothing consumed this edge yet, so it fires as normal.
    receiver
        .ready()
        .expect("first ready wakes on the queued edge");

    // Bounded burst leaves one datagram queued without hitting WouldBlock,
    // reproducing the edge-triggered partial-drain gap `ready` must probe for.
    let mut batch: FrameBatch<UdpFrame> = FrameBatch::with_capacity(1);
    let n = receiver.recv_burst(&mut batch, 1).expect("recv_burst");
    assert_eq!(n, 1, "bounded burst drains only one datagram");

    // Run the second `ready` off-thread behind a watchdog: a lost wakeup
    // hangs the channel recv, turning a regression into a test failure
    // instead of a stuck CI job.
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let res = receiver.ready();
        let _ = tx.send(res);
    });
    let res = rx
        .recv_timeout(WATCHDOG)
        .expect("ready() did not return: probe-before-park regression (lost wakeup)");
    res.expect("ready() reported the still-queued datagram");
}

#[test]
fn ready_blocks_until_sender_thread_fires_a_datagram() {
    let mut receiver = bind_receiver();
    let addr = receiver.local_addr().expect("local addr");

    // Drain to Ok(0) up front so the socket starts genuinely idle.
    let mut batch: FrameBatch<UdpFrame> = FrameBatch::with_capacity(4);
    let n = receiver.recv_burst(&mut batch, 4).expect("recv_burst");
    assert_eq!(n, 0, "socket starts empty");

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        let sender = UdpSocket::bind("127.0.0.1:0").expect("sender bind");
        sender.send_to(&[9u8; 4], addr).expect("send_to");
    });

    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let res = receiver.ready();
        let _ = tx.send(res);
    });
    let res = rx
        .recv_timeout(WATCHDOG)
        .expect("ready() did not wake for a fresh datagram: idle-block regression");
    res.expect("ready() reported the fresh datagram");
}
