//! Drives `UdpTransport::recv_burst` over loopback: a datagram flood lands as
//! owned pool frames, and the pool reclaims every slab once the batch drops.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    time::Duration,
};

use transport_core::{
    AffinityConfig, AsPayload, BatchConfig, BindConfig, BufferPool, FrameBatch, RecvBufConfig,
    RingConfig, SendBufConfig,
};
use transport_mio::{UdpFrame, UdpTransport};

fn loopback() -> BindConfig {
    let mut b = BindConfig::default();
    b.addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    b
}

#[test]
fn recv_burst_reaps_datagrams_into_owned_frames() {
    let mut receiver = UdpTransport::bind(
        loopback(),
        RecvBufConfig::default(),
        SendBufConfig::default(),
        RingConfig::default(),
        BatchConfig::default(),
        AffinityConfig::default(),
    )
    .expect("bind receiver");
    let addr = receiver.local_addr().expect("local addr");

    let sender = UdpSocket::bind("127.0.0.1:0").expect("sender bind");
    for i in 0..4u8 {
        sender.send_to(&[i; 8], addr).expect("send_to");
    }
    // Loopback delivery is not synchronous with send_to; give the kernel a beat.
    std::thread::sleep(Duration::from_millis(20));

    let mut batch: FrameBatch<UdpFrame> = FrameBatch::with_capacity(16);
    let mut got = 0;
    let mut attempts = 0;
    while got < 4 && attempts < 1000 {
        got += receiver.recv_burst(&mut batch, 16).expect("recv_burst");
        attempts += 1;
    }
    assert_eq!(got, 4, "all four datagrams reaped");
    assert_eq!(
        receiver.pool().in_use(),
        4,
        "one live slab per reaped frame"
    );

    for frame in batch.drain() {
        assert_eq!(frame.payload().len(), 8, "datagram payload width");
    }
    assert_eq!(receiver.pool().in_use(), 0, "slabs reclaimed after drain");
}

#[test]
fn recv_burst_on_idle_socket_returns_zero() {
    let mut receiver = UdpTransport::bind(
        loopback(),
        RecvBufConfig::default(),
        SendBufConfig::default(),
        RingConfig::default(),
        BatchConfig::default(),
        AffinityConfig::default(),
    )
    .expect("bind receiver");

    let mut batch: FrameBatch<UdpFrame> = FrameBatch::with_capacity(8);
    let n = receiver.recv_burst(&mut batch, 8).expect("recv_burst");
    assert_eq!(n, 0, "no data pending yields Ok(0)");
    assert_eq!(receiver.pool().in_use(), 0, "no slab left checked out");
}
