//! Locks `SharedVecPool` acquire/release accounting, exhaustion behavior,
//! and multi-thread safety under contention.

use std::{thread, time::Duration};

use transport_core::BufferPool;
use transport_mio::SharedVecPool;

#[test]
fn acquire_and_drop_restore_slot() {
    let pool = SharedVecPool::new(2, 1024);
    assert_eq!(pool.capacity(), 2);
    assert_eq!(pool.in_use(), 0);
    assert_eq!(pool.slab_size(), 1024);

    let slab = pool.acquire(512).expect("acquire slab");
    assert_eq!(pool.in_use(), 1);
    assert_eq!(slab.len(), 0);
    assert!(slab.is_empty());
    assert_eq!(slab.as_ref(), b"");
    drop(slab);
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn acquire_returns_none_when_exhausted() {
    let pool = SharedVecPool::new(2, 128);
    let a = pool.acquire(64).expect("first");
    let b = pool.acquire(64).expect("second");
    assert!(pool.acquire(64).is_none(), "third acquire must fail");
    assert_eq!(pool.in_use(), 2);
    drop(a);
    drop(b);
    assert_eq!(pool.in_use(), 0);
    assert!(pool.acquire(64).is_some(), "reclaim after drop");
}

#[test]
fn acquire_rejects_len_over_slab_size() {
    let pool = SharedVecPool::new(4, 128);
    assert!(pool.acquire(129).is_none());
    assert!(pool.acquire(128).is_some());
}

#[test]
fn buf_mut_then_set_len_bounds_as_ref() {
    let pool = SharedVecPool::new(1, 64);
    let mut slab = pool.acquire(64).expect("acquire slab");
    let buf = slab.buf_mut();
    assert_eq!(buf.len(), 64, "full-width slab exposed for a recv to write");
    buf[..3].copy_from_slice(b"abc");
    slab.set_len(3);
    assert_eq!(slab.as_ref(), b"abc", "as_ref bounded by recorded length");
}

#[test]
fn concurrent_acquires_from_multiple_threads() {
    let pool = SharedVecPool::new(64, 256);
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let pool = pool.clone();
            thread::spawn(move || {
                let mut slabs = Vec::new();
                for _ in 0..8 {
                    if let Some(s) = pool.acquire(200) {
                        slabs.push(s);
                    }
                    thread::sleep(Duration::from_micros(50));
                }
                slabs
            })
        })
        .collect();

    let mut total = 0;
    for h in handles {
        total += h.join().expect("join").len();
    }
    assert_eq!(total, 64, "all 64 slots acquired across threads");
    assert_eq!(pool.in_use(), 0, "slabs dropped on join, pool restored");
}
