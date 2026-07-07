//! Covers `SharedVecPool` acquire / oversize / drop-returns invariants.

use transport_core::BufferPool;
use transport_mio::SharedVecPool;

#[test]
fn acquire_returns_slab_of_requested_len() {
    let pool = SharedVecPool::new(4, 1500);
    let slab = pool.acquire(64).expect("slab available");
    assert_eq!(slab.as_ref().len(), 64);
    assert_eq!(pool.in_use(), 1);
}

#[test]
fn oversize_request_returns_none() {
    let pool = SharedVecPool::new(2, 128);
    assert!(pool.acquire(129).is_none());
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn free_list_exhausts_then_recovers_after_drop() {
    let pool = SharedVecPool::new(2, 256);
    let a = pool.acquire(16).expect("first slab");
    let b = pool.acquire(16).expect("second slab");
    assert!(pool.acquire(16).is_none(), "pool exhausted");
    drop(a);
    let c = pool.acquire(16).expect("slab reclaimed after drop");
    assert_eq!(pool.in_use(), 2);
    drop(b);
    drop(c);
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn capacity_reports_configured_slots() {
    let pool = SharedVecPool::new(8, 512);
    assert_eq!(pool.capacity(), 8);
}
