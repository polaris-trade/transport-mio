//! Naive Vec-backed buffer pool. Free list of pre-sized `Vec<u8>` slabs;
//! `Drop` on the slab returns the buffer to the free list. `SharedVecPool`
//! is the `Arc`-wrapped handle that implements `BufferPool`.

use parking_lot::Mutex;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use transport_core::BufferPool;

pub struct VecPool {
    free: Mutex<Vec<Vec<u8>>>,
    capacity: usize,
    slab_size: usize,
    in_use: AtomicUsize,
}

impl VecPool {
    pub fn new(capacity: usize, slab_size: usize) -> Self {
        let mut free = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            free.push(vec![0u8; slab_size]);
        }
        Self {
            free: Mutex::new(free),
            capacity,
            slab_size,
            in_use: AtomicUsize::new(0),
        }
    }

    pub fn slab_size(&self) -> usize {
        self.slab_size
    }

    fn take_slab(&self) -> Option<Vec<u8>> {
        self.free.lock().pop()
    }

    fn return_slab(&self, mut buf: Vec<u8>) {
        buf.clear();
        buf.resize(self.slab_size, 0);
        self.free.lock().push(buf);
    }
}

#[derive(Clone)]
pub struct SharedVecPool(pub Arc<VecPool>);

impl SharedVecPool {
    pub fn new(capacity: usize, slab_size: usize) -> Self {
        Self(Arc::new(VecPool::new(capacity, slab_size)))
    }

    pub fn acquire(&self, len: usize) -> Option<VecSlab> {
        if len > self.0.slab_size {
            return None;
        }
        let buf = self.0.take_slab()?;
        self.0.in_use.fetch_add(1, Ordering::Relaxed);
        Some(VecSlab {
            buf,
            len,
            pool: Arc::clone(&self.0),
        })
    }
}

impl BufferPool for SharedVecPool {
    type Slab = VecSlab;

    fn acquire(&self, len: usize) -> Option<Self::Slab> {
        self.acquire(len)
    }

    fn capacity(&self) -> usize {
        self.0.capacity
    }

    fn in_use(&self) -> usize {
        self.0.in_use.load(Ordering::Relaxed)
    }
}

pub struct VecSlab {
    buf: Vec<u8>,
    len: usize,
    pool: Arc<VecPool>,
}

impl VecSlab {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl AsRef<[u8]> for VecSlab {
    fn as_ref(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

impl Drop for VecSlab {
    fn drop(&mut self) {
        let buf = mem::take(&mut self.buf);
        self.pool.return_slab(buf);
        self.pool.in_use.fetch_sub(1, Ordering::Relaxed);
    }
}
