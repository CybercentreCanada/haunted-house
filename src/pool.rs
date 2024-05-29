//! A simple lock based resource pool for use in threaded (rather than async) contexts
//! 
//! In many cases this implementation has lots of issues. 
//! One example of this would be that T is assumed to be cheap to move.
//! 

use std::{ops::{Deref, DerefMut}, sync::Arc};

use parking_lot::{Condvar, Mutex};

struct PoolInner<T> {
    items: Mutex<Vec<T>>,
    signal: Condvar,
}

pub struct Pool<T> {
    inner: Arc<PoolInner<T>>
}

impl<T> Pool<T> {
    pub fn new(items: Vec<T>) -> Self {
        Self { inner: Arc::new(PoolInner {
            items: Mutex::new(items),
            signal: Condvar::new()
        })}
    }

    pub fn get(&self) -> Lease<T> {
        let mut items = self.inner.items.lock();
        if let Some(item) = items.pop() {
            return Lease {
                borrowed: Some(item),
                pool: self.inner.clone()
            }
        }

        loop {
            self.inner.signal.wait(&mut items);

            let mut items = self.inner.items.lock();
            if let Some(item) = items.pop() {
                return Lease {
                    borrowed: Some(item),
                    pool: self.inner.clone()
                }
            }
        }
    }
}

impl<T> Clone for Pool<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

pub struct Lease<T> {
    borrowed: Option<T>,
    pool: Arc<PoolInner<T>>
}

impl<T> Deref for Lease<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.borrowed.as_ref().unwrap()
    }
}

impl<T> DerefMut for Lease<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.borrowed.as_mut().unwrap()
    }
}

impl<T> Drop for Lease<T> {
    fn drop(&mut self) {
        self.pool.items.lock().push(self.borrowed.take().unwrap());
        self.pool.signal.notify_one();
    }
}