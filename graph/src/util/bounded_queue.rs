use std::{collections::VecDeque, sync::Mutex};

use crate::prelude::tokio::sync::Semaphore;

/// An async-friendly queue of bounded size. In contrast to a bounded channel,
/// the queue makes it possible to modify and remove entries in it.
pub struct BoundedQueue<T> {
    /// The maximum number of entries allowed in the queue
    capacity: usize,
    /// The actual items in the queue. New items are appended at the back, and
    /// popped off the front.
    queue: Mutex<VecDeque<T>>,
    /// This semaphore has as many permits as there are empty spots in the
    /// `queue`, i.e., `capacity - queue.len()` many permits
    push_semaphore: Semaphore,
    /// This semaphore has as many permits as there are entrie in the queue,
    /// i.e., `queue.len()` many
    pop_semaphore: Semaphore,
}

impl<T> std::fmt::Debug for BoundedQueue<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let queue = self.queue.lock().unwrap();
        write!(
            f,
            "BoundedQueue[cap: {}, queue: {}/{}, push: {}, pop: {}]",
            self.capacity,
            queue.len(),
            queue.capacity(),
            self.push_semaphore.available_permits(),
            self.pop_semaphore.available_permits(),
        )
    }
}

impl<T: Clone> BoundedQueue<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            queue: Mutex::new(VecDeque::with_capacity(capacity)),
            push_semaphore: Semaphore::new(capacity),
            pop_semaphore: Semaphore::new(0),
        }
    }

    /// Get an item from the queue. If the queue is currently empty
    /// this method blocks until an item is available.
    pub async fn pop(&self) -> T {
        let permit = self.pop_semaphore.acquire().await.unwrap();
        let item = self
            .queue
            .lock()
            .unwrap()
            .pop_front()
            .expect("the queue is not empty");
        permit.forget();
        self.push_semaphore.add_permits(1);
        item
    }

    /// Take an item from the front of the queue and return a copy. If the
    /// queue is currently empty this method blocks until an item is
    /// available.
    pub async fn peek(&self) -> T {
        let _permit = self.pop_semaphore.acquire().await.unwrap();
        let queue = self.queue.lock().unwrap();
        let item = queue.front().expect("the queue is not empty");
        item.clone()
    }

    /// Push an item into the queue. If the queue is currently full this method
    /// blocks until an item is available
    pub async fn push(&self, item: T) {
        let permit = self.push_semaphore.acquire().await.unwrap();
        self.queue.lock().unwrap().push_back(item);
        permit.forget();
        self.pop_semaphore.add_permits(1);
    }

    pub async fn wait_empty(&self) {
        self.push_semaphore
            .acquire_many(self.capacity as u32)
            .await
            .map(|_| ())
            .expect("we never close the push_semaphore")
    }

    pub fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.lock().unwrap().is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Iterate over the entries in the queue from newest to oldest entry
    /// atomically, applying `f` to each entry and returning the first
    /// result that is not `None`.
    ///
    /// This method locks the queue while it is executing, and `f` should
    /// therefore not do any slow work.
    pub fn find_map<B, F>(&self, f: F) -> Option<B>
    where
        F: FnMut(&T) -> Option<B>,
    {
        let queue = self.queue.lock().unwrap();
        queue.iter().rev().find_map(f)
    }

    /// Iterate over the entries in the queue from newest to oldest entry
    /// atomically, applying `f` to each entry and returning the result of
    /// the last invocation of `f`.
    ///
    /// This method locks the queue while it is executing, and `f` should
    /// therefore not do any slow work.
    pub fn fold<B, F>(&self, init: B, f: F) -> B
    where
        F: FnMut(B, &T) -> B,
    {
        let queue = self.queue.lock().unwrap();
        queue.iter().rev().fold(init, f)
    }

    pub async fn clear(&self) {
        let pushed = {
            let mut queue = self.queue.lock().unwrap();
            let pushed = queue.len();
            queue.clear();
            pushed
        };
        self.push_semaphore.add_permits(pushed);
        let _permits = self
            .pop_semaphore
            .acquire_many(pushed as u32)
            .await
            .expect("we never close the pop_semaphore");
    }
}
