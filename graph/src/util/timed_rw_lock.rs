use slog::{warn, Logger};
use std::{
    sync::RwLock,
    time::{Duration, Instant},
};

/// Adds instrumentation for timing the performance of the lock.
pub struct TimedRwLock<T> {
    id: String,
    lock: RwLock<T>,
}

impl<T> TimedRwLock<T> {
    pub fn new(id: impl Into<String>, x: T) -> Self {
        TimedRwLock {
            id: id.into(),
            lock: RwLock::new(x),
        }
    }

    pub fn write(&self, logger: &Logger) -> std::sync::RwLockWriteGuard<T> {
        let start = Instant::now();
        let guard = self.lock.write().unwrap();
        let elapsed = start.elapsed();
        if elapsed > Duration::from_millis(100) {
            warn!(logger, "Write lock took a long time to acquire";
                          "id" => &self.id,
                          "wait_ms" => elapsed.as_millis(),
            );
        }
        guard
    }

    pub fn read(&self, logger: &Logger) -> std::sync::RwLockReadGuard<T> {
        let start = Instant::now();
        let guard = self.lock.read().unwrap();
        let elapsed = start.elapsed();
        if elapsed > Duration::from_millis(100) {
            warn!(logger, "Read lock took a long time to acquire";
                          "id" => &self.id,
                          "wait_ms" => elapsed.as_millis(),
            );
        }
        guard
    }
}
