use std::time::Duration;

/// Facilitate sleeping with an exponential backoff. Sleep durations will
/// increase by a factor of 2 from `base` until they reach `ceiling`, at
/// which point any call to `sleep` or `sleep_async` will sleep for
/// `ceiling`
pub struct ExponentialBackoff {
    pub attempt: u64,
    base: Duration,
    ceiling: Duration,
}

impl ExponentialBackoff {
    pub fn new(base: Duration, ceiling: Duration) -> Self {
        ExponentialBackoff {
            attempt: 0,
            base,
            ceiling,
        }
    }

    /// Record that we made an attempt and sleep for the appropriate amount
    /// of time. Do not use this from async contexts since it uses
    /// `thread::sleep`
    pub fn sleep(&mut self) {
        std::thread::sleep(self.next_attempt());
    }

    /// Record that we made an attempt and sleep for the appropriate amount
    /// of time
    pub async fn sleep_async(&mut self) {
        tokio::time::sleep(self.next_attempt()).await
    }

    pub fn delay(&self) -> Duration {
        let mut delay = self.base.saturating_mul(1 << self.attempt);
        if delay > self.ceiling {
            delay = self.ceiling;
        }
        delay
    }

    fn next_attempt(&mut self) -> Duration {
        let delay = self.delay();
        self.attempt += 1;
        delay
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}
