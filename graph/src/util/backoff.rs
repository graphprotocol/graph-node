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
        let mut delay = self.base.saturating_mul(1u32 << self.attempt.min(31));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_delay() {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(500), Duration::from_secs(5));

        // First delay should be base (0.5s)
        assert_eq!(backoff.next_attempt(), Duration::from_millis(500));

        // Second delay should be 1s (base * 2^1)
        assert_eq!(backoff.next_attempt(), Duration::from_secs(1));

        // Third delay should be 2s (base * 2^2)
        assert_eq!(backoff.next_attempt(), Duration::from_secs(2));

        // Fourth delay should be 4s (base * 2^3)
        assert_eq!(backoff.next_attempt(), Duration::from_secs(4));

        // Seventh delay should be ceiling (5s)
        assert_eq!(backoff.next_attempt(), Duration::from_secs(5));

        // Eighth delay should also be ceiling (5s)
        assert_eq!(backoff.next_attempt(), Duration::from_secs(5));
    }

    #[test]
    fn test_overflow_delay() {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_millis(500), Duration::from_secs(45));

        // 31st should be ceiling (45s) without overflowing
        backoff.attempt = 31;
        assert_eq!(backoff.next_attempt(), Duration::from_secs(45));
        assert_eq!(backoff.next_attempt(), Duration::from_secs(45));

        backoff.attempt = 123456;
        assert_eq!(backoff.next_attempt(), Duration::from_secs(45));
    }

    #[tokio::test]
    async fn test_sleep_async() {
        let mut backoff =
            ExponentialBackoff::new(Duration::from_secs_f32(0.1), Duration::from_secs_f32(0.2));

        let start = Instant::now();
        backoff.sleep_async().await;
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_secs_f32(0.1) && elapsed < Duration::from_secs_f32(0.15));

        let start = Instant::now();
        backoff.sleep_async().await;
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_secs_f32(0.2) && elapsed < Duration::from_secs_f32(0.25));

        let start = Instant::now();
        backoff.sleep_async().await;
        let elapsed = start.elapsed();

        assert!(elapsed >= Duration::from_secs_f32(0.2) && elapsed < Duration::from_secs_f32(0.25));
    }
}
