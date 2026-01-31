use crate::adapter::EthereumAdapter as _;
use crate::EthereumAdapter;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct Health {
    provider: Arc<EthereumAdapter>,
    latency_nanos: AtomicU64,
    error_rate_bits: AtomicU64,
    consecutive_failures: AtomicU32,
}

impl Health {
    pub fn new(provider: Arc<EthereumAdapter>) -> Self {
        Self {
            provider,
            latency_nanos: AtomicU64::new(0),
            error_rate_bits: AtomicU64::new(0f64.to_bits()),
            consecutive_failures: AtomicU32::new(0),
        }
    }

    pub fn provider(&self) -> &str {
        self.provider.provider()
    }

    pub async fn check(&self) {
        let start = Instant::now();
        let success = self.provider.health_check().await.is_ok();
        self.update_metrics(success, start.elapsed());
    }

    fn update_metrics(&self, success: bool, latency: Duration) {
        self.latency_nanos
            .store(latency.as_nanos() as u64, Ordering::Relaxed);

        let prev_error_rate = f64::from_bits(self.error_rate_bits.load(Ordering::Relaxed));

        if success {
            let new_error_rate = prev_error_rate * 0.9;
            self.error_rate_bits
                .store(new_error_rate.to_bits(), Ordering::Relaxed);
            self.consecutive_failures.store(0, Ordering::Relaxed);
        } else {
            let new_error_rate = prev_error_rate * 0.9 + 0.1;
            self.error_rate_bits
                .store(new_error_rate.to_bits(), Ordering::Relaxed);
            self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn score(&self) -> f64 {
        let latency_secs =
            Duration::from_nanos(self.latency_nanos.load(Ordering::Relaxed)).as_secs_f64();
        let error_rate = f64::from_bits(self.error_rate_bits.load(Ordering::Relaxed));
        let consecutive_failures = self.consecutive_failures.load(Ordering::Relaxed);

        1.0 / (1.0 + latency_secs + error_rate + (consecutive_failures as f64))
    }
}

pub async fn health_check_task(health_checkers: Vec<Arc<Health>>, cancel_token: CancellationToken) {
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => break,
            _ = async {
                for hc in &health_checkers {
                    hc.check().await;
                }
                tokio::time::sleep(Duration::from_secs(10)).await;
            } => {}
        }
    }
}
