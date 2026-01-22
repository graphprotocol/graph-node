use crate::adapter::EthereumAdapter as EthereumAdapterTrait;
use crate::EthereumAdapter;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::time::sleep;
#[derive(Debug)]
pub struct Health {
    pub provider: Arc<EthereumAdapter>,
    latency: Arc<RwLock<Duration>>,
    error_rate: Arc<RwLock<f64>>,
    consecutive_failures: Arc<RwLock<u32>>,
}

impl Health {
    pub fn new(provider: Arc<EthereumAdapter>) -> Self {
        Self {
            provider,
            latency: Arc::new(RwLock::new(Duration::from_secs(0))),
            error_rate: Arc::new(RwLock::new(0.0)),
            consecutive_failures: Arc::new(RwLock::new(0)),
        }
    }

    pub fn provider(&self) -> &str {
        self.provider.provider()
    }

    pub async fn check(&self) {
        let start_time = Instant::now();
        // For now, we'll just simulate a health check.
        // In a real implementation, we would send a request to the provider.
        let success = self.provider.provider().contains("rpc1"); // Simulate a failure for rpc2
        let latency = start_time.elapsed();

        self.update_metrics(success, latency);
    }

    fn update_metrics(&self, success: bool, latency: Duration) {
        let mut latency_w = self.latency.write().unwrap();
        *latency_w = latency;

        let mut error_rate_w = self.error_rate.write().unwrap();
        let mut consecutive_failures_w = self.consecutive_failures.write().unwrap();

        if success {
            *error_rate_w = *error_rate_w * 0.9; // Decay the error rate
            *consecutive_failures_w = 0;
        } else {
            *error_rate_w = *error_rate_w * 0.9 + 0.1; // Increase the error rate
            *consecutive_failures_w += 1;
        }
    }

    pub fn score(&self) -> f64 {
        let latency = *self.latency.read().unwrap();
        let error_rate = *self.error_rate.read().unwrap();
        let consecutive_failures = *self.consecutive_failures.read().unwrap();

        // This is a simple scoring algorithm. A more sophisticated algorithm could be used here.
        1.0 / (1.0 + latency.as_secs_f64() + error_rate + (consecutive_failures as f64))
    }
}

pub async fn health_check_task(health_checkers: Vec<Arc<Health>>) {
    loop {
        for health_checker in &health_checkers {
            health_checker.check().await;
        }
        sleep(Duration::from_secs(10)).await;
    }
}
