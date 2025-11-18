use std::{sync::Arc, time::Duration};

use alloy::primitives::BlockNumber;
use graph::{
    cheap_clone::CheapClone,
    components::{
        metrics::{stopwatch::StopwatchMetrics, MetricsRegistry},
        store::WritableStore,
    },
    prelude::DeploymentHash,
};
use indoc::indoc;
use prometheus::{IntCounter, IntGauge};
use slog::Logger;

/// Contains metrics specific to a deployment.
pub(super) struct Metrics {
    pub(super) deployment_status: DeploymentStatus,
    pub(super) deployment_head: DeploymentHead,
    pub(super) deployment_target: DeploymentTarget,
    pub(super) deployment_synced: DeploymentSynced,
    pub(super) indexing_duration: IndexingDuration,
    pub(super) blocks_processed: BlocksProcessed,
    pub(super) stopwatch: StopwatchMetrics,
}

impl Metrics {
    /// Creates new deployment specific metrics.
    pub(super) fn new(
        logger: &Logger,
        metrics_registry: Arc<MetricsRegistry>,
        store: Arc<dyn WritableStore>,
        deployment: DeploymentHash,
    ) -> Self {
        let stopwatch = StopwatchMetrics::new(
            logger.cheap_clone(),
            deployment.cheap_clone(),
            "amp-process",
            metrics_registry.cheap_clone(),
            store.shard().to_string(),
        );

        let const_labels = [("deployment", &deployment)];

        Self {
            deployment_status: DeploymentStatus::new(&metrics_registry, const_labels.clone()),
            deployment_head: DeploymentHead::new(&metrics_registry, const_labels.clone()),
            deployment_target: DeploymentTarget::new(&metrics_registry, const_labels.clone()),
            deployment_synced: DeploymentSynced::new(&metrics_registry, const_labels.clone()),
            indexing_duration: IndexingDuration::new(&metrics_registry, const_labels.clone()),
            blocks_processed: BlocksProcessed::new(&metrics_registry, const_labels.clone()),
            stopwatch,
        }
    }
}

/// Reports the current indexing status of a deployment.
pub(super) struct DeploymentStatus(IntGauge);

impl DeploymentStatus {
    const STATUS_STARTING: i64 = 1;
    const STATUS_RUNNING: i64 = 2;
    const STATUS_STOPPED: i64 = 3;
    const STATUS_FAILED: i64 = 4;

    fn new(
        metrics_registry: &MetricsRegistry,
        const_labels: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        let int_gauge = metrics_registry
            .new_int_gauge(
                "amp_deployment_status",
                indoc!(
                    "
                    Indicates the current indexing status of a deployment.
                    Possible values:
                    1 - graph-node is preparing to start indexing;
                    2 - deployment is being indexed;
                    3 - indexing is stopped by request;
                    4 - indexing failed;
                    "
                ),
                const_labels,
            )
            .expect("failed to register `amp_deployment_status` gauge");

        Self(int_gauge)
    }

    /// Records that the graph-node is preparing to start indexing.
    pub fn starting(&self) {
        self.0.set(Self::STATUS_STARTING);
    }

    /// Records that the deployment is being indexed.
    pub fn running(&self) {
        self.0.set(Self::STATUS_RUNNING);
    }

    /// Records that the indexing stopped by request.
    pub fn stopped(&self) {
        self.0.set(Self::STATUS_STOPPED);
    }

    /// Records that the indexing failed.
    pub fn failed(&self) {
        self.0.set(Self::STATUS_FAILED);
    }
}

/// Tracks the most recent block number processed by a deployment.
pub(super) struct DeploymentHead(IntGauge);

impl DeploymentHead {
    fn new(
        metrics_registry: &MetricsRegistry,
        const_labels: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        let int_gauge = metrics_registry
            .new_int_gauge(
                "amp_deployment_head",
                "Tracks the most recent block number processed by a deployment",
                const_labels,
            )
            .expect("failed to register `amp_deployment_head` gauge");

        Self(int_gauge)
    }

    /// Updates the most recent block number processed by this deployment.
    pub(super) fn update(&self, new_most_recent_block_number: BlockNumber) {
        self.0.set(
            i64::try_from(new_most_recent_block_number)
                .expect("new most recent block number does not fit into `i64`"),
        );
    }
}

/// Tracks the target block number of a deployment.
pub(super) struct DeploymentTarget(IntGauge);

impl DeploymentTarget {
    fn new(
        metrics_registry: &MetricsRegistry,
        const_labels: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        let int_gauge = metrics_registry
            .new_int_gauge(
                "amp_deployment_target",
                "Tracks the target block number of a deployment",
                const_labels,
            )
            .expect("failed to register `amp_deployment_target` gauge");

        Self(int_gauge)
    }

    /// Updates the target block number of this deployment.
    pub(super) fn update(&self, new_target_block_number: BlockNumber) {
        self.0.set(
            i64::try_from(new_target_block_number)
                .expect("new target block number does not fit into `i64`"),
        );
    }
}

/// Indicates whether a deployment has reached the chain head or the end block since it was deployed.
pub(super) struct DeploymentSynced(IntGauge);

impl DeploymentSynced {
    const NOT_SYNCED: i64 = 0;
    const SYNCED: i64 = 1;

    pub fn new(
        metrics_registry: &MetricsRegistry,
        const_labels: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        let int_gauge = metrics_registry
            .new_int_gauge(
                "amp_deployment_synced",
                indoc!(
                    "
                    Indicates whether a deployment has reached the chain head or the end block since it was deployed.
                    Possible values:
                    0 - deployment is not synced;
                    1 - deployment is synced;
                    "
                ),
                const_labels,
            )
            .expect("failed to register `amp_deployment_synced` gauge");

        Self(int_gauge)
    }

    /// Records the current sync status of this deployment.
    pub fn record(&self, synced: bool) {
        self.0.set(if synced {
            Self::SYNCED
        } else {
            Self::NOT_SYNCED
        });
    }
}

/// Tracks the total duration in seconds of deployment indexing.
#[derive(Clone)]
pub(super) struct IndexingDuration(IntCounter);

impl IndexingDuration {
    fn new(
        metrics_registry: &MetricsRegistry,
        const_labels: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        let int_counter = metrics_registry
            .new_int_counter(
                "amp_deployment_indexing_duration_seconds",
                "Tracks the total duration in seconds of deployment indexing",
                const_labels,
            )
            .expect("failed to register `amp_deployment_indexing_duration_seconds` counter");

        Self(int_counter)
    }

    /// Records a new indexing duration of this deployment.
    pub(super) fn record(&self, duration: Duration) {
        self.0.inc_by(duration.as_secs())
    }
}

/// Tracks the total number of blocks processed by a deployment.
pub(super) struct BlocksProcessed(IntCounter);

impl BlocksProcessed {
    fn new(
        metrics_registry: &MetricsRegistry,
        const_labels: impl IntoIterator<Item = (impl ToString, impl ToString)>,
    ) -> Self {
        let int_counter = metrics_registry
            .new_int_counter(
                "amp_deployment_blocks_processed_count",
                "Tracks the total number of blocks processed by a deployment",
                const_labels,
            )
            .expect("failed to register `amp_deployment_blocks_processed_count` counter");

        Self(int_counter)
    }

    /// Records a new processed block.
    pub(super) fn record_one(&self) {
        self.record(1);
    }

    /// Records the new processed blocks.
    pub(super) fn record(&self, number_of_blocks_processed: usize) {
        self.0.inc_by(number_of_blocks_processed as u64);
    }
}
