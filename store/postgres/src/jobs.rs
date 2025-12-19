//! Jobs for database maintenance
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use diesel::{sql_query, sql_types::Double};
use diesel_async::RunQueryDsl;

use graph::prelude::{error, Logger, MetricsRegistry, StoreError, ENV_VARS};
use graph::prometheus::Gauge;
use graph::util::jobs::{Job, Runner};

use crate::ConnectionPool;
use crate::{unused, Store, SubgraphStore};

pub fn register(
    runner: &mut Runner,
    store: Arc<Store>,
    primary_pool: ConnectionPool,
    registry: Arc<MetricsRegistry>,
) {
    const ONE_MINUTE: Duration = Duration::from_secs(60);
    const ONE_HOUR: Duration = Duration::from_secs(60 * 60);

    runner.register(
        Arc::new(VacuumDeploymentsJob::new(store.subgraph_store())),
        ONE_MINUTE,
    );

    runner.register(
        Arc::new(NotificationQueueUsage::new(primary_pool, registry)),
        ONE_MINUTE,
    );

    runner.register(
        Arc::new(MirrorPrimary::new(store.subgraph_store())),
        15 * ONE_MINUTE,
    );

    // Remove unused deployments every 2 hours
    runner.register(
        Arc::new(UnusedJob::new(store.subgraph_store())),
        2 * ONE_HOUR,
    );

    runner.register(
        Arc::new(RefreshMaterializedView::new(store.subgraph_store())),
        6 * ONE_HOUR,
    );

    if let Some(interval) = ENV_VARS.store.account_like_scan_interval_hours {
        runner.register(
            Arc::new(AccountLikeJob::new(store.subgraph_store())),
            interval * ONE_HOUR,
        );
    }
}

/// A job that vacuums `subgraphs.deployment` and `subgraphs.head`. With a
/// large number of subgraphs, the autovacuum daemon might not run often
/// enough to keep this table, which is _very_ write-heavy, from getting
/// bloated. We therefore set up a separate job that vacuums the table once
/// a minute
struct VacuumDeploymentsJob {
    store: Arc<SubgraphStore>,
}

impl VacuumDeploymentsJob {
    fn new(store: Arc<SubgraphStore>) -> VacuumDeploymentsJob {
        VacuumDeploymentsJob { store }
    }
}

#[async_trait]
impl Job for VacuumDeploymentsJob {
    fn name(&self) -> &str {
        "Vacuum subgraphs.deployment and subgraphs.head"
    }

    async fn run(&self, logger: &Logger) {
        for res in self.store.vacuum().await {
            if let Err(e) = res {
                error!(logger, "Vacuum of subgraphs.deployment failed: {}", e);
            }
        }
    }
}

struct NotificationQueueUsage {
    primary: ConnectionPool,
    usage_gauge: Box<Gauge>,
}

impl NotificationQueueUsage {
    fn new(primary: ConnectionPool, registry: Arc<MetricsRegistry>) -> Self {
        let usage_gauge = registry
            .new_gauge(
                "notification_queue_usage",
                "Time series of pg_notification_queue_usage()",
                HashMap::new(),
            )
            .expect("Can register the notification_queue_usage gauge");
        NotificationQueueUsage {
            primary,
            usage_gauge,
        }
    }

    async fn update(&self) -> Result<(), StoreError> {
        #[derive(QueryableByName)]
        struct Usage {
            #[diesel(sql_type = Double)]
            usage: f64,
        }
        let usage_gauge = self.usage_gauge.clone();
        let mut conn = self.primary.get().await?;
        let res = sql_query("select pg_notification_queue_usage() as usage")
            .get_result::<Usage>(&mut conn)
            .await?;
        usage_gauge.set(res.usage);
        Ok(())
    }
}

#[async_trait]
impl Job for NotificationQueueUsage {
    fn name(&self) -> &str {
        "Report pg_notification_queue_usage()"
    }

    async fn run(&self, logger: &Logger) {
        if let Err(e) = self.update().await {
            error!(
                logger,
                "Update of `notification_queue_usage` gauge failed: {}", e
            );
        }
    }
}

struct MirrorPrimary {
    store: Arc<SubgraphStore>,
}

impl MirrorPrimary {
    fn new(store: Arc<SubgraphStore>) -> MirrorPrimary {
        MirrorPrimary { store }
    }
}

#[async_trait]
impl Job for MirrorPrimary {
    fn name(&self) -> &str {
        "Reconcile important tables from the primary"
    }

    async fn run(&self, logger: &Logger) {
        self.store.mirror_primary_tables(logger).await;
    }
}

struct RefreshMaterializedView {
    store: Arc<SubgraphStore>,
}

impl RefreshMaterializedView {
    fn new(store: Arc<SubgraphStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl Job for RefreshMaterializedView {
    fn name(&self) -> &str {
        "Refresh materialized views"
    }

    async fn run(&self, logger: &Logger) {
        self.store.refresh_materialized_views(logger).await;
    }
}

struct UnusedJob {
    store: Arc<SubgraphStore>,
}

impl UnusedJob {
    fn new(store: Arc<SubgraphStore>) -> UnusedJob {
        UnusedJob { store }
    }
}

#[async_trait]
impl Job for UnusedJob {
    fn name(&self) -> &str {
        "Record and remove unused deployments"
    }

    /// Record unused deployments and remove ones that were recorded at
    /// least `UNUSED_INTERVAL` ago
    async fn run(&self, logger: &Logger) {
        // Work on removing about 5 minutes
        const REMOVAL_DEADLINE: Duration = Duration::from_secs(5 * 60);

        let start = Instant::now();

        if let Err(e) = self.store.record_unused_deployments().await {
            error!(logger, "failed to record unused deployments"; "error" => e.to_string());
            return;
        }

        let remove = match self
            .store
            .list_unused_deployments(unused::Filter::UnusedLongerThan(
                ENV_VARS.store.remove_unused_interval,
            ))
            .await
        {
            Ok(remove) => remove,
            Err(e) => {
                error!(logger, "failed to list removable deployments"; "error" => e.to_string());
                return;
            }
        };

        for deployment in remove {
            match self.store.remove_deployment(deployment.id).await {
                Ok(()) => { /* ignore */ }
                Err(e) => {
                    error!(logger, "failed to remove unused deployment";
                                   "sgd" => deployment.id.to_string(),
                                   "deployment" => deployment.deployment,
                                   "error" => e.to_string());
                }
            }
            // Stop working on removing after a while to not block other
            // jobs for too long
            if start.elapsed() > REMOVAL_DEADLINE {
                return;
            }
        }
    }
}

struct AccountLikeJob {
    store: Arc<SubgraphStore>,
}

impl AccountLikeJob {
    fn new(store: Arc<SubgraphStore>) -> AccountLikeJob {
        AccountLikeJob { store }
    }
}

#[async_trait]
impl Job for AccountLikeJob {
    fn name(&self) -> &str {
        "Set account-like flag on eligible tables"
    }

    async fn run(&self, logger: &Logger) {
        // Safe to unwrap due to a startup validation
        // which ensures these values are present when account_like_scan_interval_hours is set.
        let min_versions_count = ENV_VARS.store.account_like_min_versions_count.unwrap();
        let ratio = ENV_VARS.store.account_like_max_unique_ratio.unwrap();

        self.store
            .identify_and_set_account_like(logger, min_versions_count, ratio)
            .await
            .unwrap_or_else(|e| {
                error!(
                    logger,
                    "Failed to set account-like flag on eligible tables: {}", e
                )
            });
    }
}
