//! Jobs for database maintenance
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use graph::prelude::{error, Logger};
use graph::util::jobs::{Job, Runner};

use crate::{Store, SubgraphStore};

pub fn register(runner: &mut Runner, store: Arc<Store>) {
    runner.register(
        Arc::new(VacuumDeploymentsJob::new(store.subgraph_store())),
        Duration::from_secs(60),
    );
}

/// A job that vacuums `subgraphs.subgraph_deployment`. With a large number
/// of subgraphs, the autovacuum daemon might not run often enough to keep
/// this table, which is _very_ write-heavy, from getting bloated. We
/// therefore set up a separate job that vacuums the table once a minute
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
        "Vacuum subgraphs.subgraph_deployment"
    }

    async fn run(&self, logger: &Logger) {
        for res in self.store.vacuum().await {
            if let Err(e) = res {
                error!(
                    logger,
                    "Vacuum of subgraphs.subgraph_deployment failed: {}", e
                );
            }
        }
    }
}
