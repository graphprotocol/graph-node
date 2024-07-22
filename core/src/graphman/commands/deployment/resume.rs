use anyhow::anyhow;
use graph::components::store::StoreEvent;
use graph_store_postgres::command_support::catalog;
use slog::info;

use crate::graphman::deployment_search::{DeploymentSearch, DeploymentSelector};
use crate::graphman::{GraphmanContext, GraphmanError};
use crate::graphman_primitives::{BoxedFuture, ExtensibleGraphmanCommand, GraphmanCommand};

#[derive(Clone, Debug)]
pub struct ResumeDeploymentCommand {
    pub deployment: DeploymentSelector,
}

impl ResumeDeploymentCommand {
    fn try_execute(self, ctx: &GraphmanContext) -> Result<bool, GraphmanError> {
        let Self { deployment } = self;

        let search = DeploymentSearch::new(ctx.pool.clone());

        let locator = search.one_by_deployment(deployment)?.locator();

        let conn = ctx.pool.get()?;
        let mut conn = catalog::Connection::new(conn);

        let site = conn.locate_site(locator.clone())?.ok_or_else(|| {
            GraphmanError::Datastore(anyhow!("failed to locate site for '{locator}'"))
        })?;

        let Some((_, is_paused)) = conn.assignment_status(&site)? else {
            info!(ctx.logger, "Deployment '{}' not found", locator);
            return Ok(false);
        };

        if !is_paused {
            info!(ctx.logger, "Deployment '{}' is not paused", locator);
            return Ok(true);
        }

        info!(ctx.logger, "Resuming deployment '{}'", locator);

        let changes = conn.resume_subgraph(&site)?;
        conn.send_store_event(&ctx.notification_sender, &StoreEvent::new(changes))?;

        info!(ctx.logger, "Successfully resumed deployment '{}'", locator);
        Ok(true)
    }
}

impl<Ctx> GraphmanCommand<Ctx> for ResumeDeploymentCommand
where
    Ctx: AsRef<GraphmanContext> + Send + 'static,
{
    type Output = bool;
    type Error = GraphmanError;
    type Future = BoxedFuture<Self::Output, Self::Error>;

    fn execute(self, ctx: Ctx) -> Self::Future {
        Box::pin(async move { self.try_execute(ctx.as_ref()) })
    }
}

impl ExtensibleGraphmanCommand for ResumeDeploymentCommand {}
