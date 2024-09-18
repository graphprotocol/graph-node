use async_graphql::Context;
use async_graphql::Result;

use crate::entities::DeploymentInfo;
use crate::entities::DeploymentSelector;
use crate::entities::DeploymentVersionSelector;
use crate::resolvers::context::GraphmanContext;

pub fn run(
    ctx: &Context<'_>,
    deployment: Option<DeploymentSelector>,
    version: Option<DeploymentVersionSelector>,
) -> Result<Vec<DeploymentInfo>> {
    let load_status = ctx.look_ahead().field("status").exists();
    let ctx = GraphmanContext::new(ctx)?;

    let deployment = deployment
        .map(TryInto::try_into)
        .transpose()?
        .unwrap_or(graphman::deployment::DeploymentSelector::All);

    let version = version
        .map(Into::into)
        .unwrap_or(graphman::deployment::DeploymentVersionSelector::All);

    let deployments = graphman::commands::deployment::info::load_deployments(
        ctx.primary_pool.clone(),
        &deployment,
        &version,
    )?;

    let statuses = if load_status {
        graphman::commands::deployment::info::load_deployment_statuses(
            ctx.store.clone(),
            &deployments,
        )?
    } else {
        Default::default()
    };

    let resp = deployments
        .into_iter()
        .map(|deployment| {
            let status = statuses.get(&deployment.id).cloned().map(Into::into);

            let mut info: DeploymentInfo = deployment.into();
            info.status = status;

            info
        })
        .collect();

    Ok(resp)
}
