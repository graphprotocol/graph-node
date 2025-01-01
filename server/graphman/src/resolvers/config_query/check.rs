use async_graphql::{Context, Result};
use graphman::commands::config::check::check;

use crate::{entities::ConfigCheckResponse, resolvers::context::GraphmanContext};

pub fn run(ctx: &Context<'_>) -> Result<ConfigCheckResponse> {
    let ctx = GraphmanContext::new(ctx)?;
    let res = check(&ctx.config, true)?;
    Ok(ConfigCheckResponse::from(
        res.validated,
        res.validated_subgraph_settings,
        res.config_json.unwrap_or_default(),
    ))
}
