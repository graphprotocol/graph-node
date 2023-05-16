use std::collections::BTreeMap;
use std::str::FromStr;

use graph::log::logger_with_levels;
use graph::prelude::NodeId;
use graph::url::Url;
use graph_core::graphman::config::{ChainSection, Config, Deployment, Opt};
use graph_core::graphman::context::GraphmanContext;
use graph_core::graphman::core;
use graph_core::graphman::core::info::InfoResult;
use graph_core::graphman::deployment::DeploymentSearch;
use graph_core::graphman::shard::Shard;
use graph_store_postgres::PRIMARY_SHARD;
use juniper::{
    graphql_object, graphql_value, EmptySubscription, FieldError, FieldResult, GraphQLObject,
};

use crate::inputs::ConfigInput;

#[allow(unused)]
struct Subscription;

pub struct GQLContext {}

// To make our context usable by Juniper, we have to implement a marker trait.
impl juniper::Context for GQLContext {}

#[derive(GraphQLObject)]
#[graphql(description = "Success message")]
struct SuccessMessage {
    message: String,
}

pub struct Query;

#[graphql_object(
    // Here we specify the context type for the object.
    // We need to do this in every type that
    // needs access to the context.
    context = GQLContext,
)]
impl Query {
    fn info(
        config: ConfigInput,
        deployment: Option<String>,
        context: &GQLContext,
    ) -> FieldResult<InfoResult> {
        let ConfigInput {
            all,
            node_id,
            ipfs,
            current,
            pending,
            status,
            used,
            postgres_url,
            fork_base,
            version_label,
        } = config;

        let all = all.unwrap_or_else(|| false);

        let deployment = if deployment.is_some() && all != true {
            DeploymentSearch::from_str(&deployment.unwrap())?
        } else {
            DeploymentSearch::All
        };

        let node_id = node_id.unwrap_or_else(|| "default".to_string());
        let ipfs = ipfs.unwrap_or_else(|| vec!["https://api.thegraph.com/ipfs/".to_string()]);

        let current = current.unwrap_or_else(|| false);
        let pending = pending.unwrap_or_else(|| false);
        let status = status.unwrap_or_else(|| false);
        let used = used.unwrap_or_else(|| false);

        let node = match NodeId::new(&node_id) {
            Err(()) => {
                return Err(FieldError::new(
                    "Invalid node id value",
                    graphql_value!({ "invalid node id": node_id }),
                ))
            }
            Ok(node) => node,
        };

        let logger = logger_with_levels(false, None);
        let mut stores = BTreeMap::new();

        let mut opt = Opt::default();
        opt.postgres_url = Some(postgres_url);

        stores.insert(PRIMARY_SHARD.to_string(), Shard::from_opt(true, &opt)?);

        let config = Config {
            node: node.clone(),
            general: None,
            stores,
            chains: ChainSection::from_opt(&opt).unwrap(),
            deployment: Deployment::from_opt(&opt),
        };

        let fork_base = if fork_base.is_some() {
            Some(Url::from_str(&fork_base.unwrap()).unwrap())
        } else {
            None
        };

        let ctx =
            GraphmanContext::new(logger.clone(), node, config, ipfs, fork_base, version_label);

        let (primary, store) = if status {
            let (store, primary) = ctx.store_and_primary();
            (primary, Some(store))
        } else {
            (ctx.primary_pool(), None)
        };

        let res = core::info::run(primary, store, deployment, current, pending, used)?;

        println!("{:?}", res);
        Ok(res)
    }

    fn unassign(context: &GQLContext, id: String) -> FieldResult<SuccessMessage> {
        // core::assign::unassign(primary, sender, search);
        Ok(SuccessMessage {
            message: format!("Unassigned {} successfully!", id),
        })
    }
}
pub struct Mutation;

#[graphql_object(context = GQLContext)]
impl Mutation {
    fn create_something() -> FieldResult<String> {
        Ok(format!("Test"))
    }
}

// A root schema consists of a query, a mutation, and a subscription.
// Request queries can be executed against a RootNode.
pub type Schema = juniper::RootNode<'static, Query, Mutation, EmptySubscription<GQLContext>>;

pub fn create_schema() -> Schema {
    Schema::new(Query {}, Mutation {}, EmptySubscription::new())
}
