use std::iter::FromIterator;
use std::{collections::HashMap, sync::Arc};

use graph::{
    data::query::QueryTarget,
    prelude::{
        anyhow::{self, anyhow},
        q, serde_json, DeploymentHash, GraphQlRunner as _, Query, QueryVariables, SubgraphName,
    },
};
use graph_graphql::prelude::GraphQlRunner;
use graph_store_postgres::Store;

use crate::manager::PanicSubscriptionManager;

pub async fn run(
    runner: Arc<GraphQlRunner<Store, PanicSubscriptionManager>>,
    target: String,
    query: String,
    vars: Vec<String>,
) -> Result<(), anyhow::Error> {
    let target = if target.starts_with("Qm") {
        let id =
            DeploymentHash::new(target).map_err(|id| anyhow!("illegal deployment id `{}`", id))?;
        QueryTarget::Deployment(id)
    } else {
        let name = SubgraphName::new(target.clone())
            .map_err(|()| anyhow!("illegal subgraph name `{}`", target))?;
        QueryTarget::Name(name)
    };

    let document = graphql_parser::parse_query(&query)?.into_static();
    let vars: Vec<(String, q::Value)> = vars
        .into_iter()
        .map(|v| {
            let mut pair = v.splitn(2, '=').map(|s| s.to_string());
            let key = pair.next();
            let value = pair
                .next()
                .map(|s| q::Value::String(s))
                .unwrap_or(q::Value::Null);
            match key {
                Some(key) => Ok((key, value)),
                None => Err(anyhow!(
                    "malformed variable `{}`, it must be of the form `key=value`",
                    v
                )),
            }
        })
        .collect::<Result<_, _>>()?;
    let query = Query::new(
        document,
        Some(QueryVariables::new(HashMap::from_iter(vars))),
    );

    let res = runner.run_query(query, target, false).await;
    let json = serde_json::to_string(&res)?;
    println!("{}", json);

    Ok(())
}
