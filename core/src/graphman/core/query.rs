use std::fs::File;
use std::io::Write;
use std::iter::FromIterator;
use std::{collections::HashMap, sync::Arc};

use graph::data::query::Trace;
use graph::prelude::r;
use graph::{
    data::query::QueryTarget,
    prelude::{
        anyhow::{self, anyhow},
        serde_json, DeploymentHash, GraphQlRunner as _, Query, QueryVariables, SubgraphName,
    },
};
use graph_graphql::prelude::GraphQlRunner;
use graph_store_postgres::Store;

use crate::graphman::utils::PanicSubscriptionManager;

pub async fn run<U>(
    runner: Arc<GraphQlRunner<Store, PanicSubscriptionManager>>,
    target: String,
    query: String,
    vars: Vec<String>,
    output: Option<String>,
    trace: Option<String>,
    on_trace: U,
) -> Result<(), anyhow::Error>
where
    U: Fn(&Trace) -> Result<(), anyhow::Error>,
{
    let target = if target.starts_with("Qm") {
        let id =
            DeploymentHash::new(target).map_err(|id| anyhow!("illegal deployment id `{}`", id))?;
        QueryTarget::Deployment(id, Default::default())
    } else {
        let name = SubgraphName::new(target.clone())
            .map_err(|()| anyhow!("illegal subgraph name `{}`", target))?;
        QueryTarget::Name(name, Default::default())
    };

    let document = graphql_parser::parse_query(&query)?.into_static();
    let vars: Vec<(String, r::Value)> = vars
        .into_iter()
        .map(|v| {
            let mut pair = v.splitn(2, '=').map(|s| s.to_string());
            let key = pair.next();
            let value = pair.next().map(r::Value::String).unwrap_or(r::Value::Null);
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
        true,
    );

    let res = runner.run_query(query, target).await;
    if let Some(err) = res.errors().first().cloned() {
        return Err(err.into());
    }

    if let Some(output) = output {
        let mut f = File::create(output)?;
        let json = serde_json::to_string(&res)?;
        writeln!(f, "{}", json)?;
    }

    // The format of this file is pretty awful, but good enough to fish out
    // interesting SQL queries
    if let Some(trace) = trace {
        let mut f = File::create(trace)?;
        let json = serde_json::to_string(&res.traces())?;
        writeln!(f, "{}", json)?;
    }

    for trace in res.traces() {
        on_trace(trace)?;
    }
    Ok(())
}
