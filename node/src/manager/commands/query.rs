use std::fs::File;
use std::io::Write;
use std::iter::FromIterator;
use std::{collections::HashMap, sync::Arc};

use graph::data::query::Trace;
use graph::log::escape_control_chars;
use graph::prelude::{q, r};
use graph::{
    data::query::QueryTarget,
    prelude::{
        anyhow::{self, anyhow},
        serde_json, DeploymentHash, GraphQlRunner as _, Query, QueryVariables, SubgraphName,
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
    output: Option<String>,
    trace: Option<String>,
) -> Result<(), anyhow::Error> {
    let target = if target.starts_with("Qm") {
        let id =
            DeploymentHash::new(target).map_err(|id| anyhow!("illegal deployment id `{}`", id))?;
        QueryTarget::Deployment(id, Default::default())
    } else {
        let name = SubgraphName::new(target.clone())
            .map_err(|()| anyhow!("illegal subgraph name `{}`", target))?;
        QueryTarget::Name(name, Default::default())
    };

    let document = q::parse_query(&query)?.into_static();
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

        // Escape control characters in the query output, as a precaution against injecting control
        // characters in a terminal.
        let json = escape_control_chars(serde_json::to_string(&res)?);
        writeln!(f, "{}", json)?;
    }

    // The format of this file is pretty awful, but good enough to fish out
    // interesting SQL queries
    if let Some(trace) = trace {
        let mut f = File::create(trace)?;
        let json = serde_json::to_string(&res.trace)?;
        writeln!(f, "{}", json)?;
    }

    print_brief_trace("root", &res.trace, 0)?;

    Ok(())
}

fn print_brief_trace(name: &str, trace: &Trace, indent: usize) -> Result<(), anyhow::Error> {
    use Trace::*;

    match trace {
        None => { /* do nothing */ }
        Root {
            elapsed,
            setup,
            blocks: children,
            ..
        } => {
            let elapsed = *elapsed;
            let qt = trace.query_total();
            let pt = elapsed - qt.elapsed;

            println!(
                "{space:indent$}{name:rest$} {setup:7}ms {elapsed:7}ms",
                space = " ",
                indent = indent,
                rest = 48 - indent,
                name = name,
                setup = setup.as_millis(),
                elapsed = elapsed.as_millis(),
            );
            for twc in children {
                print_brief_trace(name, &twc.trace, indent + 2)?;
            }
            println!("\nquery:      {:7}ms", qt.elapsed.as_millis());
            println!("other:      {:7}ms", pt.as_millis());
            println!("total:      {:7}ms", elapsed.as_millis())
        }
        Block { children, .. } => {
            for (name, trace) in children {
                print_brief_trace(name, trace, indent + 2)?;
            }
        }

        Query {
            elapsed,
            entity_count,
            children,
            ..
        } => {
            println!(
                "{space:indent$}{name:rest$} {elapsed:7}ms [{count:7} entities]",
                space = " ",
                indent = indent,
                rest = 50 - indent,
                name = name,
                elapsed = elapsed.as_millis(),
                count = entity_count
            );
            for (name, trace) in children {
                print_brief_trace(name, trace, indent + 2)?;
            }
        }
    }

    Ok(())
}
