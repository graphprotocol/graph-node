use std::sync::Arc;
use std::time::Duration;

use graph::data::query::Trace;
use graph::prelude::anyhow;
use graph_core::graphman::core;
use graph_core::graphman::utils::PanicSubscriptionManager;
use graph_graphql::prelude::GraphQlRunner;
use graph_store_postgres::Store;

fn print_brief_trace(name: &str, trace: &Trace, indent: usize) -> Result<(), anyhow::Error> {
    use Trace::*;

    fn query_time(trace: &Trace) -> Duration {
        match trace {
            None => Duration::from_millis(0),
            Root { children, .. } => children
                .iter()
                .map(|(_, trace)| query_time(trace))
                .sum::<Duration>(),
            Query {
                elapsed, children, ..
            } => {
                *elapsed
                    + children
                        .iter()
                        .map(|(_, trace)| query_time(trace))
                        .sum::<Duration>()
            }
        }
    }

    match trace {
        None => { /* do nothing */ }
        Root {
            elapsed, children, ..
        } => {
            let elapsed = *elapsed.lock().unwrap();
            let qt = query_time(trace);
            let pt = elapsed - qt;

            println!(
                "{space:indent$}{name:rest$} {elapsed:7}ms",
                space = " ",
                indent = indent,
                rest = 48 - indent,
                name = name,
                elapsed = elapsed.as_millis(),
            );
            for (name, trace) in children {
                print_brief_trace(name, trace, indent + 2)?;
            }
            println!("\nquery:      {:7}ms", qt.as_millis());
            println!("other:      {:7}ms", pt.as_millis());
            println!("total:      {:7}ms", elapsed.as_millis())
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

pub async fn run(
    runner: Arc<GraphQlRunner<Store, PanicSubscriptionManager>>,
    target: String,
    query: String,
    vars: Vec<String>,
    output: Option<String>,
    trace: Option<String>,
) -> Result<(), anyhow::Error> {
    core::query::run(runner, target, query, vars, output, trace, |trace| {
        print_brief_trace("root", trace, 0)
    })
    .await
}
