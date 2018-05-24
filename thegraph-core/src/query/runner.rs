use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use graphql_parser::query as gqlq;
use graphql_parser::schema as gqls;
use slog;
use std::sync::Arc;
use tokio_core::reactor::Handle;

use thegraph::components::store::*;
use thegraph::prelude::{Query, QueryRunner as QueryRunnerTrait, Store};

use super::execution;

/// A resolver that fetches entities from a `Store`.
#[derive(Clone)]
struct StoreResolver {
    store: Arc<Store>,
}

impl StoreResolver {
    pub fn new(store: Arc<Store>) -> Self {
        StoreResolver { store }
    }
}

impl execution::Resolver for StoreResolver {
    fn resolve_entities(
        &self,
        _parent: &Option<gqlq::Value>,
        entity: &String,
        _arguments: &Vec<&(gqlq::Name, gqlq::Value)>,
    ) -> gqlq::Value {
        self.store
            .find(StoreQuery {
                entity: entity.to_owned(),
                filters: vec![],
                order_by: None,
                order_direction: None,
                range: None,
            })
            .map(|entities| {
                gqlq::Value::List(
                    entities
                        .into_iter()
                        .map(|e| e.into())
                        .collect::<Vec<gqlq::Value>>(),
                )
            })
            .unwrap_or(gqlq::Value::Null)
    }

    fn resolve_entity(
        &self,
        _parent: &Option<gqlq::Value>,
        entity: &String,
        _arguments: &Vec<&(gqlq::Name, gqlq::Value)>,
    ) -> gqlq::Value {
        self.store
            .get(StoreKey {
                entity: entity.to_owned(),
                id: "1".to_string(),
            })
            .map(|entity| entity.into())
            .unwrap_or(gqlq::Value::Null)
    }

    fn resolve_enum_value(
        &self,
        enum_type: &gqls::EnumType,
        value: Option<&gqlq::Value>,
    ) -> gqlq::Value {
        match value {
            Some(gqlq::Value::Enum(name)) => enum_type
                .values
                .iter()
                .find(|enum_value| &enum_value.name == name)
                .map_or(gqlq::Value::Null, |enum_value| {
                    gqlq::Value::Enum(enum_value.name.clone())
                }),
            _ => gqlq::Value::Null,
        }
    }

    fn resolve_scalar_value(
        &self,
        _scalar_type: &gqls::ScalarType,
        value: Option<&gqlq::Value>,
    ) -> gqlq::Value {
        value
            .map(|value| value.clone())
            .unwrap_or(gqlq::Value::Null)
    }

    fn resolve_enum_values(
        &self,
        enum_type: &gqls::EnumType,
        value: Option<&gqlq::Value>,
    ) -> gqlq::Value {
        match value {
            Some(gqlq::Value::List(values)) => gqlq::Value::List(
                values
                    .iter()
                    .map(|value| self.resolve_enum_value(enum_type, Some(value)))
                    .collect::<Vec<gqlq::Value>>(),
            ),
            _ => gqlq::Value::Null,
        }
    }

    fn resolve_scalar_values(
        &self,
        scalar_type: &gqls::ScalarType,
        value: Option<&gqlq::Value>,
    ) -> gqlq::Value {
        match value {
            Some(gqlq::Value::List(values)) => gqlq::Value::List(
                values
                    .iter()
                    .map(|value| self.resolve_scalar_value(scalar_type, Some(value)))
                    .collect::<Vec<gqlq::Value>>(),
            ),
            _ => gqlq::Value::Null,
        }
    }

    fn resolve_abstract_type<'a>(
        &self,
        _schema: &'a gqls::Document,
        _abstract_type: &gqls::TypeDefinition,
        _object_value: &gqlq::Value,
    ) -> Option<&'a gqls::ObjectType> {
        None
    }
}

/// Common query runner implementation for The Graph.
pub struct QueryRunner<S> {
    logger: slog::Logger,
    query_sink: Sender<Query>,
    store: Arc<S>,
    runtime: Handle,
}

impl<S> QueryRunner<S>
where
    S: Store + Sized + 'static,
{
    /// Creates a new query runner.
    pub fn new(logger: &slog::Logger, runtime: Handle, store: S) -> Self {
        let (sink, stream) = channel(100);
        let runner = QueryRunner {
            logger: logger.new(o!("component" => "QueryRunner")),
            query_sink: sink,
            store: Arc::new(store),
            runtime,
        };
        runner.run_queries(stream);
        runner
    }

    /// Spawns a Tokio task to run any queries received through the given stream.
    fn run_queries(&self, stream: Receiver<Query>) {
        info!(self.logger, "Preparing to run queries");

        let logger = self.logger.clone();
        let store = self.store.clone();

        self.runtime.spawn(stream.for_each(move |query| {
            let options = execution::ExecutionOptions {
                logger: logger.clone(),
                resolver: StoreResolver::new(store.clone()),
            };
            let result = execution::execute(&query, options);

            query
                .result_sender
                .send(result)
                .expect("Failed to deliver query result");
            Ok(())
        }));
    }
}

impl<S> QueryRunnerTrait for QueryRunner<S> {
    fn query_sink(&mut self) -> Sender<Query> {
        self.query_sink.clone()
    }
}
