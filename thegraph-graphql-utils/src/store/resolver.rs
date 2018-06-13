use graphql_parser::query as q;
use slog;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use store::query::build_query;
use thegraph::components::store::*;
use thegraph::prelude::BasicStore;

use resolver::Resolver as ResolverTrait;

/// A resolver that fetches entities from a `Store`.
#[derive(Clone)]
pub struct StoreResolver {
    logger: slog::Logger,
    store: Arc<Mutex<BasicStore>>,
}

impl StoreResolver {
    pub fn new(logger: &slog::Logger, store: Arc<Mutex<BasicStore>>) -> Self {
        StoreResolver {
            logger: logger.new(o!("component" => "StoreResolver")),
            store,
        }
    }
}

impl ResolverTrait for StoreResolver {
    fn resolve_entities(
        &self,
        _parent: &Option<q::Value>,
        _field: &q::Name,
        entity: &q::Name,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> q::Value {
        let store = self.store.lock().unwrap();
        store
            .find(build_query(entity, arguments))
            .map(|entities| {
                q::Value::List(
                    entities
                        .into_iter()
                        .map(|e| e.into())
                        .collect::<Vec<q::Value>>(),
                )
            })
            .unwrap_or(q::Value::Null)
    }

    fn resolve_entity(
        &self,
        parent: &Option<q::Value>,
        field: &q::Name,
        entity: &q::Name,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> q::Value {
        let id = arguments.get(&"id".to_string()).and_then(|id| match id {
            q::Value::String(s) => Some(s),
            _ => None,
        });

        if let Some(id) = id {
            let store = self.store.lock().unwrap();
            return store
                .get(StoreKey {
                    entity: entity.to_owned(),
                    id: id.to_owned(),
                })
                .map(|entity| entity.into())
                .unwrap_or(q::Value::Null);
        }

        debug!(self.logger, "Parent: {:#?}", parent);
        debug!(self.logger, "Field: {}", field);
        debug!(self.logger, "Entity: {}", entity);

        match parent {
            Some(q::Value::Object(parent_object)) => match parent_object.get(field) {
                Some(q::Value::String(id)) => self.store
                    .lock()
                    .unwrap()
                    .get(StoreKey {
                        entity: entity.to_owned(),
                        id: id.to_owned(),
                    })
                    .map(|entity| entity.into())
                    .unwrap_or(q::Value::Null),
                _ => q::Value::Null,
            },
            _ => {
                let mut query = build_query(entity, arguments);
                query.range = Some(StoreRange { first: 1, skip: 0 });
                self.store
                    .lock()
                    .unwrap()
                    .find(query)
                    .map(|entities| {
                        entities
                            .into_iter()
                            .next()
                            .map(|entity| entity.into())
                            .unwrap_or(q::Value::Null)
                    })
                    .unwrap_or(q::Value::Null)
            }
        }
    }
}
