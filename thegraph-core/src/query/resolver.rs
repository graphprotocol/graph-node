use futures::prelude::*;
use futures::sync::mpsc::Sender;
use futures::sync::oneshot;
use graphql_parser::query as gqlq;
use graphql_parser::schema as gqls;
use slog;
use std::collections::HashMap;

use store::query::build_query;
use thegraph::components::store::*;
use thegraph_graphql_utils::Resolver as ResolverTrait;

/// A resolver that fetches entities from a `Store`.
#[derive(Clone)]
pub struct StoreResolver {
    logger: slog::Logger,
    store_requests: Sender<StoreRequest>,
}

impl StoreResolver {
    pub fn new(logger: &slog::Logger, store_requests: Sender<StoreRequest>) -> Self {
        StoreResolver {
            logger: logger.new(o!("component" => "StoreResolver")),
            store_requests,
        }
    }
}

impl ResolverTrait for StoreResolver {
    fn resolve_entities(
        &self,
        _parent: &Option<gqlq::Value>,
        entity: &gqlq::Name,
        arguments: &HashMap<&gqlq::Name, gqlq::Value>,
    ) -> gqlq::Value {
        // Prepare a store request
        let (result_sender, result_receiver) = oneshot::channel();
        let store_query = build_query(entity, arguments);
        let request = StoreRequest::Find(store_query, result_sender);

        // Send store request
        self.store_requests.clone().send(request).wait().unwrap();

        // Handle the a response
        result_receiver
            .wait()
            .map(|result| {
                result
                    .map(|entities| {
                        gqlq::Value::List(
                            entities
                                .into_iter()
                                .map(|e| e.into())
                                .collect::<Vec<gqlq::Value>>(),
                        )
                    })
                    .unwrap_or(gqlq::Value::Null)
            })
            .unwrap_or(gqlq::Value::Null)
    }

    fn resolve_entity(
        &self,
        _parent: &Option<gqlq::Value>,
        entity: &gqlq::Name,
        _arguments: &HashMap<&gqlq::Name, gqlq::Value>,
    ) -> gqlq::Value {
        // Prepare a store request
        let (result_sender, result_receiver) = oneshot::channel();
        let store_key = StoreKey {
            entity: entity.to_owned(),
            id: "1".to_string(),
        };
        let request = StoreRequest::Get(store_key, result_sender);

        // Send store request
        self.store_requests.clone().send(request).wait().unwrap();

        // Handle the a response
        result_receiver
            .wait()
            .map(|result| {
                result
                    .map(|entity| entity.into())
                    .unwrap_or(gqlq::Value::Null)
            })
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
