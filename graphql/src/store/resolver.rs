use graphql_parser::{query as q, schema as s};
use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::result;
use std::sync::Arc;

use graph::components::store::*;
use graph::data::subgraph::schema::SubgraphDeploymentEntity;
use graph::prelude::*;

use prelude::*;
use query::ast as qast;
use schema::ast as sast;
use store::query::{collect_entities_from_query_field, parse_subgraph_id};

/// A resolver that fetches entities from a `Store`.
pub struct StoreResolver<S> {
    logger: Logger,
    store: Arc<S>,
}

impl<S> Clone for StoreResolver<S>
where
    S: Store,
{
    fn clone(&self) -> Self {
        StoreResolver {
            logger: self.logger.clone(),
            store: self.store.clone(),
        }
    }
}

impl<S> StoreResolver<S>
where
    S: Store,
{
    pub fn new(logger: &Logger, store: Arc<S>) -> Self {
        StoreResolver {
            logger: logger.new(o!("component" => "StoreResolver")),
            store,
        }
    }

    /// If the field has a `@derivedFrom(field: "foo")` directive, obtain the
    /// name of the field (e.g. `"foo"`)
    fn get_derived_from_directive(field_definition: &s::Field) -> Option<&s::Directive> {
        field_definition
            .directives
            .iter()
            .find(|directive| directive.name == s::Name::from("derivedFrom"))
    }

    /// Adds a filter for matching entities that correspond to a derived field.
    ///
    /// Returns true if the field is a derived field (i.e., if it is defined with
    /// a @derivedFrom directive).
    fn add_filter_for_derived_field(
        query: &mut EntityQuery,
        parent: &Option<q::Value>,
        field_definition: &s::Field,
        object_type: ObjectOrInterface,
    ) -> bool {
        let derived_from_field = Self::get_derived_from_directive(field_definition)
            .and_then(|directive| {
                qast::get_argument_value(&directive.arguments, &q::Name::from("field"))
            })
            .and_then(|value| match value {
                q::Value::String(s) => Some(s),
                _ => None,
            })
            .and_then(|derived_from_field_name| {
                sast::get_field_type(object_type, derived_from_field_name)
            });

        if let Some(derived_from_field) = derived_from_field {
            // This field is derived from a field in the object type that we're trying
            // to resolve values for; e.g. a `bandMembers` field maybe be derived from
            // a `bands` or `band` field in a `Musician` type.
            //
            // Our goal here is to identify the ID of the parent entity (e.g. the ID of
            // a band) and add a `Contains("bands", <id>)` or `Equal("band", <id>)` filter
            // to the arguments

            let field_name = derived_from_field.name.clone();

            // To achieve this, we first identify the parent ID
            let parent_id = parent
                .as_ref()
                .and_then(|value| match value {
                    q::Value::Object(o) => Some(o),
                    _ => None,
                })
                .and_then(|object| object.get(&q::Name::from("id")))
                .and_then(|value| match value {
                    q::Value::String(s) => Some(Value::from(s)),
                    _ => None,
                })
                .expect("Parent object is missing an \"id\"")
                .clone();

            // Depending on whether the field we're deriving from has a list or a
            // single value type, we either create a `Contains` or `Equal`
            // filter argument
            let filter = match derived_from_field.field_type {
                s::Type::ListType(_) => EntityFilter::Contains(field_name, parent_id),
                s::Type::NonNullType(ref inner) => match inner.deref() {
                    s::Type::ListType(_) => EntityFilter::Contains(field_name, parent_id),
                    _ => EntityFilter::Equal(field_name, parent_id),
                },
                _ => EntityFilter::Equal(field_name, parent_id),
            };

            // Add the `Contains`/`Equal` filter to the top-level `And` filter, creating one
            // if necessary
            let top_level_filter = query.filter.get_or_insert(EntityFilter::And(vec![]));
            *top_level_filter = match top_level_filter {
                EntityFilter::And(ref mut filters) => {
                    let mut filters = filters.clone();
                    filters.push(filter);
                    EntityFilter::And(filters)
                }
                _ => top_level_filter.clone(),
            };

            true
        } else {
            false
        }
    }

    /// Adds a filter for matching entities that are referenced by the given field.
    fn add_filter_for_reference_field(
        query: &mut EntityQuery,
        parent: &Option<q::Value>,
        field_definition: &s::Field,
        _object_type: ObjectOrInterface,
    ) {
        if let Some(q::Value::Object(object)) = parent {
            // Create an `Or(Equals("id", ref_id1), ...)` filter that includes
            // all referenced IDs.
            let filter = object
                .get(&field_definition.name)
                .and_then(|value| match value {
                    q::Value::String(id) => {
                        Some(EntityFilter::Equal(String::from("id"), Value::from(id)))
                    }
                    q::Value::List(ids) => Some(EntityFilter::Or(
                        ids.iter()
                            .filter_map(|id| match id {
                                q::Value::String(s) => Some(s),
                                _ => None,
                            })
                            .map(|id| EntityFilter::Equal(String::from("id"), Value::from(id)))
                            .collect(),
                    )),
                    _ => None,
                })
                .unwrap_or_else(|| {
                    // Unreachable, caught by `UnknownField` error.
                    panic!(
                        "Field \"{}\" missing in parent object",
                        field_definition.name
                    )
                });

            // Add the `Or` filter to the top-level `And` filter, creating one if necessary
            let top_level_filter = query.filter.get_or_insert(EntityFilter::And(vec![]));
            *top_level_filter = match top_level_filter {
                EntityFilter::And(ref mut filters) => {
                    let mut filters = filters.clone();
                    filters.push(filter);
                    EntityFilter::And(filters)
                }
                _ => top_level_filter.clone(),
            };
        }
    }

    /// Returns true if the object has no references in the given field.
    fn references_field_is_empty(parent: &Option<q::Value>, field: &q::Name) -> bool {
        parent
            .as_ref()
            .and_then(|value| match value {
                q::Value::Object(object) => Some(object),
                _ => None,
            })
            .and_then(|object| object.get(field))
            .map(|value| match value {
                q::Value::List(values) => values.is_empty(),
                _ => true,
            })
            .unwrap_or(true)
    }

    /// Compute special fields that are not stored such as as `entityCount`.
    fn add_computed_fields(
        &self,
        mut entity: Entity,
        object_type: ObjectOrInterface,
    ) -> Result<Entity, QueryExecutionError> {
        let subgraph_deployment_entity_pair = SubgraphDeploymentEntity::subgraph_entity_pair();
        if (
            parse_subgraph_id(object_type)?,
            object_type.name().to_owned(),
        ) == subgraph_deployment_entity_pair
        {
            let id = entity
                .id()
                .expect("subgraph deployment entity should have ID");
            let hash = SubgraphDeploymentId::new(id).expect("invalid subgraph ID in database");
            entity.insert(
                "entityCount".to_owned(),
                self.store
                    .count_entities(hash)
                    .map_err(QueryExecutionError::StoreError)?
                    .into(),
            );
        }
        Ok(entity)
    }
}

impl<S> Resolver for StoreResolver<S>
where
    S: Store,
{
    fn resolve_objects(
        &self,
        parent: &Option<q::Value>,
        _field: &q::Name,
        field_definition: &s::Field,
        object_type: ObjectOrInterface<'_>,
        arguments: &HashMap<&q::Name, q::Value>,
        types_for_interface: &BTreeMap<Name, Vec<ObjectType>>,
    ) -> Result<q::Value, QueryExecutionError> {
        let object_type = object_type.into();
        let mut query = build_query(object_type, arguments, types_for_interface)?;

        // Add matching filter for derived fields
        let is_derived =
            Self::add_filter_for_derived_field(&mut query, parent, field_definition, object_type);

        // Return an empty list if we're dealing with a non-derived field that
        // holds an empty list of references; there's no point in querying the store
        // if the result will be empty anyway
        if !is_derived
            && parent.is_some()
            && Self::references_field_is_empty(parent, &field_definition.name)
        {
            return Ok(q::Value::List(vec![]));
        }

        // Add matching filter for reference fields
        if !is_derived {
            Self::add_filter_for_reference_field(&mut query, parent, field_definition, object_type);
        }

        let mut entity_values = Vec::new();
        for entity in self.store.find(query)? {
            entity_values.push(self.add_computed_fields(entity, object_type)?.into())
        }
        Ok(q::Value::List(entity_values))
    }

    fn resolve_object(
        &self,
        parent: &Option<q::Value>,
        field: &q::Name,
        object_type: ObjectOrInterface<'_>,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> Result<q::Value, QueryExecutionError> {
        let id = arguments.get(&"id".to_string()).and_then(|id| match id {
            q::Value::String(s) => Some(s),
            _ => None,
        });

        // subgraph_id directive is injected in all types.
        let subgraph_id = parse_subgraph_id(object_type).unwrap();
        let entity = if let Some(id) = id {
            self.store.get(EntityKey {
                subgraph_id: subgraph_id.clone(),
                entity_type: object_type.name().to_owned(),
                entity_id: id.to_owned(),
            })?
        } else {
            match parent {
                Some(q::Value::Object(parent_object)) => match parent_object.get(field) {
                    Some(q::Value::String(id)) => self.store.get(EntityKey {
                        subgraph_id: subgraph_id.clone(),
                        entity_type: object_type.name().to_owned(),
                        entity_id: id.to_owned(),
                    })?,
                    _ => None,
                },
                _ => panic!("top level queries must either take an `id` or return a list"),
            }
        };

        Ok(match entity {
            Some(entity) => self.add_computed_fields(entity, object_type)?.into(),
            None => q::Value::Null,
        })
    }

    fn resolve_field_stream<'a, 'b>(
        &self,
        schema: &'a s::Document,
        object_type: &'a s::ObjectType,
        field: &'b q::Field,
    ) -> result::Result<StoreEventStreamBox, QueryExecutionError> {
        // Fail if the field does not exist on the object type
        if sast::get_field_type(object_type, &field.name).is_none() {
            return Err(QueryExecutionError::UnknownField(
                field.position,
                object_type.name.clone(),
                field.name.clone(),
            ));
        }

        // Collect all entities involved in the query field
        let entities = collect_entities_from_query_field(schema, object_type, field);

        // Subscribe to the store and return the entity change stream
        let deployment_id = parse_subgraph_id(object_type)?;
        Ok(self.store.subscribe(entities).throttle_while_syncing(
            &self.logger,
            self.store.clone(),
            deployment_id,
            *SUBSCRIPTION_THROTTLE_INTERVAL,
        ))
    }
}
