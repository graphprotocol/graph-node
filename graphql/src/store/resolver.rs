use graphql_parser::{query as q, schema as s};
use std::collections::{BTreeMap, HashMap};
use std::result;
use std::sync::Arc;

use graph::components::store::*;
use graph::prelude::*;

use crate::prelude::*;
use crate::query::ast as qast;
use crate::query::ext::BlockConstraint;
use crate::schema::ast as sast;

use crate::store::query::{collect_entities_from_query_field, parse_subgraph_id};

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

    /// Adds a filter for matching entities that correspond to a derived field.
    ///
    /// Returns true if the field is a derived field (i.e., if it is defined with
    /// a @derivedFrom directive).
    fn add_filter_for_derived_field(
        query: &mut EntityQuery,
        parent: &Option<q::Value>,
        derived_from_field: &s::Field,
    ) {
        // This field is derived from a field in the object type that we're trying
        // to resolve values for; e.g. a `bandMembers` field maybe be derived from
        // a `bands` or `band` field in a `Musician` type.
        //
        // Our goal here is to identify the ID of the parent entity (e.g. the ID of
        // a band) and add a `Contains("bands", [<id>])` or `Equal("band", <id>)`
        // filter to the arguments.

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
        let filter = if sast::is_list_or_non_null_list_field(derived_from_field) {
            EntityFilter::Contains(field_name, Value::List(vec![parent_id]))
        } else {
            EntityFilter::Equal(field_name, parent_id)
        };

        // Add the `Contains`/`Equal` filter to the top-level `And` filter, creating one
        // if necessary
        let top_level_filter = query.filter.get_or_insert(EntityFilter::And(vec![]));
        match top_level_filter {
            EntityFilter::And(ref mut filters) => {
                filters.push(filter);
            }
            _ => unreachable!("top level filter is always `And`"),
        };
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
                    // Caught by `UnknownField` error.
                    unreachable!(
                        "Field \"{}\" missing in parent object",
                        field_definition.name
                    )
                });

            // Add the `Or` filter to the top-level `And` filter, creating one if necessary
            let top_level_filter = query.filter.get_or_insert(EntityFilter::And(vec![]));
            match top_level_filter {
                EntityFilter::And(ref mut filters) => {
                    filters.push(filter);
                }
                _ => unreachable!("top level filter is always `And`"),
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

    fn get_prefetched_child<'a>(
        parent: &'a Option<q::Value>,
        field: &q::Field,
    ) -> Option<&'a q::Value> {
        match parent {
            Some(q::Value::Object(map)) => {
                let key = format!("prefetch:{}", qast::get_response_key(field));
                map.get(&key)
            }
            _ => None,
        }
    }

    fn was_prefetched(parent: &Option<q::Value>) -> bool {
        match parent {
            Some(q::Value::Object(map)) => map.contains_key(super::prefetch::PREFETCH_KEY),
            _ => false,
        }
    }

    fn resolve_objects_prefetch(
        &self,
        parent: &Option<q::Value>,
        field: &q::Field,
        object_type: ObjectOrInterface<'_>,
    ) -> Result<q::Value, QueryExecutionError> {
        if let Some(child) = Self::get_prefetched_child(parent, field) {
            Ok(child.clone())
        } else {
            Err(QueryExecutionError::ResolveEntitiesError(format!(
                "internal error resolving {}.{} for {:?}: \
                 expected prefetched result, but found nothing",
                object_type.name(),
                &field.name,
                parent
            )))
        }
    }

    fn resolve_object_prefetch(
        &self,
        parent: &Option<q::Value>,
        field: &q::Field,
        field_definition: &s::Field,
        object_type: ObjectOrInterface<'_>,
    ) -> Result<q::Value, QueryExecutionError> {
        if let Some(q::Value::List(children)) = Self::get_prefetched_child(parent, field) {
            if children.len() > 1 {
                let derived_from_field =
                    sast::get_derived_from_field(object_type, field_definition)
                        .expect("only derived fields can lead to multiple children here");

                return Err(QueryExecutionError::AmbiguousDerivedFromResult(
                    field.position.clone(),
                    field.name.to_owned(),
                    object_type.name().to_owned(),
                    derived_from_field.name.to_owned(),
                ));
            } else {
                return Ok(children
                    .into_iter()
                    .next()
                    .map(|value| value.clone())
                    .unwrap_or(q::Value::Null));
            }
        } else {
            return Err(QueryExecutionError::ResolveEntitiesError(format!(
                "internal error resolving {}.{} for {:?}: \
                 expected prefetched result, but found nothing",
                object_type.name(),
                &field.name,
                parent
            )));
        }
    }
}

impl<S> Resolver for StoreResolver<S>
where
    S: Store,
{
    fn prefetch<'r>(
        &self,
        ctx: &ExecutionContext<'r, Self>,
        selection_set: &q::SelectionSet,
    ) -> Result<Option<q::Value>, Vec<QueryExecutionError>> {
        super::prefetch::run(ctx, selection_set, self.store.clone()).map(|value| Some(value))
    }

    fn locate_block(&self, bc: &BlockConstraint) -> Result<BlockNumber, QueryExecutionError> {
        match bc.block {
            BlockLocator::Number(number) => Ok(number),
            BlockLocator::Hash(hash) => self
                .store
                .block_number(&bc.subgraph, hash)
                .map_err(|e| e.into())
                .and_then(|number| {
                    number.ok_or_else(|| {
                        QueryExecutionError::ValueParseError(
                            "block.hash".to_owned(),
                            "no block with that hash found".to_owned(),
                        )
                    })
                }),
        }
    }

    fn resolve_objects(
        &self,
        parent: &Option<q::Value>,
        field: &q::Field,
        field_definition: &s::Field,
        object_type: ObjectOrInterface<'_>,
        arguments: &HashMap<&q::Name, q::Value>,
        types_for_interface: &BTreeMap<Name, Vec<ObjectType>>,
        block: BlockNumber,
        max_first: u32,
    ) -> Result<q::Value, QueryExecutionError> {
        if Self::was_prefetched(parent) {
            return self.resolve_objects_prefetch(parent, field, object_type);
        }

        let object_type = object_type.into();
        let mut query = build_query(
            object_type,
            block,
            arguments,
            types_for_interface,
            max_first,
        )?;

        // Add matching filter for derived fields
        let derived_from_field = sast::get_derived_from_field(object_type, field_definition);
        let is_derived = derived_from_field.is_some();
        if let Some(derived_from_field) = derived_from_field {
            Self::add_filter_for_derived_field(&mut query, parent, derived_from_field);
        }

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
            entity_values.push(entity.into())
        }
        Ok(q::Value::List(entity_values))
    }

    fn resolve_object(
        &self,
        parent: &Option<q::Value>,
        field: &q::Field,
        field_definition: &s::Field,
        object_type: ObjectOrInterface<'_>,
        arguments: &HashMap<&q::Name, q::Value>,
        types_for_interface: &BTreeMap<Name, Vec<ObjectType>>,
        block: BlockNumber,
    ) -> Result<q::Value, QueryExecutionError> {
        if Self::was_prefetched(parent) {
            return self.resolve_object_prefetch(parent, field, field_definition, object_type);
        }

        let id = arguments.get(&"id".to_string()).and_then(|id| match id {
            q::Value::String(s) => Some(s),
            _ => None,
        });

        // The subgraph_id directive is injected in all types.
        let subgraph_id = parse_subgraph_id(object_type).unwrap();
        let subgraph_id_for_resolve_object = subgraph_id.clone();

        let resolve_object_with_id = |id: &String| -> Result<Option<Entity>, QueryExecutionError> {
            let collection = match object_type {
                ObjectOrInterface::Object(_) => {
                    EntityCollection::All(vec![object_type.name().to_owned()])
                }
                ObjectOrInterface::Interface(interface) => {
                    let entity_types = types_for_interface[&interface.name]
                        .iter()
                        .map(|o| o.name.clone())
                        .collect();
                    EntityCollection::All(entity_types)
                }
            };
            let query = EntityQuery::new(subgraph_id_for_resolve_object, block, collection)
                .filter(EntityFilter::Equal(String::from("id"), Value::from(id)))
                .first(1);
            Ok(self.store.find(query)?.into_iter().next())
        };

        let entity = if let Some(id) = id {
            resolve_object_with_id(id)?
        } else {
            // Identify whether the field is derived with @derivedFrom
            let derived_from_field = sast::get_derived_from_field(object_type, field_definition);
            if let Some(derived_from_field) = derived_from_field {
                // The field is derived -> build a query for the entity that might be
                // referencing the parent object

                let mut arguments = arguments.clone();

                // We use first: 2 here to detect and fail if there is more than one
                // entity that matches the `@derivedFrom`.
                let first_arg_name = q::Name::from("first");
                arguments.insert(&first_arg_name, q::Value::Int(q::Number::from(2)));

                let skip_arg_name = q::Name::from("skip");
                arguments.insert(&skip_arg_name, q::Value::Int(q::Number::from(0)));
                let mut query =
                    build_query(object_type, block, &arguments, types_for_interface, 2)?;
                Self::add_filter_for_derived_field(&mut query, parent, derived_from_field);

                // Find the entity or entities that reference the parent entity
                let entities = self.store.find(query)?;

                if entities.len() > 1 {
                    return Err(QueryExecutionError::AmbiguousDerivedFromResult(
                        field.position.clone(),
                        field.name.to_owned(),
                        object_type.name().to_owned(),
                        derived_from_field.name.to_owned(),
                    ));
                } else {
                    entities.into_iter().next()
                }
            } else {
                match parent {
                    Some(q::Value::Object(parent_object)) => match parent_object.get(&field.name) {
                        Some(q::Value::String(id)) => resolve_object_with_id(id)?,
                        _ => None,
                    },
                    _ => panic!("top level queries must either take an `id` or return a list"),
                }
            }
        };

        Ok(entity.map_or(q::Value::Null, Into::into))
    }

    fn resolve_field_stream<'a, 'b>(
        &self,
        schema: &'a s::Document,
        object_type: &'a s::ObjectType,
        field: &'b q::Field,
    ) -> result::Result<StoreEventStreamBox, QueryExecutionError> {
        // Fail if the field does not exist on the object type
        if sast::get_field(object_type, &field.name).is_none() {
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
