//! Run a GraphQL query and fetch all the entitied needed to build the
//! final result

use graphql_parser::query as q;
use graphql_parser::schema as s;
use lazy_static::lazy_static;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Deref;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use graph::prelude::{Entity, EntityFilter, QueryExecutionError, Store, Value as StoreValue};

use crate::execution::{ExecutionContext, ObjectOrInterface, Resolver};
use crate::query::ast as qast;
use crate::schema::ast as sast;
use crate::store::build_query;

lazy_static! {
    static ref ARG_FIRST: String = String::from("first");
    static ref ARG_SKIP: String = String::from("skip");
    static ref ARG_ID: String = String::from("id");
}

pub const PREFETCH_KEY: &str = ":prefetch";

/// Similar to the TypeCondition from graphql_parser, but with
/// derives that make it possible to use it as the key in a HashMap
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum TypeCondition {
    Any,
    On(q::Name),
}

impl TypeCondition {
    /// Return a `TypeCondition` that matches when `self` and `other` match
    /// simultaneously. If the two conditions can never match at the same time,
    /// return `None`
    fn and(&self, other: &Self) -> Option<Self> {
        use TypeCondition::*;
        match (self, other) {
            (Any, _) => Some(other.clone()),
            (_, Any) => Some(self.clone()),
            (On(name1), On(name2)) if name1 == name2 => Some(self.clone()),
            _ => None,
        }
    }

    /// Return `true` if any of the entities matches this type condition
    fn matches(&self, entities: &Vec<Node>) -> bool {
        use TypeCondition::*;
        match self {
            Any => true,
            On(name) => entities.iter().any(|entity| entity.typename() == name),
        }
    }

    /// Return the type that matches this condition; for `Any`, use `object_type`
    fn matching_type<'a>(
        &self,
        schema: &'a s::Document,
        object_type: &'a ObjectOrInterface<'a>,
    ) -> Option<ObjectOrInterface<'a>> {
        use TypeCondition::*;

        match self {
            Any => Some(object_type.to_owned()),
            On(name) => object_or_interface_by_name(schema, name),
        }
    }
}

impl From<Option<q::TypeCondition>> for TypeCondition {
    fn from(cond: Option<q::TypeCondition>) -> Self {
        match cond {
            None => TypeCondition::Any,
            Some(q::TypeCondition::On(name)) => TypeCondition::On(name),
        }
    }
}

/// Intermediate data structure to hold the results of prefetching entities
/// and their nested associations. For each association of `entity`, `children`
/// has an entry mapping the response key to the list of nodes.
#[derive(Debug, Clone)]
struct Node {
    entity: Entity,
    /// We are using an `Rc` here for two reasons: it allows us to defer
    /// copying objects until the end, when converting to `q::Value` forces
    /// us to copy any child that is referenced by multiple parents. It also
    /// makes it possible to avoid unnecessary copying of a child that is
    /// referenced by only one parent - without the `Rc` we would have to copy
    /// since we do not know that only one parent uses it.
    children: BTreeMap<String, Vec<Rc<Node>>>,
}

impl From<Entity> for Node {
    fn from(entity: Entity) -> Self {
        Node {
            entity,
            children: BTreeMap::default(),
        }
    }
}

/// Convert a list of nodes into a `q::Value::List` where each node has also
/// been converted to a `q::Value`
fn node_list_as_value(nodes: Vec<Rc<Node>>) -> q::Value {
    q::Value::List(
        nodes
            .into_iter()
            .map(|node| Rc::try_unwrap(node).unwrap_or_else(|rc| rc.as_ref().clone()))
            .map(|node| node.into())
            .collect(),
    )
}

/// We pass the root node of the result around as a vec of nodes, not as
/// a single node so that we can use the same functions on interior node
/// lists which are the result of querying the database. The root list
/// consists of exactly one entry, and that entry has an empty
/// (not even a `__typename`) entity.
///
/// That distinguishes it from both the result of a query that matches
/// nothing (an empty `Vec`), and a result that finds just one entity
/// (the entity is not completely empty)
fn is_root_node(nodes: &Vec<Node>) -> bool {
    if let Some(node) = nodes.iter().next() {
        node.entity.is_empty()
    } else {
        false
    }
}

fn make_root_node() -> Vec<Node> {
    vec![Node {
        entity: Entity::new(),
        children: BTreeMap::default(),
    }]
}

/// Recursively convert a `Node` into the corresponding `q::Value`, which is
/// always a `q::Value::Object`. The entity's associations are mapped to
/// entries `r:{response_key}` as that name is guaranteed to not conflict
/// with any field of the entity. Also add an entry `:prefetch` so that
/// the resolver can later tell whether the `q::Value` was produced by prefetch
/// and should therefore have `r:{response_key}` entries.
impl From<Node> for q::Value {
    fn from(node: Node) -> Self {
        let mut map: BTreeMap<_, _> = node.entity.into();
        map.insert(PREFETCH_KEY.to_owned(), q::Value::Boolean(true));
        for (key, nodes) in node.children.into_iter() {
            map.insert(format!("r:{}", key), node_list_as_value(nodes));
        }
        q::Value::Object(map)
    }
}

impl Deref for Node {
    type Target = Entity;

    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

impl Node {
    fn typename(&self) -> &str {
        self.get("__typename")
            .expect("all entities have a __typename")
            .as_str()
            .expect("__typename must be a string")
    }
}

/// Encapsulate how we should join a list of parent entities with a list of
/// child entities.
#[derive(Debug)]
struct Join<'a> {
    /// The object type of the child entities
    child_type: ObjectOrInterface<'a>,
    /// The name of the field in the parent type
    parent_field: &'a str,
    /// Whether the parent field is a list or scalar
    parent_is_list: bool,
    /// The name of the field in the child type
    child_field: &'a str,
    /// Whether the child field is a list or a scalar
    child_is_list: bool,
}

impl<'a> Join<'a> {
    /// Construct a `Join` based on the parent field pointing to the child
    fn new(schema: &'a s::Document, field: &'a s::Field) -> Self {
        let child_type = object_or_interface_from_type(&schema, &field.field_type)
            .expect("we only collect fields that are objects or interfaces");

        let (parent_field, parent_is_list, child_field, child_is_list) =
            if let Some(derived_from_field) = sast::get_derived_from_field(child_type, field) {
                let is_list = sast::is_list_or_non_null_list_field(derived_from_field);
                ("id", false, derived_from_field.name.as_str(), is_list)
            } else {
                let is_list = sast::is_list_or_non_null_list_field(field);
                (field.name.as_str(), is_list, "id", false)
            };
        Join {
            child_type,
            parent_field,
            parent_is_list,
            child_field,
            child_is_list,
        }
    }

    /// Perform the join. The child nodes are distributed into the parent nodes
    /// according to the join condition, and are stored in the `response_key`
    /// entry in each parent's `children` map.
    ///
    /// The `children` must contain the nodes in the correct order for each
    /// parent; we simply pick out matching children for each parent but
    /// otherwise maintain the order in `children`
    fn perform(&self, parents: &mut Vec<Node>, children: Vec<Node>, response_key: &str) {
        let children: Vec<_> = children.into_iter().map(|child| Rc::new(child)).collect();

        if is_root_node(parents) {
            let root = parents
                .first_mut()
                .expect("is_root_node checked that we have a node");
            root.children.insert(response_key.to_owned(), children);
            return;
        }

        // Build a map child_key -> Vec<child> for joining by grouping
        // children by their child_field
        let mut grouped: BTreeMap<&str, Vec<Rc<Node>>> = BTreeMap::default();
        for child in children.iter() {
            match child
                .get(self.child_field)
                .expect("the query that produces 'child' ensures there is always an entry")
            {
                StoreValue::String(key) => grouped.entry(key).or_default().push(child.clone()),
                StoreValue::List(list) => {
                    for key in list {
                        match key {
                            StoreValue::String(key) => {
                                grouped.entry(key).or_default().push(child.clone())
                            }
                            _ => unreachable!("a list of join keys contains only strings"),
                        }
                    }
                }
                _ => unreachable!("join fields are strings or lists"),
            }
        }

        // Add appropriate children using grouped map
        for parent in parents.iter_mut() {
            // Set the `response_key` field in `parent`. Make sure that even
            // if `parent` has no matching `children`, the field gets set (to
            // an empty `Vec`)
            // This is complicated by the fact that, if there was a type
            // condition, we should only change parents that meet the type
            // condition; we set it for all parents regardless, as that does
            // not cause problems in later processing, but make sure that we
            // do not clobber an entry under this `response_key` that might
            // have been set by a previous join by appending values rather
            // than using straight insert into the parent
            let mut values = parent
                .get(self.parent_field)
                .and_then(|key| match key {
                    StoreValue::String(key) => {
                        grouped.get(key.as_str()).map(|values| values.clone())
                    }
                    StoreValue::List(keys) => {
                        // Note that each `grouped.get` returns a vec with
                        // at most one element, and since `key` is the id
                        // of that element, the resulting `values` will not
                        // contain duplicates
                        Some(
                            keys.iter()
                                .filter_map(|key| key.as_str())
                                .filter_map(|key| grouped.get(key))
                                .flatten()
                                .map(|rc| rc.clone())
                                .collect::<Vec<_>>(),
                        )
                    }
                    StoreValue::Null => None,
                    _ => unreachable!("join fields must be strings or lists of strings"),
                })
                .unwrap_or(vec![]);
            parent
                .children
                .entry(response_key.to_owned())
                .or_default()
                .append(&mut values);
        }
    }
}

/// Run the query in `ctx` in such a manner that we only perform one query
/// per 'level' in the query. A query like `musicians { id bands { id } }`
/// will perform two queries: one for musicians, and one for bands, regardless
/// of how many musicians there are.
///
/// The returned value contains a `q::Value::Object` that contains a tree of
/// all the entities (converted into objects) in the form in which they need
/// to be returned. Nested object fields appear under the key `r:response_key`
/// in these objects, and are always `q::Value::List` of objects. In addition,
/// each entity has a property `:prefetch` set so that the resolver can
/// tell whether the `r:response_key` associations should be there.
///
/// For the above example, the returned object would have one entry under
/// `r:musicians`, which is a list of all the musicians; each musician has an
/// entry `r:bands` that contains a list of the bands for that musician. Note
/// that even for single-object fields, we return a list so that we can spot
/// cases where the store contains data that violates the data model by having
/// multiple values for what should be a relationship to a single object in
/// @derivedFrom fields
pub fn run<'a, R, S>(
    ctx: &ExecutionContext<'a, R>,
    selection_set: &q::SelectionSet,
    store: Arc<S>,
) -> Result<q::Value, QueryExecutionError>
where
    R: Resolver,
    S: Store,
{
    execute_root_selection_set(ctx, store.as_ref(), selection_set).map(|nodes| {
        let mut map = BTreeMap::default();
        map.insert(PREFETCH_KEY.to_owned(), q::Value::Boolean(true));
        q::Value::Object(nodes.into_iter().fold(map, |mut map, node| {
            // For root nodes, we only care about the children
            for (key, nodes) in node.children.into_iter() {
                map.insert(format!("r:{}", key), node_list_as_value(nodes));
            }
            map
        }))
    })
}

/// Executes the root selection set of a query.
fn execute_root_selection_set<'a, R, S>(
    ctx: &ExecutionContext<'a, R>,
    store: &S,
    selection_set: &'a q::SelectionSet,
) -> Result<Vec<Node>, QueryExecutionError>
where
    R: Resolver,
    S: Store,
{
    // Obtain the root Query type and fail if there isn't one
    let query_type = match sast::get_root_query_type(&ctx.schema.document) {
        Some(t) => t,
        None => return Err(QueryExecutionError::NoRootQueryObjectType),
    };

    // Split the toplevel fields into introspection fields and
    // 'normal' data fields
    let mut data_set = q::SelectionSet {
        span: selection_set.span.clone(),
        items: Vec::new(),
    };

    for (_, type_fields) in collect_fields(ctx, &query_type.into(), selection_set, None) {
        let fields = match type_fields.get(&TypeCondition::Any) {
            None => return Ok(vec![]),
            Some(fields) => fields,
        };

        let name = fields[0].name.clone();
        let selections = fields
            .into_iter()
            .map(|f| q::Selection::Field((*f).clone()));
        // See if this is an introspection or data field. We don't worry about
        // nonexistant fields; those will cause an error later when we execute
        // the query in `execution::execute_root_selection_set`
        if sast::get_field(query_type, &name).is_some() {
            data_set.items.extend(selections)
        }
    }

    // Execute the root selection set against the root query type
    execute_selection_set(&ctx, store, make_root_node(), &data_set, &query_type.into())
        .map_err(|mut e| e.pop().unwrap())
}

fn object_or_interface_from_type<'a>(
    schema: &'a s::Document,
    field_type: &'a s::Type,
) -> Option<ObjectOrInterface<'a>> {
    match field_type {
        s::Type::NonNullType(inner_type) => object_or_interface_from_type(schema, inner_type),
        s::Type::ListType(inner_type) => object_or_interface_from_type(schema, inner_type),
        s::Type::NamedType(name) => object_or_interface_by_name(schema, name),
    }
}

fn object_or_interface_by_name<'a>(
    schema: &'a s::Document,
    name: &s::Name,
) -> Option<ObjectOrInterface<'a>> {
    match sast::get_named_type(schema, name) {
        Some(s::TypeDefinition::Object(t)) => Some(t.into()),
        Some(s::TypeDefinition::Interface(t)) => Some(t.into()),
        _ => None,
    }
}

fn execute_selection_set<'a, R, S>(
    ctx: &ExecutionContext<'a, R>,
    store: &S,
    mut parents: Vec<Node>,
    selection_set: &'a q::SelectionSet,
    object_type: &ObjectOrInterface,
) -> Result<Vec<Node>, Vec<QueryExecutionError>>
where
    R: Resolver,
    S: Store,
{
    let mut errors: Vec<QueryExecutionError> = Vec::new();

    // Group fields with the same response key, so we can execute them together
    let grouped_field_set = collect_fields(ctx, object_type, selection_set, None);

    // Process all field groups in order
    for (response_key, type_map) in grouped_field_set {
        match ctx.deadline {
            Some(deadline) if deadline < Instant::now() => {
                errors.push(QueryExecutionError::Timeout);
                break;
            }
            _ => (),
        }

        for (type_cond, fields) in type_map {
            if !type_cond.matches(&parents) {
                continue;
            }

            let concrete_type = type_cond
                .matching_type(&ctx.schema.document, object_type)
                .expect("collect_fields does not create type conditions for nonexistent types");

            if let Some(ref field) = concrete_type.field(&fields[0].name) {
                let ctx = ctx.for_field(&fields[0]);
                let join = Join::new(&ctx.schema.document, field);

                match execute_field(
                    &ctx,
                    store,
                    &concrete_type,
                    &parents,
                    &join,
                    &fields[0],
                    field,
                ) {
                    Ok(children) => {
                        let child_selection_set = crate::execution::merge_selection_sets(fields);
                        let child_object_type =
                            object_or_interface_from_type(&ctx.schema.document, &field.field_type)
                                .expect("type of child field is object or interface");
                        match execute_selection_set(
                            &ctx,
                            store,
                            children,
                            &child_selection_set,
                            &child_object_type,
                        ) {
                            Ok(children) => join.perform(&mut parents, children, response_key),
                            Err(mut e) => errors.append(&mut e),
                        }
                    }
                    Err(mut e) => {
                        errors.append(&mut e);
                    }
                };
            } else {
                errors.push(QueryExecutionError::UnknownField(
                    fields[0].position,
                    object_type.name().to_owned(),
                    fields[0].name.clone(),
                ))
            }
        }
    }

    if errors.is_empty() {
        Ok(parents)
    } else {
        if errors.is_empty() {
            errors.push(QueryExecutionError::EmptySelectionSet(
                object_type.name().to_owned(),
            ));
        }
        Err(errors)
    }
}

/// Collects fields of a selection set. The resulting map indicates for each
/// response key from which types to fetch what fields to express the effect
/// of fragment spreads
fn collect_fields<'a, R>(
    ctx: &ExecutionContext<'a, R>,
    object_type: &ObjectOrInterface,
    selection_set: &'a q::SelectionSet,
    visited_fragments: Option<HashSet<&'a q::Name>>,
) -> HashMap<&'a String, HashMap<TypeCondition, Vec<&'a q::Field>>>
where
    R: Resolver,
{
    let mut visited_fragments = visited_fragments.unwrap_or_default();
    let mut grouped_fields: HashMap<_, HashMap<_, Vec<_>>> = HashMap::new();

    // Only consider selections that are not skipped and should be included
    let selections: Vec<_> = selection_set
        .items
        .iter()
        .filter(|selection| !qast::skip_selection(selection, ctx.variable_values.deref()))
        .filter(|selection| qast::include_selection(selection, ctx.variable_values.deref()))
        .collect();

    fn is_reference_field(
        schema: &s::Document,
        object_type: &ObjectOrInterface,
        field: &q::Field,
    ) -> bool {
        object_type
            .field(&field.name)
            .map(|field_def| sast::get_type_definition_from_field(schema, field_def))
            .unwrap_or(None)
            .map(|type_def| match type_def {
                s::TypeDefinition::Interface(_) | s::TypeDefinition::Object(_) => true,
                _ => false,
            })
            .unwrap_or(false)
    }

    for selection in selections {
        match selection {
            q::Selection::Field(ref field) => {
                // Only consider fields that point to objects or interfaces, and
                // ignore nonexistent fields
                if is_reference_field(&ctx.schema.document, object_type, field) {
                    let response_key = qast::get_response_key(field);

                    // Create a field group for this response key and add the field
                    // with no type condition
                    grouped_fields
                        .entry(response_key)
                        .or_default()
                        .entry(TypeCondition::Any)
                        .or_default()
                        .push(field);
                }
            }

            q::Selection::FragmentSpread(spread) => {
                // Only consider the fragment if it hasn't already been included,
                // as would be the case if the same fragment spread ...Foo appeared
                // twice in the same selection set
                if !visited_fragments.contains(&spread.fragment_name) {
                    visited_fragments.insert(&spread.fragment_name);

                    qast::get_fragment(&ctx.document, &spread.fragment_name).map(|fragment| {
                        let fragment_grouped_field_set = collect_fields(
                            ctx,
                            object_type,
                            &fragment.selection_set,
                            Some(visited_fragments.clone()),
                        );

                        // Add all items from each fragments group to the field group
                        // with the corresponding response key
                        let fragment_cond =
                            TypeCondition::from(Some(fragment.type_condition.clone()));
                        for (response_key, type_fields) in fragment_grouped_field_set {
                            for (type_cond, mut group) in type_fields {
                                if let Some(cond) = fragment_cond.and(&type_cond) {
                                    grouped_fields
                                        .entry(response_key)
                                        .or_default()
                                        .entry(cond)
                                        .or_default()
                                        .append(&mut group);
                                }
                            }
                        }
                    });
                }
            }

            q::Selection::InlineFragment(fragment) => {
                let fragment_cond = TypeCondition::from(fragment.type_condition.clone());
                // Fields for this fragment need to be looked up in the type
                // mentioned in the condition
                let fragment_type = fragment_cond.matching_type(&ctx.schema.document, object_type);

                // The `None` case here indicates an error where the type condition
                // mentions a nonexistent type; the overall query execution logic will catch
                // that
                if let Some(fragment_type) = fragment_type {
                    let fragment_grouped_field_set = collect_fields(
                        ctx,
                        &fragment_type,
                        &fragment.selection_set,
                        Some(visited_fragments.clone()),
                    );

                    for (response_key, type_fields) in fragment_grouped_field_set {
                        for (type_cond, mut group) in type_fields {
                            if let Some(cond) = fragment_cond.and(&type_cond) {
                                grouped_fields
                                    .entry(response_key)
                                    .or_default()
                                    .entry(cond)
                                    .or_default()
                                    .append(&mut group);
                            }
                        }
                    }
                }
            }
        };
    }

    grouped_fields
}

/// Executes a field.
fn execute_field<'a, R, S>(
    ctx: &ExecutionContext<'a, R>,
    store: &S,
    object_type: &ObjectOrInterface<'_>,
    parents: &Vec<Node>,
    join: &Join<'a>,
    field: &'a q::Field,
    field_definition: &'a s::Field,
) -> Result<Vec<Node>, Vec<QueryExecutionError>>
where
    R: Resolver,
    S: Store,
{
    let mut argument_values = match object_type {
        ObjectOrInterface::Object(object_type) => {
            crate::execution::coerce_argument_values(ctx, object_type, field)
        }
        ObjectOrInterface::Interface(interface_type) => {
            // This assumes that all implementations of the interface accept
            // the same arguments for this field
            match ctx
                .schema
                .types_for_interface
                .get(&interface_type.name)
                .expect("interface type exists")
                .first()
            {
                Some(object_type) => {
                    crate::execution::coerce_argument_values(ctx, &object_type, field)
                }
                None => {
                    // Nobody is implementing this interface
                    return Ok(vec![]);
                }
            }
        }
    }?;

    if !argument_values.contains_key(&*ARG_FIRST) {
        let first = if sast::is_list_or_non_null_list_field(field_definition) {
            // This makes `build_range` use the default, 100
            q::Value::Null
        } else {
            // For non-list fields, get up to 2 entries so we can spot
            // ambiguous references that should only have one entry
            q::Value::Int(2.into())
        };
        argument_values.insert(&*ARG_FIRST, first);
    }

    if !argument_values.contains_key(&*ARG_SKIP) {
        // Use the default in build_range
        argument_values.insert(&*ARG_SKIP, q::Value::Null);
    }

    fetch(
        store,
        &parents,
        &join,
        &argument_values,
        ctx.schema.types_for_interface(),
        ctx.max_first,
    )
    .map_err(|e| vec![e])
}

/// Query child entities for `parents` from the store. The `join` indicates
/// in which child field to look for the parent's id/join field
fn fetch<S: Store>(
    store: &S,
    parents: &Vec<Node>,
    join: &Join<'_>,
    arguments: &HashMap<&q::Name, q::Value>,
    types_for_interface: &BTreeMap<s::Name, Vec<s::ObjectType>>,
    max_first: u32,
) -> Result<Vec<Node>, QueryExecutionError> {
    let mut query = build_query(join.child_type, arguments, types_for_interface, max_first)?;

    if let Some(q::Value::String(id)) = arguments.get(&*ARG_ID) {
        query.filter = Some(
            EntityFilter::Equal(ARG_ID.to_owned(), StoreValue::from(id.to_owned()))
                .and_maybe(query.filter),
        );
    }

    if !is_root_node(parents) {
        // For anything but the root node, restrict the children we select
        // by the parent list
        let mut ids = if join.parent_is_list {
            parents
                .iter()
                .filter_map(|entity| entity.get(join.parent_field))
                .flat_map(|list| match list {
                    StoreValue::List(values) => values.iter().filter_map(|value| value.as_str()),
                    _ => unreachable!("parent_field is not a list of strings"),
                })
                .collect::<Vec<_>>()
        } else {
            parents
                .iter()
                .filter_map(|entity| entity.get(join.parent_field))
                .filter_map(|value| match value {
                    StoreValue::String(s) => Some(s.as_str()),
                    StoreValue::Null => None,
                    _ => unreachable!("parent_field must be a string or null"),
                })
                .collect::<Vec<_>>()
        };
        if ids.is_empty() {
            return Ok(vec![]);
        }
        ids.sort_unstable();
        ids.dedup();
        let ids = ids.into_iter().map(|id| StoreValue::from(id)).collect();
        // We want to find all children that point to one of the parent `ids`
        // If `child_field` is a list that is any child that has one of the
        // parent `ids` in its `child_field`, hence the intersection
        let filter = if join.child_is_list {
            EntityFilter::Intersects(join.child_field.to_owned(), ids)
        } else {
            EntityFilter::In(join.child_field.to_owned(), ids)
        };
        query.filter = Some(filter.and_maybe(query.filter));
        // Apply order by/range conditions to each batch of children, grouped
        // by parent
        query.window = Some(join.child_field.to_owned());
    }

    store
        .find(query)
        .map(|entities| entities.into_iter().map(|entity| entity.into()).collect())
}
