//! Run a GraphQL query and fetch all the entitied needed to build the
//! final result

use graphql_parser::query as q;
use graphql_parser::schema as s;
use lazy_static::lazy_static;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use std::time::Instant;

use graph::data::graphql::ext::ObjectTypeExt;
use graph::prelude::{
    BlockNumber, ChildMultiplicity, EntityCollection, EntityFilter, EntityLink, EntityOrder,
    EntityWindow, Logger, ParentLink, QueryExecutionError, QueryStore, Schema, Value as StoreValue,
    WindowAttribute,
};

use crate::execution::{ExecutionContext, ObjectOrInterface, Resolver};
use crate::query::ast as qast;
use crate::schema::ast as sast;
use crate::store::{build_query, StoreResolver};

lazy_static! {
    static ref ARG_FIRST: String = String::from("first");
    static ref ARG_SKIP: String = String::from("skip");
    static ref ARG_ID: String = String::from("id");
}

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
    entity: BTreeMap<String, q::Value>,
    /// We are using an `Rc` here for two reasons: it allows us to defer
    /// copying objects until the end, when converting to `q::Value` forces
    /// us to copy any child that is referenced by multiple parents. It also
    /// makes it possible to avoid unnecessary copying of a child that is
    /// referenced by only one parent - without the `Rc` we would have to copy
    /// since we do not know that only one parent uses it.
    children: BTreeMap<String, Vec<Rc<Node>>>,
}

impl From<BTreeMap<String, q::Value>> for Node {
    fn from(entity: BTreeMap<String, q::Value>) -> Self {
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
        entity: BTreeMap::new(),
        children: BTreeMap::default(),
    }]
}

/// Recursively convert a `Node` into the corresponding `q::Value`, which is
/// always a `q::Value::Object`. The entity's associations are mapped to
/// entries `r:{response_key}` as that name is guaranteed to not conflict
/// with any field of the entity.
impl From<Node> for q::Value {
    fn from(node: Node) -> Self {
        let mut map = node.entity;
        for (key, nodes) in node.children.into_iter() {
            map.insert(format!("prefetch:{}", key), node_list_as_value(nodes));
        }
        q::Value::Object(map)
    }
}

trait ValueExt {
    fn as_str(&self) -> Option<&str>;
}

impl ValueExt for q::Value {
    fn as_str(&self) -> Option<&str> {
        match self {
            q::Value::String(s) => Some(s),
            _ => None,
        }
    }
}

impl Node {
    fn id(&self) -> Result<String, graph::prelude::failure::Error> {
        match self.get("id") {
            None => Err(graph::prelude::failure::format_err!(
                "Entity is missing an `id` attribute"
            )),
            Some(q::Value::String(s)) => Ok(s.to_owned()),
            _ => Err(graph::prelude::failure::format_err!(
                "Entity has non-string `id` attribute"
            )),
        }
    }

    fn get(&self, key: &str) -> Option<&q::Value> {
        self.entity.get(key)
    }

    fn typename(&self) -> &str {
        self.get("__typename")
            .expect("all entities have a __typename")
            .as_str()
            .expect("__typename must be a string")
    }
}

/// Describe a field that we join on. The distinction between scalar and
/// list is important for generating the right filter, and handling results
/// correctly
#[derive(Debug)]
enum JoinField<'a> {
    List(&'a str),
    Scalar(&'a str),
}

impl<'a> JoinField<'a> {
    fn new(field: &'a s::Field) -> Self {
        let name = field.name.as_str();
        if sast::is_list_or_non_null_list_field(field) {
            JoinField::List(name)
        } else {
            JoinField::Scalar(name)
        }
    }

    fn window_attribute(&self) -> WindowAttribute {
        match self {
            JoinField::Scalar(name) => WindowAttribute::Scalar(name.to_string()),
            JoinField::List(name) => WindowAttribute::List(name.to_string()),
        }
    }
}

#[derive(Debug)]
enum JoinRelation<'a> {
    // Name of field in which child stores parent ids
    Direct(JoinField<'a>),
    // Name of the field in the parent type containing child ids
    Derived(JoinField<'a>),
}

#[derive(Debug)]
struct JoinCond<'a> {
    /// The (concrete) object type of the parent, interfaces will have
    /// one `JoinCond` for each implementing type
    parent_type: &'a str,
    /// The (concrete) object type of the child, interfaces will have
    /// one `JoinCond` for each implementing type
    child_type: &'a str,
    parent_field: JoinField<'a>,
    relation: JoinRelation<'a>,
}

impl<'a> JoinCond<'a> {
    fn new(
        parent_type: &'a s::ObjectType,
        child_type: &'a s::ObjectType,
        field_name: &s::Name,
    ) -> Self {
        let field = parent_type
            .field(field_name)
            .expect("field_name is a valid field of parent_type");
        let (relation, parent_field) =
            if let Some(derived_from_field) = sast::get_derived_from_field(child_type, field) {
                (
                    JoinRelation::Direct(JoinField::new(derived_from_field)),
                    JoinField::Scalar("id"),
                )
            } else {
                (
                    JoinRelation::Derived(JoinField::new(field)),
                    JoinField::new(field),
                )
            };
        JoinCond {
            parent_type: parent_type.name.as_str(),
            child_type: child_type.name.as_str(),
            parent_field,
            relation,
        }
    }

    fn entity_link(
        &self,
        parents_by_id: Vec<(String, &Node)>,
        multiplicity: ChildMultiplicity,
    ) -> (Vec<String>, EntityLink) {
        match &self.relation {
            JoinRelation::Direct(field) => {
                // we only need the parent ids
                let ids = parents_by_id.into_iter().map(|(id, _)| id).collect();
                (
                    ids,
                    EntityLink::Direct(field.window_attribute(), multiplicity),
                )
            }
            JoinRelation::Derived(field) => {
                let (ids, parent_link) = match field {
                    JoinField::Scalar(child_field) => {
                        // child_field contains a String id of the child; extract
                        // those and the parent ids
                        let (ids, child_ids): (Vec<_>, Vec<_>) = parents_by_id
                            .into_iter()
                            .filter_map(|(id, node)| {
                                node.get(*child_field)
                                    .and_then(|value| value.as_str())
                                    .and_then(|child_id| Some((id, child_id.to_owned())))
                            })
                            .unzip();

                        (ids, ParentLink::Scalar(child_ids))
                    }
                    JoinField::List(child_field) => {
                        // child_field stores a list of child ids; extract them,
                        // turn them into a list of strings and combine with the
                        // parent ids
                        let (ids, child_ids): (Vec<_>, Vec<_>) = parents_by_id
                            .into_iter()
                            .filter_map(|(id, node)| {
                                node.get(*child_field)
                                    .and_then(|value| match value {
                                        q::Value::List(values) => {
                                            let values: Vec<_> = values
                                                .into_iter()
                                                .filter_map(|value| {
                                                    value.as_str().map(|value| value.to_owned())
                                                })
                                                .collect();
                                            if values.is_empty() {
                                                None
                                            } else {
                                                Some(values)
                                            }
                                        }
                                        _ => None,
                                    })
                                    .and_then(|child_ids| Some((id, child_ids)))
                            })
                            .unzip();
                        (ids, ParentLink::List(child_ids))
                    }
                };
                (ids, EntityLink::Parent(parent_link))
            }
        }
    }
}

/// Encapsulate how we should join a list of parent entities with a list of
/// child entities.
#[derive(Debug)]
struct Join<'a> {
    /// The object type of the child entities
    child_type: ObjectOrInterface<'a>,
    conds: Vec<JoinCond<'a>>,
}

impl<'a> Join<'a> {
    /// Construct a `Join` based on the parent field pointing to the child
    fn new(
        schema: &'a Schema,
        parent_type: &'a ObjectOrInterface<'a>,
        child_type: &'a ObjectOrInterface<'a>,
        field_name: &s::Name,
    ) -> Self {
        let parent_types = parent_type
            .object_types(schema)
            .expect("the name of the parent type is valid");
        let child_types = child_type
            .object_types(schema)
            .expect("the name of the child type is valid");

        let conds = parent_types
            .iter()
            .flat_map::<Vec<_>, _>(|parent_type| {
                child_types
                    .iter()
                    .map(|child_type| JoinCond::new(parent_type, child_type, field_name))
                    .collect()
            })
            .collect();

        Join {
            child_type: child_type.clone(),
            conds,
        }
    }

    /// Perform the join. The child nodes are distributed into the parent nodes
    /// according to the `parent_id` returned by the database in each child as
    /// attribute `g$parent_id`, and are stored in the `response_key` entry
    /// in each parent's `children` map.
    ///
    /// The `children` must contain the nodes in the correct order for each
    /// parent; we simply pick out matching children for each parent but
    /// otherwise maintain the order in `children`
    fn perform(parents: &mut Vec<Node>, children: Vec<Node>, response_key: &str) {
        let children: Vec<_> = children.into_iter().map(|child| Rc::new(child)).collect();

        if parents.len() == 1 {
            let parent = parents.first_mut().expect("we just checked");
            parent.children.insert(response_key.to_owned(), children);
            return;
        }

        // Build a map parent_id -> Vec<child> that we will use to add
        // children to their parent. This relies on the fact that interfaces
        // make sure that id's are distinct across all implementations of the
        // interface.
        let mut grouped: BTreeMap<&str, Vec<Rc<Node>>> = BTreeMap::default();
        for child in children.iter() {
            match child
                .get("g$parent_id")
                .expect("the query that produces 'child' ensures there is always a g$parent_id")
            {
                q::Value::String(key) => grouped.entry(&key).or_default().push(child.clone()),
                _ => unreachable!("the parent_id returned by the query is always a string"),
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
                .id()
                .ok()
                .and_then(|id| grouped.get(&*id).map(|values| values.clone()))
                .unwrap_or(vec![]);
            parent
                .children
                .entry(response_key.to_owned())
                .or_default()
                .append(&mut values);
        }
    }

    fn windows(&self, parents: &Vec<Node>, multiplicity: ChildMultiplicity) -> Vec<EntityWindow> {
        let mut windows = vec![];

        for cond in &self.conds {
            // Get the cond.parent_field attributes from each parent that
            // is of type cond.parent_type
            let mut parents_by_id = parents
                .iter()
                .filter(|parent| parent.typename() == cond.parent_type)
                .filter_map(|parent| parent.id().ok().map(|id| (id, parent)))
                .collect::<Vec<_>>();

            if !parents_by_id.is_empty() {
                parents_by_id.sort_unstable_by(|(id1, _), (id2, _)| id1.cmp(id2));
                parents_by_id.dedup_by(|(id1, _), (id2, _)| id1 == id2);

                let (ids, link) = cond.entity_link(parents_by_id, multiplicity);
                windows.push(EntityWindow {
                    child_type: cond.child_type.to_owned(),
                    ids,
                    link,
                });
            }
        }
        windows
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
/// in these objects, and are always `q::Value::List` of objects.
///
/// For the above example, the returned object would have one entry under
/// `r:musicians`, which is a list of all the musicians; each musician has an
/// entry `r:bands` that contains a list of the bands for that musician. Note
/// that even for single-object fields, we return a list so that we can spot
/// cases where the store contains data that violates the data model by having
/// multiple values for what should be a relationship to a single object in
/// @derivedFrom fields
pub fn run(
    resolver: &StoreResolver,
    ctx: &ExecutionContext<impl Resolver>,
    selection_set: &q::SelectionSet,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    execute_root_selection_set(resolver, ctx, selection_set).map(|nodes| {
        let map = BTreeMap::default();
        q::Value::Object(nodes.into_iter().fold(map, |mut map, node| {
            // For root nodes, we only care about the children
            for (key, nodes) in node.children.into_iter() {
                map.insert(format!("prefetch:{}", key), node_list_as_value(nodes));
            }
            map
        }))
    })
}

/// Executes the root selection set of a query.
fn execute_root_selection_set(
    resolver: &StoreResolver,
    ctx: &ExecutionContext<impl Resolver>,
    selection_set: &q::SelectionSet,
) -> Result<Vec<Node>, Vec<QueryExecutionError>> {
    // Obtain the root Query type and fail if there isn't one
    let query_type = match sast::get_root_query_type(&ctx.query.schema.document) {
        Some(t) => t,
        None => return Err(vec![QueryExecutionError::NoRootQueryObjectType]),
    };

    // Split the toplevel fields into introspection fields and
    // 'normal' data fields
    let mut data_set = q::SelectionSet {
        span: selection_set.span.clone(),
        items: Vec::new(),
    };

    for (_, type_fields) in collect_fields(ctx, &query_type.into(), vec![selection_set], None) {
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
    execute_selection_set(
        resolver,
        ctx,
        make_root_node(),
        vec![&data_set],
        &query_type.into(),
    )
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

fn execute_selection_set<'a>(
    resolver: &StoreResolver,
    ctx: &'a ExecutionContext<impl Resolver>,
    mut parents: Vec<Node>,
    selection_sets: Vec<&'a q::SelectionSet>,
    object_type: &ObjectOrInterface,
) -> Result<Vec<Node>, Vec<QueryExecutionError>> {
    let mut errors: Vec<QueryExecutionError> = Vec::new();

    // Group fields with the same response key, so we can execute them together
    let grouped_field_set = collect_fields(ctx, object_type, selection_sets, None);

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
                .matching_type(&ctx.query.schema.document, object_type)
                .expect("collect_fields does not create type conditions for nonexistent types");

            if let Some(ref field) = concrete_type.field(&fields[0].name) {
                let child_type =
                    object_or_interface_from_type(&ctx.query.schema.document, &field.field_type)
                        .expect("we only collect fields that are objects or interfaces");

                let join = Join::new(
                    ctx.query.schema.as_ref(),
                    &concrete_type,
                    &child_type,
                    &field.name,
                );

                match execute_field(
                    resolver,
                    &ctx,
                    &concrete_type,
                    &parents,
                    &join,
                    &fields[0],
                    field,
                ) {
                    Ok(children) => {
                        let child_object_type = object_or_interface_from_type(
                            &ctx.query.schema.document,
                            &field.field_type,
                        )
                        .expect("type of child field is object or interface");
                        match execute_selection_set(
                            resolver,
                            ctx,
                            children,
                            fields.into_iter().map(|f| &f.selection_set).collect(),
                            &child_object_type,
                        ) {
                            Ok(children) => Join::perform(&mut parents, children, response_key),
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
fn collect_fields<'a>(
    ctx: &'a ExecutionContext<impl Resolver>,
    object_type: &ObjectOrInterface,
    selection_sets: Vec<&'a q::SelectionSet>,
    visited_fragments: Option<HashSet<&'a q::Name>>,
) -> HashMap<&'a String, HashMap<TypeCondition, Vec<&'a q::Field>>> {
    let mut visited_fragments = visited_fragments.unwrap_or_default();
    let mut grouped_fields: HashMap<_, HashMap<_, Vec<_>>> = HashMap::new();

    for selection_set in selection_sets {
        // Only consider selections that are not skipped and should be included
        let selections = selection_set
            .items
            .iter()
            .filter(|selection| !qast::skip_selection(selection, &ctx.query.variables))
            .filter(|selection| qast::include_selection(selection, &ctx.query.variables));

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
                    if is_reference_field(&ctx.query.schema.document, object_type, field) {
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

                        ctx.query
                            .get_fragment(&spread.fragment_name)
                            .map(|fragment| {
                                let fragment_grouped_field_set = collect_fields(
                                    ctx,
                                    object_type,
                                    vec![&fragment.selection_set],
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
                    let fragment_type =
                        fragment_cond.matching_type(&ctx.query.schema.document, object_type);

                    // The `None` case here indicates an error where the type condition
                    // mentions a nonexistent type; the overall query execution logic will catch
                    // that
                    if let Some(fragment_type) = fragment_type {
                        let fragment_grouped_field_set = collect_fields(
                            ctx,
                            &fragment_type,
                            vec![&fragment.selection_set],
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
    }

    grouped_fields
}

/// Executes a field.
fn execute_field(
    resolver: &StoreResolver,
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &ObjectOrInterface<'_>,
    parents: &Vec<Node>,
    join: &Join<'_>,
    field: &q::Field,
    field_definition: &s::Field,
) -> Result<Vec<Node>, Vec<QueryExecutionError>> {
    let argument_values = match object_type {
        ObjectOrInterface::Object(object_type) => {
            crate::execution::coerce_argument_values(ctx, object_type, field)
        }
        ObjectOrInterface::Interface(interface_type) => {
            // This assumes that all implementations of the interface accept
            // the same arguments for this field
            match ctx
                .query
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

    let multiplicity = if sast::is_list_or_non_null_list_field(field_definition) {
        ChildMultiplicity::Many
    } else {
        ChildMultiplicity::Single
    };
    fetch(
        ctx.logger.clone(),
        resolver.store.as_ref(),
        &parents,
        &join,
        argument_values,
        multiplicity,
        ctx.query.schema.types_for_interface(),
        resolver.block,
        ctx.max_first,
    )
    .map_err(|e| vec![e])
}

/// Query child entities for `parents` from the store. The `join` indicates
/// in which child field to look for the parent's id/join field. When
/// `is_single` is `true`, there is at most one child per parent.
fn fetch(
    logger: Logger,
    store: &(impl QueryStore + ?Sized),
    parents: &Vec<Node>,
    join: &Join<'_>,
    arguments: HashMap<&q::Name, q::Value>,
    multiplicity: ChildMultiplicity,
    types_for_interface: &BTreeMap<s::Name, Vec<s::ObjectType>>,
    block: BlockNumber,
    max_first: u32,
) -> Result<Vec<Node>, QueryExecutionError> {
    let mut query = build_query(
        join.child_type,
        block,
        &arguments,
        types_for_interface,
        max_first,
    )?;

    if multiplicity == ChildMultiplicity::Single {
        // Suppress 'order by' in lookups of scalar values since
        // that causes unnecessary work in the database
        query.order = EntityOrder::Unordered;
    }

    query.logger = Some(logger);
    if let Some(q::Value::String(id)) = arguments.get(&*ARG_ID) {
        query.filter = Some(
            EntityFilter::Equal(ARG_ID.to_owned(), StoreValue::from(id.to_owned()))
                .and_maybe(query.filter),
        );
    }

    if !is_root_node(parents) {
        // For anything but the root node, restrict the children we select
        // by the parent list
        let windows = join.windows(parents, multiplicity);
        if windows.len() == 0 {
            return Ok(vec![]);
        }
        query.collection = EntityCollection::Window(windows);
    }

    store
        .find_query_values(query)
        .map(|entities| entities.into_iter().map(|entity| entity.into()).collect())
}
