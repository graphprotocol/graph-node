//! Run a GraphQL query and fetch all the entitied needed to build the
//! final result

use anyhow::{anyhow, Error};
use indexmap::IndexMap;
use lazy_static::lazy_static;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::once;
use std::rc::Rc;
use std::time::Instant;

use graph::prelude::{
    q, s, ApiSchema, BlockNumber, ChildMultiplicity, EntityCollection, EntityFilter, EntityLink,
    EntityOrder, EntityWindow, Logger, ParentLink, QueryExecutionError, QueryStore,
    Value as StoreValue, WindowAttribute,
};
use graph::{components::store::EntityType, data::graphql::*};

use crate::execution::{ExecutionContext, Resolver};
use crate::query::ast as qast;
use crate::schema::ast as sast;
use crate::store::{build_query, StoreResolver};

lazy_static! {
    static ref ARG_FIRST: String = String::from("first");
    static ref ARG_SKIP: String = String::from("skip");
    static ref ARG_ID: String = String::from("id");
}

/// An `ObjectType` with `Hash` and `Eq` derived from the name.
#[derive(Clone, Debug)]
struct ObjectCondition<'a>(&'a s::ObjectType);

impl std::hash::Hash for ObjectCondition<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.name.hash(state)
    }
}

impl PartialEq for ObjectCondition<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.name.eq(&other.0.name)
    }
}

impl Eq for ObjectCondition<'_> {}

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
            .map(Into::into)
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
fn is_root_node<'a>(mut nodes: impl Iterator<Item = &'a Node>) -> bool {
    if let Some(node) = nodes.next() {
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
    fn id(&self) -> Result<String, Error> {
        match self.get("id") {
            None => Err(anyhow!("Entity is missing an `id` attribute")),
            Some(q::Value::String(s)) => Ok(s.to_owned()),
            _ => Err(anyhow!("Entity has non-string `id` attribute")),
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
    parent_type: EntityType,
    /// The (concrete) object type of the child, interfaces will have
    /// one `JoinCond` for each implementing type
    child_type: EntityType,
    relation: JoinRelation<'a>,
}

impl<'a> JoinCond<'a> {
    fn new(
        parent_type: &'a s::ObjectType,
        child_type: &'a s::ObjectType,
        field_name: &str,
    ) -> Self {
        let field = parent_type
            .field(field_name)
            .expect("field_name is a valid field of parent_type");
        let relation =
            if let Some(derived_from_field) = sast::get_derived_from_field(child_type, field) {
                JoinRelation::Direct(JoinField::new(derived_from_field))
            } else {
                JoinRelation::Derived(JoinField::new(field))
            };
        JoinCond {
            parent_type: parent_type.into(),
            child_type: child_type.into(),
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
        schema: &'a ApiSchema,
        parent_type: ObjectOrInterface<'a>,
        child_type: ObjectOrInterface<'a>,
        field_name: &str,
    ) -> Self {
        let parent_types = parent_type
            .object_types(schema.schema())
            .expect("the name of the parent type is valid");
        let child_types = child_type
            .object_types(schema.schema())
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

        Join { child_type, conds }
    }

    /// Perform the join. The child nodes are distributed into the parent nodes
    /// according to the `parent_id` returned by the database in each child as
    /// attribute `g$parent_id`, and are stored in the `response_key` entry
    /// in each parent's `children` map.
    ///
    /// The `children` must contain the nodes in the correct order for each
    /// parent; we simply pick out matching children for each parent but
    /// otherwise maintain the order in `children`
    fn perform(mut parents: Vec<&mut Node>, children: Vec<Node>, response_key: &str) {
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
        for parent in parents {
            // Set the `response_key` field in `parent`. Make sure that even if `parent` has no
            // matching `children`, the field gets set (to an empty `Vec`).
            //
            // This `insert` will overwrite in the case where the response key occurs both at the
            // interface level and in nested object type conditions. The values for the interface
            // query are always joined first, and may then be overwritten by the merged selection
            // set under the object type condition. See also: e0d6da3e-60cf-41a5-b83c-b60a7a766d4a
            let values = parent.id().ok().and_then(|id| grouped.get(&*id).cloned());
            parent
                .children
                .insert(response_key.to_owned(), values.unwrap_or(vec![]));
        }
    }

    fn windows(
        &self,
        parents: &Vec<&mut Node>,
        multiplicity: ChildMultiplicity,
    ) -> Vec<EntityWindow> {
        let mut windows = vec![];

        for cond in &self.conds {
            let mut parents_by_id = parents
                .iter()
                .filter(|parent| parent.typename() == cond.parent_type.as_str())
                .filter_map(|parent| parent.id().ok().map(|id| (id, &**parent)))
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
    let query_type = ctx.query.schema.query_type.as_ref().into();

    let grouped_field_set = collect_fields(ctx, query_type, once(selection_set));

    // Execute the root selection set against the root query type
    execute_selection_set(resolver, ctx, make_root_node(), grouped_field_set)
}

fn execute_selection_set<'a>(
    resolver: &StoreResolver,
    ctx: &'a ExecutionContext<impl Resolver>,
    mut parents: Vec<Node>,
    grouped_field_set: IndexMap<&'a str, CollectedResponseKey<'a>>,
) -> Result<Vec<Node>, Vec<QueryExecutionError>> {
    let schema = &ctx.query.schema;
    let mut errors: Vec<QueryExecutionError> = Vec::new();

    // Process all field groups in order
    for (response_key, collected_fields) in grouped_field_set {
        if let Some(deadline) = ctx.deadline {
            if deadline < Instant::now() {
                errors.push(QueryExecutionError::Timeout);
                break;
            }
        }

        for (type_cond, fields) in collected_fields {
            // Filter out parents that do not match the type condition.
            let parents: Vec<&mut Node> = if is_root_node(parents.iter()) {
                parents.iter_mut().collect()
            } else {
                parents
                    .iter_mut()
                    .filter(|p| type_cond.matches(p.typename(), schema.types_for_interface()))
                    .collect()
            };

            if parents.is_empty() {
                continue;
            }

            // Unwrap: The query was validated to contain only valid fields,
            // and `collect_fields` will skip introspection fields.
            let field = type_cond.field(&fields[0].name).unwrap();
            let child_type = schema
                .document()
                .object_or_interface(field.field_type.get_base_type())
                .expect("we only collect fields that are objects or interfaces");

            let join = Join::new(
                ctx.query.schema.as_ref(),
                type_cond,
                child_type,
                &field.name,
            );

            // Group fields with the same response key, so we can execute them together
            let grouped_field_set =
                collect_fields(ctx, child_type, fields.iter().map(|f| &f.selection_set));

            match execute_field(
                resolver, &ctx, type_cond, &parents, &join, &fields[0], field,
            ) {
                Ok(children) => {
                    match execute_selection_set(resolver, ctx, children, grouped_field_set) {
                        Ok(children) => Join::perform(parents, children, response_key),
                        Err(mut e) => errors.append(&mut e),
                    }
                }
                Err(mut e) => {
                    errors.append(&mut e);
                }
            };
        }
    }

    if errors.is_empty() {
        Ok(parents)
    } else {
        Err(errors)
    }
}

/// If the top-level selection is on an object, there will be a single entry in `obj_types` with all
/// the collected fields.
///
/// The interesting case is if the top-level selection is an interface. `iface_cond` will be the
/// interface type and `iface_fields` the selected fields on the interface. `obj_types` are the
/// fields selected on objects by fragments. In `collect_fields`, the `iface_fields` will then be
/// merged into each entry in `obj_types`. See also: e0d6da3e-60cf-41a5-b83c-b60a7a766d4a
#[derive(Default)]
struct CollectedResponseKey<'a> {
    iface_cond: Option<&'a s::InterfaceType>,
    iface_fields: Vec<&'a q::Field>,
    obj_types: IndexMap<ObjectCondition<'a>, Vec<&'a q::Field>>,
}

impl<'a> CollectedResponseKey<'a> {
    fn collect_field(&mut self, type_condition: ObjectOrInterface<'a>, field: &'a q::Field) {
        match type_condition {
            ObjectOrInterface::Interface(i) => {
                // `collect_fields` will never call this with two different interfaces types.
                assert!(
                    self.iface_cond.is_none() || self.iface_cond.map(|x| &x.name) == Some(&i.name)
                );
                self.iface_cond = Some(i);
                self.iface_fields.push(field);
            }
            ObjectOrInterface::Object(o) => {
                self.obj_types
                    .entry(ObjectCondition(o))
                    .or_default()
                    .push(field);
            }
        }
    }
}

impl<'a> IntoIterator for CollectedResponseKey<'a> {
    type Item = (ObjectOrInterface<'a>, Vec<&'a q::Field>);
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        // Make sure the interface fields are processed first.
        // See also: e0d6da3e-60cf-41a5-b83c-b60a7a766d4a
        let iface_fields = self.iface_fields;
        Box::new(
            self.iface_cond
                .map(|cond| (ObjectOrInterface::Interface(cond), iface_fields))
                .into_iter()
                .chain(
                    self.obj_types
                        .into_iter()
                        .map(|(c, f)| (ObjectOrInterface::Object(c.0), f)),
                ),
        )
    }
}

/// Collects fields of a selection set. The resulting map indicates for each
/// response key from which types to fetch what fields to express the effect
/// of fragment spreads
fn collect_fields<'a>(
    ctx: &'a ExecutionContext<impl Resolver>,
    parent_ty: ObjectOrInterface<'a>,
    selection_sets: impl Iterator<Item = &'a q::SelectionSet>,
) -> IndexMap<&'a str, CollectedResponseKey<'a>> {
    let mut grouped_fields = IndexMap::new();

    for selection_set in selection_sets {
        collect_fields_inner(
            ctx,
            parent_ty,
            selection_set,
            &mut HashSet::new(),
            &mut grouped_fields,
        );
    }

    // For interfaces, if a response key occurs both under the interface and under concrete types,
    // we want to add the fields selected at the interface level to the selections in the specific
    // concrete types, effectively merging the selection sets.
    // See also: e0d6da3e-60cf-41a5-b83c-b60a7a766d4a
    for collected_response_key in grouped_fields.values_mut() {
        for obj_type_fields in collected_response_key.obj_types.values_mut() {
            obj_type_fields.extend_from_slice(&collected_response_key.iface_fields)
        }
    }

    grouped_fields
}

// When querying an object type, `type_condition` will always be that object type, even if it passes
// through fragments for interfaces which that type implements.
//
// When querying an interface, `type_condition` will start as the interface itself at the root, and
// change to an implementing object type if it passes to a fragment with a concrete type condition.
fn collect_fields_inner<'a>(
    ctx: &'a ExecutionContext<impl Resolver>,
    type_condition: ObjectOrInterface<'a>,
    selection_set: &'a q::SelectionSet,
    visited_fragments: &mut HashSet<&'a str>,
    output: &mut IndexMap<&'a str, CollectedResponseKey<'a>>,
) {
    fn is_reference_field(
        schema: &s::Document,
        object_type: ObjectOrInterface,
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

    fn collect_fragment<'a>(
        ctx: &'a ExecutionContext<impl Resolver>,
        outer_type_condition: ObjectOrInterface<'a>,
        frag_ty_condition: Option<&'a q::TypeCondition>,
        frag_selection_set: &'a q::SelectionSet,
        visited_fragments: &mut HashSet<&'a str>,
        output: &mut IndexMap<&'a str, CollectedResponseKey<'a>>,
    ) {
        let schema = &ctx.query.schema.document();
        let fragment_ty = match frag_ty_condition {
            // Unwrap: Validation ensures this interface exists.
            Some(q::TypeCondition::On(ty_name)) if outer_type_condition.is_interface() => {
                schema.object_or_interface(ty_name).unwrap()
            }
            _ => outer_type_condition,
        };

        // The check above makes any type condition on an outer object type redunant.
        // A type condition on the same interface as the outer one is also redundant.
        let redundant_condition = fragment_ty.name() == outer_type_condition.name();
        if redundant_condition || fragment_ty.is_object() {
            collect_fields_inner(
                ctx,
                fragment_ty,
                &frag_selection_set,
                visited_fragments,
                output,
            );
        } else {
            // This is an interface fragment in the root selection for an interface.
            // We deal with this by expanding the fragment into one fragment for
            // each type in the intersection between the root interface and the
            // interface in the fragment type condition.
            let types_for_interface = ctx.query.schema.types_for_interface();
            let root_tys = &types_for_interface[&outer_type_condition.into()];
            let fragment_tys = &types_for_interface[&fragment_ty.into()];
            let intersection_tys = root_tys.iter().filter(|root_ty| {
                fragment_tys
                    .iter()
                    .map(|t| &t.name)
                    .any(|t| *t == root_ty.name)
            });
            for ty in intersection_tys {
                collect_fields_inner(
                    ctx,
                    ty.into(),
                    &frag_selection_set,
                    visited_fragments,
                    output,
                );
            }
        }
    }

    // Only consider selections that are not skipped and should be included
    let selections = selection_set
        .items
        .iter()
        .filter(|selection| !qast::skip_selection(selection, &ctx.query.variables))
        .filter(|selection| qast::include_selection(selection, &ctx.query.variables));

    for selection in selections {
        match selection {
            q::Selection::Field(ref field) => {
                // Only consider fields that point to objects or interfaces, and
                // ignore nonexistent fields
                if is_reference_field(&ctx.query.schema.document(), type_condition, field) {
                    let response_key = qast::get_response_key(field);
                    output
                        .entry(response_key)
                        .or_default()
                        .collect_field(type_condition, field);
                }
            }

            q::Selection::FragmentSpread(spread) => {
                // Only consider the fragment if it hasn't already been included,
                // as would be the case if the same fragment spread ...Foo appeared
                // twice in the same selection set
                if visited_fragments.insert(&spread.fragment_name) {
                    let fragment = ctx.query.get_fragment(&spread.fragment_name);
                    collect_fragment(
                        ctx,
                        type_condition,
                        Some(&fragment.type_condition),
                        &fragment.selection_set,
                        visited_fragments,
                        output,
                    );
                }
            }

            q::Selection::InlineFragment(fragment) => {
                collect_fragment(
                    ctx,
                    type_condition,
                    fragment.type_condition.as_ref(),
                    &fragment.selection_set,
                    visited_fragments,
                    output,
                );
            }
        }
    }
}

/// Executes a field.
fn execute_field(
    resolver: &StoreResolver,
    ctx: &ExecutionContext<impl Resolver>,
    object_type: ObjectOrInterface<'_>,
    parents: &Vec<&mut Node>,
    join: &Join<'_>,
    field: &q::Field,
    field_definition: &s::Field,
) -> Result<Vec<Node>, Vec<QueryExecutionError>> {
    let argument_values = crate::execution::coerce_argument_values(&ctx.query, object_type, field)?;

    let multiplicity = if sast::is_list_or_non_null_list_field(field_definition) {
        ChildMultiplicity::Many
    } else {
        ChildMultiplicity::Single
    };
    fetch(
        ctx.logger.clone(),
        resolver.store.as_ref(),
        parents,
        &join,
        argument_values,
        multiplicity,
        ctx.query.schema.types_for_interface(),
        resolver.block_number(),
        ctx.max_first,
        ctx.max_skip,
        ctx.query.query_id.clone(),
    )
    .map_err(|e| vec![e])
}

/// Query child entities for `parents` from the store. The `join` indicates
/// in which child field to look for the parent's id/join field. When
/// `is_single` is `true`, there is at most one child per parent.
fn fetch(
    logger: Logger,
    store: &(impl QueryStore + ?Sized),
    parents: &Vec<&mut Node>,
    join: &Join<'_>,
    arguments: HashMap<&str, q::Value>,
    multiplicity: ChildMultiplicity,
    types_for_interface: &BTreeMap<EntityType, Vec<s::ObjectType>>,
    block: BlockNumber,
    max_first: u32,
    max_skip: u32,
    query_id: String,
) -> Result<Vec<Node>, QueryExecutionError> {
    let mut query = build_query(
        join.child_type,
        block,
        &arguments,
        types_for_interface,
        max_first,
        max_skip,
    )?;
    query.query_id = Some(query_id);

    if multiplicity == ChildMultiplicity::Single {
        // Suppress 'order by' in lookups of scalar values since
        // that causes unnecessary work in the database
        query.order = EntityOrder::Unordered;
    }

    query.logger = Some(logger);
    if let Some(q::Value::String(id)) = arguments.get(ARG_ID.as_str()) {
        query.filter = Some(
            EntityFilter::Equal(ARG_ID.to_owned(), StoreValue::from(id.to_owned()))
                .and_maybe(query.filter),
        );
    }

    if !is_root_node(parents.iter().map(|p| &**p)) {
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
