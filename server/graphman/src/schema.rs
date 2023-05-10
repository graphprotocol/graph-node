use graph_core::graphman::core;
use juniper::{graphql_object, EmptySubscription, FieldResult, GraphQLObject};

#[allow(unused)]
struct Subscription;

pub struct GQLContext {}

// To make our context usable by Juniper, we have to implement a marker trait.
impl juniper::Context for GQLContext {}

#[derive(GraphQLObject)]
#[graphql(description = "Success message")]
struct SuccessMessage {
    message: String,
}

pub struct Query;

#[graphql_object(
    // Here we specify the context type for the object.
    // We need to do this in every type that
    // needs access to the context.
    context = GQLContext,
)]
impl Query {
    fn info(context: &GQLContext, id: String) -> FieldResult<SuccessMessage> {
        // core::info::run(pool, store, search, current, pending, used);
        Ok(SuccessMessage {
            message: format!("Unassigned {} successfully!", id),
        })
    }

    fn unassign(context: &GQLContext, id: String) -> FieldResult<SuccessMessage> {
        // core::assign::unassign(primary, sender, search);
        Ok(SuccessMessage {
            message: format!("Unassigned {} successfully!", id),
        })
    }
}
pub struct Mutation;

#[graphql_object(context = GQLContext)]
impl Mutation {
    fn create_something() -> FieldResult<String> {
        Ok(format!("Test"))
    }
}

// A root schema consists of a query, a mutation, and a subscription.
// Request queries can be executed against a RootNode.
pub type Schema = juniper::RootNode<'static, Query, Mutation, EmptySubscription<GQLContext>>;

pub fn create_schema() -> Schema {
    Schema::new(Query {}, Mutation {}, EmptySubscription::new())
}
