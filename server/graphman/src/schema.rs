use async_graphql::EmptySubscription;
use async_graphql::Schema;

use crate::resolvers::MutationRoot;
use crate::resolvers::QueryRoot;

pub type GraphmanSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;
