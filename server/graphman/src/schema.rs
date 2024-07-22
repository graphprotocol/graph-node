use async_graphql::{EmptySubscription, Schema};

use crate::resolvers::{MutationRoot, QueryRoot};

pub type GraphmanSchema = Schema<QueryRoot, MutationRoot, EmptySubscription>;
