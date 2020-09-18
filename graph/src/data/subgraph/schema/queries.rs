use super::SubgraphHealth;
use crate::components::graphql::GraphQlRunner;
use crate::components::store::{Store, SubgraphDeploymentStore};
use crate::data::graphql::ValueMap;
use crate::data::query::{Query, QueryVariables};
use crate::data::subgraph::schema::SUBGRAPHS_ID;
use crate::data::subgraph::SubgraphDeploymentId;
use crate::prelude::CheapClone;
use failure::Error;
use graphql_parser::parse_query;
use graphql_parser::query as q;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::sync::Arc;

/// Helper to lazily query the store for a subgraph's metadata.
pub struct LazyMetadata<S, Q> {
    pub store: Arc<S>,
    pub graphql_runner: Arc<Q>,
    pub id: SubgraphDeploymentId,
}

impl<S: SubgraphDeploymentStore + Store, Q: GraphQlRunner> LazyMetadata<S, Q> {
    pub async fn health(&self) -> Result<SubgraphHealth, Error> {
        let value = self
            .graphql_runner
            .cheap_clone()
            .query_metadata(Query::new(
                self.store.api_schema(&SUBGRAPHS_ID).unwrap(),
                parse_query(
                    r#"
                        query deployment($id: ID!) {
                            subgraphDeployment(id: $id) {
                                health
                            }
                        }
                    "#,
                )
                .unwrap(),
                Some(QueryVariables::new(HashMap::from_iter(
                    vec![(String::from("id"), q::Value::String(self.id.to_string()))].into_iter(),
                ))),
                None,
            ))
            .await?;

        let deployment = match value.as_ref() {
            q::Value::Object(map) => match &map["subgraphDeployment"] {
                q::Value::Object(deployment) => deployment,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };

        deployment.get_required("health")
    }

    pub async fn has_non_fatal_errors(&self) -> Result<bool, Error> {
        let value = self
            .graphql_runner
            .cheap_clone()
            .query_metadata(Query::new(
                self.store.api_schema(&SUBGRAPHS_ID).unwrap(),
                parse_query(
                    r#"
                        query deployment($id: ID!) {
                            subgraphDeployment(id: $id) {
                                nonFatalErrors(limit: 1) {
                                    id
                                }
                            }
                        }
                    "#,
                )
                .unwrap(),
                Some(QueryVariables::new(HashMap::from_iter(
                    vec![(String::from("id"), q::Value::String(self.id.to_string()))].into_iter(),
                ))),
                None,
            ))
            .await?;

        let deployment = match value.as_ref() {
            q::Value::Object(map) => match &map["subgraphDeployment"] {
                q::Value::Object(deployment) => deployment,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        };

        let has_non_fatal_errors = match &deployment["nonFatalErrors"] {
            q::Value::List(errs) => !errs.is_empty(),
            _ => unreachable!(),
        };

        Ok(has_non_fatal_errors)
    }
}
