use std::collections::HashMap;
use std::iter::FromIterator;

use graph::data::subgraph::schema::*;
use graph::data::subgraph::UnresolvedDataSource;
use graph::prelude::{DataSourceLoader as DataSourceLoaderTrait, GraphQlRunner, *};
use graph_graphql::graphql_parser::{parse_query, query as q};

pub struct DataSourceLoader<L, Q, S> {
    store: Arc<S>,
    link_resolver: Arc<L>,
    graphql_runner: Arc<Q>,
}

impl<L, Q, S> DataSourceLoader<L, Q, S>
where
    L: LinkResolver,
    S: Store + SubgraphDeploymentStore,
    Q: GraphQlRunner,
{
    pub fn new(store: Arc<S>, link_resolver: Arc<L>, graphql_runner: Arc<Q>) -> Arc<Self> {
        Arc::new(Self {
            store,
            link_resolver,
            graphql_runner,
        })
    }

    fn dynamic_data_sources_query(
        self: Arc<Self>,
        deployment: &SubgraphDeploymentId,
    ) -> Result<Query, Error> {
        // Obtain the "subgraphs" schema
        let schema = self.store.subgraph_schema(&SUBGRAPHS_ID)?;

        // Construct a query for the subgraph deployment and all its
        // dynamic data sources
        Ok(Query {
            schema,
            document: parse_query(
                r#"
                query deployment($id: ID!) {
                  subgraphDeployment(id: $id) {
                    dynamicDataSources(orderBy: id) {
                      kind
                      network
                      name
                      source { address abi }
                      mapping {
                        kind
                        apiVersion
                        language
                        file
                        entities
                        abis { name file }
                        eventHandlers { event handler }
                      }
                      templates {
                        kind
                        network
                        name
                        source { abi }
                        mapping {
                          kind
                          apiVersion
                          language
                          file
                          entities
                          abis { name file }
                          eventHandlers { event handler }
                        }
                      }
                    }
                  }
                }
                "#,
            )
            .expect("invalid query for dynamic data sources"),
            variables: Some(QueryVariables::new(HashMap::from_iter(
                vec![(String::from("id"), q::Value::String(deployment.to_string()))].into_iter(),
            ))),
        })
    }

    fn query_dynamic_data_sources(
        self: Arc<Self>,
        deployment: SubgraphDeploymentId,
        query: Query,
    ) -> impl Future<Item = QueryResult, Error = Error> + Send {
        self.graphql_runner
            .run_query(query)
            .map_err(move |e| format_err!("Failed to load manifest `{}`: {}", deployment, e))
    }

    fn parse_data_sources(
        self: Arc<Self>,
        deployment_id: SubgraphDeploymentId,
        query_result: QueryResult,
    ) -> Result<Vec<EthereumContractDataSourceEntity>, Error> {
        let data = match query_result.data.expect("subgraph deployment not found") {
            q::Value::Object(obj) => Ok(obj),
            _ => Err(format_err!(
                "Query result for deployment `{}` is not an on object",
                deployment_id,
            )),
        }?;

        // Extract the deployment from the query result
        let deployment = match data.get("subgraphDeployment") {
            Some(q::Value::Object(obj)) => Ok(obj),
            _ => Err(format_err!(
                "Deployment `{}` is not an object",
                deployment_id,
            )),
        }?;

        // Extract the dynamic data sources from the query result
        let values = match deployment.get("dynamicDataSources") {
            Some(q::Value::List(objs)) => {
                if objs.iter().all(|obj| match obj {
                    q::Value::Object(_) => true,
                    _ => false,
                }) {
                    Ok(objs)
                } else {
                    Err(format_err!(
                        "Not all dynamic data sources of deployment `{}` are objects",
                        deployment_id
                    ))
                }
            }
            _ => Err(format_err!(
                "Dynamic data sources of deployment `{}` are not a list",
                deployment_id
            )),
        }?;

        // Parse the raw data sources into typed entities
        let entities = values.into_iter().try_fold(vec![], |mut entities, value| {
            entities.push(EthereumContractDataSourceEntity::try_from_value(value)?);
            Ok(entities)
        }) as Result<Vec<_>, Error>;

        entities.map_err(|e| {
            format_err!(
                "Failed to parse dynamic data source entities of deployment `{}`: {}",
                deployment_id,
                e
            )
        })
    }

    fn convert_to_unresolved_data_sources(
        self: Arc<Self>,
        entities: Vec<EthereumContractDataSourceEntity>,
    ) -> impl Future<Item = Vec<UnresolvedDataSource>, Error = Error> + Send {
        // Turn the entities into unresolved data sources
        future::ok(
            entities
                .into_iter()
                .map(Into::into)
                .collect::<Vec<UnresolvedDataSource>>(),
        )
    }

    fn resolve_data_sources(
        self: Arc<Self>,
        unresolved_data_sources: Vec<UnresolvedDataSource>,
    ) -> impl Future<Item = Vec<DataSource>, Error = Error> + Send {
        // Resolve the data sources and return them
        stream::iter_ok(unresolved_data_sources).fold(vec![], move |mut resolved, data_source| {
            data_source
                .resolve(&*self.link_resolver)
                .and_then(|data_source| {
                    resolved.push(data_source);
                    future::ok(resolved)
                })
        })
    }
}

impl<L, Q, S> DataSourceLoaderTrait for DataSourceLoader<L, Q, S>
where
    L: LinkResolver,
    Q: GraphQlRunner,
    S: Store + SubgraphDeploymentStore,
{
    fn load_dynamic_data_sources(
        self: Arc<Self>,
        deployment_id: &SubgraphDeploymentId,
    ) -> Box<Future<Item = Vec<DataSource>, Error = Error> + Send> {
        let self1 = self.clone();
        let self2 = self.clone();
        let self3 = self.clone();
        let self4 = self.clone();

        let deployment_id1 = deployment_id.clone();
        let deployment_id2 = deployment_id.clone();

        Box::new(
            future::result(self.dynamic_data_sources_query(deployment_id))
                .and_then(move |query| self1.query_dynamic_data_sources(deployment_id1, query))
                .and_then(move |query_result| {
                    self2.parse_data_sources(deployment_id2, query_result)
                })
                .and_then(move |typed_entities| {
                    self3.convert_to_unresolved_data_sources(typed_entities)
                })
                .and_then(move |unresolved_data_sources| {
                    self4.resolve_data_sources(unresolved_data_sources)
                }),
        )
    }
}
