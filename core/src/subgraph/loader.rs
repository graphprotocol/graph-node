use std::collections::HashMap;
use std::iter::FromIterator;
use std::time::Instant;

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
    pub fn new(store: Arc<S>, link_resolver: Arc<L>, graphql_runner: Arc<Q>) -> Self {
        Self {
            store,
            link_resolver,
            graphql_runner,
        }
    }

    fn dynamic_data_sources_query(
        &self,
        deployment: &SubgraphDeploymentId,
        skip: i32,
    ) -> Result<Query, Error> {
        // Obtain the "subgraphs" schema
        let schema = self.store.api_schema(&SUBGRAPHS_ID)?;

        // Construct a query for the subgraph deployment and all its
        // dynamic data sources
        Ok(Query {
            schema,
            document: parse_query(
                r#"
                query deployment($id: ID!, $skip: Int!) {
                  subgraphDeployment(id: $id) {
                    dynamicDataSources(orderBy: id, skip: $skip) {
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
                        blockHandlers { handler filter}
                        callHandlers {  function handler}
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
                          blockHandlers { handler filter}
                          callHandlers { function handler}
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
                vec![
                    (String::from("id"), q::Value::String(deployment.to_string())),
                    (String::from("skip"), q::Value::Int(skip.into())),
                ]
                .into_iter(),
            ))),
        })
    }

    fn query_dynamic_data_sources(
        &self,
        deployment_id: SubgraphDeploymentId,
        query: Query,
    ) -> impl Future<Item = q::Value, Error = Error> + Send {
        let deployment_id1 = deployment_id.clone();

        self.graphql_runner
            .run_query_with_complexity(query, None, None, None)
            .map_err(move |e| {
                format_err!(
                    "Failed to query subgraph deployment `{}`: {}",
                    deployment_id1,
                    e
                )
            })
            .and_then(move |result| {
                if result.errors.is_some() {
                    Err(format_err!(
                        "Failed to query subgraph deployment `{}`: {:?}",
                        deployment_id,
                        result.errors
                    ))
                } else {
                    result.data.ok_or_else(|| {
                        format_err!("No data found for subgraph deployment `{}`", deployment_id)
                    })
                }
            })
    }

    fn parse_data_sources(
        &self,
        deployment_id: SubgraphDeploymentId,
        query_result: q::Value,
    ) -> Result<Vec<EthereumContractDataSourceEntity>, Error> {
        let data = match query_result {
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
        let entities = values.iter().try_fold(vec![], |mut entities, value| {
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
        &self,
        entities: Vec<EthereumContractDataSourceEntity>,
    ) -> Vec<UnresolvedDataSource> {
        // Turn the entities into unresolved data sources
        entities
            .into_iter()
            .map(Into::into)
            .collect::<Vec<UnresolvedDataSource>>()
    }

    fn resolve_data_sources(
        self: Arc<Self>,
        unresolved_data_sources: Vec<UnresolvedDataSource>,
        logger: Logger,
    ) -> impl Future<Item = Vec<DataSource>, Error = Error> + Send {
        // Resolve the data sources and return them
        stream::iter_ok(unresolved_data_sources).fold(vec![], move |mut resolved, data_source| {
            data_source
                .resolve(&*self.link_resolver, logger.clone())
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
        logger: Logger,
    ) -> Box<dyn Future<Item = Vec<DataSource>, Error = Error> + Send> {
        struct LoopState {
            data_sources: Vec<DataSource>,
            skip: i32,
        }

        let start_time = Instant::now();
        let initial_state = LoopState {
            data_sources: vec![],
            skip: 0,
        };

        // Clones for async looping
        let self1 = self.clone();
        let deployment_id = deployment_id.clone();
        let timing_logger = logger.clone();

        Box::new(
            future::loop_fn(initial_state, move |mut state| {
                let logger = logger.clone();

                let deployment_id1 = deployment_id.clone();
                let deployment_id2 = deployment_id.clone();
                let deployment_id3 = deployment_id.clone();

                let self2 = self1.clone();
                let self3 = self1.clone();
                let self4 = self1.clone();
                let self5 = self1.clone();
                let self6 = self1.clone();

                future::result(self2.dynamic_data_sources_query(&deployment_id1, state.skip))
                    .and_then(move |query| self3.query_dynamic_data_sources(deployment_id2, query))
                    .and_then(move |query_result| {
                        self4.parse_data_sources(deployment_id3, query_result)
                    })
                    .and_then(move |typed_entities| {
                        future::ok(self5.convert_to_unresolved_data_sources(typed_entities))
                    })
                    .and_then(move |unresolved_data_sources| {
                        self6.resolve_data_sources(unresolved_data_sources, logger)
                    })
                    .map(move |data_sources| {
                        if data_sources.is_empty() {
                            future::Loop::Break(state)
                        } else {
                            state.skip += data_sources.len() as i32;
                            state.data_sources.extend(data_sources);
                            future::Loop::Continue(state)
                        }
                    })
            })
            .map(move |state| {
                trace!(
                    timing_logger,
                    "Loaded dynamic data sources";
                    "ms" => start_time.elapsed().as_millis()
                );
                state.data_sources
            }),
        )
    }
}
