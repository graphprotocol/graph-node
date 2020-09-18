use std::collections::HashMap;
use std::iter::FromIterator;
use std::time::Instant;

use async_trait::async_trait;

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
        //
        // See also: ed42d219c6704a4aab57ce1ea66698e7.
        // Note: This query needs to be in sync with the metadata schema.
        let document = parse_query(
            r#"
            query deployment($id: ID!, $skip: Int!) {
              subgraphDeployment(id: $id) {
                dynamicDataSources(orderBy: id, skip: $skip) {
                  kind
                  network
                  name
                  context
                  source { address abi }
                  mapping {
                    kind
                    apiVersion
                    language
                    file
                    entities
                    abis { name file }
                    blockHandlers { handler filter }
                    callHandlers {  function handler }
                    eventHandlers { event handler topic0 }
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
                      blockHandlers { handler filter }
                      callHandlers { function handler }
                      eventHandlers { event handler topic0 }
                    }
                  }
                }
              }
            }
            "#,
        )
        .expect("invalid query for dynamic data sources");
        let variables = Some(QueryVariables::new(HashMap::from_iter(
            vec![
                (String::from("id"), q::Value::String(deployment.to_string())),
                (String::from("skip"), q::Value::Int(skip.into())),
            ]
            .into_iter(),
        )));

        Ok(Query::new(schema, document, variables, None))
    }

    fn parse_data_sources(
        &self,
        deployment_id: &SubgraphDeploymentId,
        query_result: q::Value,
    ) -> Result<Vec<UnresolvedDataSource>, Error> {
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
            entities.push(UnresolvedDataSource::try_from_value(value)?);
            Ok(entities)
        });

        entities.map_err(|e: Error| {
            format_err!(
                "Failed to parse dynamic data source entities of deployment `{}`: {}",
                deployment_id,
                e
            )
        })
    }

    async fn resolve_data_sources(
        &self,
        unresolved_data_sources: Vec<UnresolvedDataSource>,
        logger: &Logger,
    ) -> Result<Vec<DataSource>, Error> {
        // Resolve the data sources and return them
        let mut result = Vec::new();
        for item in unresolved_data_sources.into_iter() {
            let resolved = item.resolve(&*self.link_resolver, logger).await?;
            result.push(resolved);
        }
        Ok(result)
    }
}

#[async_trait]
impl<L, Q, S> DataSourceLoaderTrait for DataSourceLoader<L, Q, S>
where
    L: LinkResolver,
    Q: GraphQlRunner,
    S: Store + SubgraphDeploymentStore,
{
    async fn load_dynamic_data_sources(
        &self,
        deployment_id: SubgraphDeploymentId,
        logger: Logger,
    ) -> Result<Vec<DataSource>, Error> {
        let start_time = Instant::now();

        let mut data_sources = vec![];

        loop {
            let skip = data_sources.len() as i32;
            let query = self.dynamic_data_sources_query(&deployment_id, skip)?;
            let query_result = self
                .graphql_runner
                .cheap_clone()
                .query_metadata(query)
                .await?;
            let unresolved_data_sources =
                self.parse_data_sources(&deployment_id, query_result.as_ref().clone())?;
            let next_data_sources = self
                .resolve_data_sources(unresolved_data_sources, &logger)
                .await?;

            if next_data_sources.is_empty() {
                break;
            }

            data_sources.extend(next_data_sources);
        }

        trace!(
            logger,
            "Loaded dynamic data sources";
            "ms" => start_time.elapsed().as_millis()
        );

        Ok(data_sources)
    }
}
