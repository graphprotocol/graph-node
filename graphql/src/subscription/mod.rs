use graphql_parser::{query as q, schema as s, Style};
use std::collections::HashMap;
use std::result::Result;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use graph::prelude::*;

use crate::execution::*;
use crate::query::ast as qast;
use crate::schema::ast as sast;

use lazy_static::lazy_static;

lazy_static! {
    static ref SUBSCRIPTION_QUERY_SEMAPHORE: Semaphore = {
        // This is duplicating the logic in main.rs to get the connection pool size, which is
        // unfortunate. But because this module has no share state otherwise, it's not simple to
        // refactor so that the semaphore isn't a global.
        // See also 82d5dad6-b633-4350-86d9-70c8b2e65805
        let db_conn_pool_size = std::env::var("STORE_CONNECTION_POOL_SIZE")
            .unwrap_or("10".into())
            .parse::<usize>()
            .expect("invalid STORE_CONNECTION_POOL_SIZE");

        // Limit the amount of connections that can be taken up by subscription queries.
        Semaphore::new((0.7 * db_conn_pool_size as f64).ceil() as usize)
    };
}

/// Options available for subscription execution.
pub struct SubscriptionExecutionOptions<R>
where
    R: Resolver,
{
    /// The logger to use during subscription execution.
    pub logger: Logger,

    /// The resolver to use.
    pub resolver: R,

    /// Individual timeout for each subscription query.
    pub timeout: Option<Duration>,

    /// Maximum complexity for a subscription query.
    pub max_complexity: Option<u64>,

    /// Maximum depth for a subscription query.
    pub max_depth: u8,

    /// Maximum value for the `first` argument.
    pub max_first: u32,
}

pub fn execute_subscription<R>(
    subscription: &Subscription,
    options: SubscriptionExecutionOptions<R>,
) -> Result<SubscriptionResult, SubscriptionError>
where
    R: Resolver + 'static,
{
    // Obtain the only operation of the subscription (fail if there is none or more than one)
    let operation = qast::get_operation(&subscription.query.document, None)?;

    // Parse variable values
    let coerced_variable_values = match coerce_variable_values(
        &subscription.query.schema,
        operation,
        &subscription.query.variables,
    ) {
        Ok(values) => values,
        Err(errors) => return Err(SubscriptionError::from(errors)),
    };

    // Create a fresh execution context
    let ctx = ExecutionContext {
        logger: options.logger,
        resolver: Arc::new(options.resolver),
        schema: subscription.query.schema.clone(),
        document: subscription.query.document.clone(),
        fields: vec![],
        variable_values: Arc::new(coerced_variable_values),
        deadline: None,
        max_first: options.max_first,
        block: BLOCK_NUMBER_MAX,
        mode: ExecutionMode::Prefetch,
    };

    match operation {
        // Execute top-level `subscription { ... }` expressions
        q::OperationDefinition::Subscription(q::Subscription { selection_set, .. }) => {
            let root_type = sast::get_root_query_type_def(&ctx.schema.document).unwrap();
            let validation_errors =
                ctx.validate_fields(&"Query".to_owned(), root_type, selection_set);
            if !validation_errors.is_empty() {
                return Err(SubscriptionError::from(validation_errors));
            }

            let complexity = ctx
                .root_query_complexity(root_type, selection_set, options.max_depth)
                .map_err(|e| vec![e])?;

            info!(
                ctx.logger,
                "Execute subscription";
                "query" => ctx.document.format(&Style::default().indent(0)).replace('\n', " "),
                "complexity" => complexity,
            );

            match options.max_complexity {
                Some(max_complexity) if complexity > max_complexity => {
                    Err(vec![QueryExecutionError::TooComplex(complexity, max_complexity)].into())
                }
                _ => {
                    let source_stream = create_source_event_stream(&ctx, selection_set)?;
                    let response_stream = map_source_to_response_stream(
                        &ctx,
                        selection_set,
                        source_stream,
                        options.timeout,
                    );
                    Ok(response_stream)
                }
            }
        }

        // Everything else (queries, mutations) is unsupported
        _ => Err(SubscriptionError::from(QueryExecutionError::NotSupported(
            "Only subscriptions are supported".to_string(),
        ))),
    }
}

fn create_source_event_stream(
    ctx: &ExecutionContext<impl Resolver>,
    selection_set: &q::SelectionSet,
) -> Result<StoreEventStreamBox, SubscriptionError> {
    let subscription_type = sast::get_root_subscription_type(&ctx.schema.document)
        .ok_or(QueryExecutionError::NoRootSubscriptionObjectType)?;

    let grouped_field_set = collect_fields(ctx, &subscription_type, &selection_set, None);

    if grouped_field_set.is_empty() {
        return Err(SubscriptionError::from(QueryExecutionError::EmptyQuery));
    } else if grouped_field_set.len() > 1 {
        return Err(SubscriptionError::from(
            QueryExecutionError::MultipleSubscriptionFields,
        ));
    }

    let fields = grouped_field_set.get_index(0).unwrap();
    let field = fields.1[0];
    let argument_values = coerce_argument_values(&ctx, subscription_type, field)?;

    resolve_field_stream(ctx, subscription_type, field, argument_values)
}

fn resolve_field_stream(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    field: &q::Field,
    _argument_values: HashMap<&q::Name, q::Value>,
) -> Result<StoreEventStreamBox, SubscriptionError> {
    ctx.resolver
        .resolve_field_stream(&ctx.schema.document, object_type, field)
        .map_err(SubscriptionError::from)
}

fn map_source_to_response_stream(
    ctx: &ExecutionContext<impl Resolver + 'static>,
    selection_set: &q::SelectionSet,
    source_stream: StoreEventStreamBox,
    timeout: Option<Duration>,
) -> QueryResultStream {
    let logger = ctx.logger.clone();
    let resolver = ctx.resolver.clone();
    let schema = ctx.schema.clone();
    let document = ctx.document.clone();
    let selection_set = selection_set.to_owned();
    let variable_values = ctx.variable_values.clone();
    let max_first = ctx.max_first;

    // Create a stream with a single empty event. By chaining this in front
    // of the real events, we trick the subscription into executing its query
    // at least once. This satisfies the GraphQL over Websocket protocol
    // requirement of "respond[ing] with at least one GQL_DATA message", see
    // https://github.com/apollographql/subscriptions-transport-ws/blob/master/PROTOCOL.md#gql_data
    let trigger_stream = futures03::stream::iter(vec![Ok(StoreEvent {
        tag: 0,
        changes: Default::default(),
    })]);

    Box::new(
        trigger_stream
            .chain(source_stream.compat())
            .then(move |res| match res {
                Err(()) => {
                    futures03::future::ready(QueryExecutionError::EventStreamError.into()).boxed()
                }
                Ok(event) => execute_subscription_event(
                    logger.clone(),
                    resolver.clone(),
                    schema.clone(),
                    document.clone(),
                    selection_set.clone(),
                    variable_values.clone(),
                    event,
                    timeout.clone(),
                    max_first,
                )
                .boxed(),
            }),
    )
}

async fn execute_subscription_event(
    logger: Logger,
    resolver: Arc<impl Resolver + 'static>,
    schema: Arc<Schema>,
    document: q::Document,
    selection_set: q::SelectionSet,
    variable_values: Arc<HashMap<q::Name, q::Value>>,
    event: StoreEvent,
    timeout: Option<Duration>,
    max_first: u32,
) -> QueryResult {
    debug!(logger, "Execute subscription event"; "event" => format!("{:?}", event));

    // Create a fresh execution context with deadline.
    let ctx = ExecutionContext {
        logger,
        resolver,
        schema,
        document,
        fields: vec![],
        variable_values,
        deadline: timeout.map(|t| Instant::now() + t),
        max_first,
        block: BLOCK_NUMBER_MAX,
        mode: ExecutionMode::Prefetch,
    };

    // We have established that this exists earlier in the subscription execution
    let subscription_type = sast::get_root_subscription_type(&ctx.schema.document)
        .unwrap()
        .clone();

    // Use a semaphore to prevent subscription queries, which can be numerous and might query all at
    // once, from flooding the blocking thread pool and the DB connection pool.
    let _permit = SUBSCRIPTION_QUERY_SEMAPHORE.acquire();
    let result = graph::spawn_blocking_allow_panic(async move {
        execute_selection_set(&ctx, &selection_set, &subscription_type, &None)
    })
    .await
    .map_err(|e| vec![QueryExecutionError::Panic(e.to_string())])
    .and_then(|x| x);

    match result {
        Ok(value) => QueryResult::new(Some(value)),
        Err(e) => QueryResult::from(e),
    }
}
