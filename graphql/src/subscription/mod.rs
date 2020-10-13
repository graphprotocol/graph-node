use graphql_parser::{query as q, schema as s};
use std::collections::HashMap;
use std::iter;
use std::result::Result;
use std::time::{Duration, Instant};

use graph::prelude::*;

use crate::execution::*;

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

    /// Maximum value for the `skip` argument.
    pub max_skip: u32,

    pub load_manager: Arc<dyn QueryLoadManager>,
}

pub fn execute_subscription<R>(
    subscription: Subscription,
    options: SubscriptionExecutionOptions<R>,
) -> Result<SubscriptionResult, SubscriptionError>
where
    R: Resolver + CheapClone + 'static,
{
    let query = crate::execution::Query::new(
        &options.logger,
        subscription.query,
        options.max_complexity,
        options.max_depth,
    )?;
    execute_prepared_subscription(query, options)
}

pub(crate) fn execute_prepared_subscription<R>(
    query: Arc<crate::execution::Query>,
    options: SubscriptionExecutionOptions<R>,
) -> Result<SubscriptionResult, SubscriptionError>
where
    R: Resolver + CheapClone + 'static,
{
    // Create a fresh execution context
    let ctx = ExecutionContext {
        logger: options.logger,
        resolver: options.resolver,
        query: query.clone(),
        deadline: None,
        max_first: options.max_first,
        max_skip: options.max_skip,
        cache_status: Default::default(),
        load_manager: options.load_manager,
        nested_resolver: false,
    };

    if !query.is_subscription() {
        return Err(SubscriptionError::from(QueryExecutionError::NotSupported(
            "Only subscriptions are supported".to_string(),
        )));
    }

    info!(
        ctx.logger,
        "Execute subscription";
        "query" => &query.query_text,
    );

    let source_stream = create_source_event_stream(&ctx)?;
    let response_stream = map_source_to_response_stream(&ctx, source_stream, options.timeout);
    Ok(response_stream)
}

fn create_source_event_stream(
    ctx: &ExecutionContext<impl Resolver>,
) -> Result<StoreEventStreamBox, SubscriptionError> {
    let subscription_type = ctx
        .query
        .schema
        .subscription_type
        .as_ref()
        .ok_or(QueryExecutionError::NoRootSubscriptionObjectType)?;

    let grouped_field_set = collect_fields(
        ctx,
        &subscription_type,
        iter::once(ctx.query.selection_set.as_ref()),
    );

    if grouped_field_set.is_empty() {
        return Err(SubscriptionError::from(QueryExecutionError::EmptyQuery));
    } else if grouped_field_set.len() > 1 {
        return Err(SubscriptionError::from(
            QueryExecutionError::MultipleSubscriptionFields,
        ));
    }

    let fields = grouped_field_set.get_index(0).unwrap();
    let field = fields.1[0];
    let argument_values = coerce_argument_values(&ctx, &subscription_type, field)?;

    resolve_field_stream(ctx, &subscription_type, field, argument_values)
}

fn resolve_field_stream(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    field: &q::Field,
    _argument_values: HashMap<&q::Name, q::Value>,
) -> Result<StoreEventStreamBox, SubscriptionError> {
    ctx.resolver
        .resolve_field_stream(&ctx.query.schema.document(), object_type, field)
        .map_err(SubscriptionError::from)
}

fn map_source_to_response_stream(
    ctx: &ExecutionContext<impl Resolver + CheapClone + 'static>,
    source_stream: StoreEventStreamBox,
    timeout: Option<Duration>,
) -> QueryResultStream {
    let logger = ctx.logger.cheap_clone();
    let resolver = ctx.resolver.cheap_clone();
    let query = ctx.query.cheap_clone();
    let max_first = ctx.max_first;
    let max_skip = ctx.max_skip;
    let load_manager = ctx.load_manager.cheap_clone();

    // Create a stream with a single empty event. By chaining this in front
    // of the real events, we trick the subscription into executing its query
    // at least once. This satisfies the GraphQL over Websocket protocol
    // requirement of "respond[ing] with at least one GQL_DATA message", see
    // https://github.com/apollographql/subscriptions-transport-ws/blob/master/PROTOCOL.md#gql_data
    let trigger_stream = futures03::stream::iter(vec![Ok(Arc::new(StoreEvent {
        tag: 0,
        changes: Default::default(),
    }))]);

    Box::new(
        trigger_stream
            .chain(source_stream.compat())
            .then(move |res| match res {
                Err(()) => {
                    futures03::future::ready(Arc::new(QueryExecutionError::EventStreamError.into()))
                        .boxed()
                }
                Ok(event) => execute_subscription_event(
                    logger.clone(),
                    resolver.cheap_clone(),
                    query.clone(),
                    event,
                    timeout,
                    max_first,
                    max_skip,
                    load_manager.cheap_clone(),
                )
                .boxed(),
            }),
    )
}

async fn execute_subscription_event(
    logger: Logger,
    resolver: impl Resolver + 'static,
    query: Arc<crate::execution::Query>,
    event: Arc<StoreEvent>,
    timeout: Option<Duration>,
    max_first: u32,
    max_skip: u32,
    load_manager: Arc<dyn QueryLoadManager>,
) -> Arc<QueryResult> {
    debug!(logger, "Execute subscription event"; "event" => format!("{:?}", event));

    // Create a fresh execution context with deadline.
    let ctx = Arc::new(ExecutionContext {
        logger,
        resolver,
        query,
        deadline: timeout.map(|t| Instant::now() + t),
        max_first,
        max_skip,
        cache_status: Default::default(),
        load_manager,
        nested_resolver: false,
    });

    let subscription_type = match ctx.query.schema.subscription_type.as_ref() {
        Some(t) => t.cheap_clone(),
        None => return Arc::new(QueryExecutionError::NoRootSubscriptionObjectType.into()),
    };

    execute_root_selection_set(
        ctx.cheap_clone(),
        ctx.query.selection_set.cheap_clone(),
        subscription_type,
        None,
    )
    .await
}
