use std::collections::HashMap;
use std::iter;
use std::result::Result;
use std::time::{Duration, Instant};

use graph::{components::store::SubscriptionManager, prelude::*};

use crate::{
    execution::*,
    prelude::{BlockConstraint, StoreResolver},
    schema::api::ErrorPolicy,
};

/// Options available for subscription execution.
pub struct SubscriptionExecutionOptions {
    /// The logger to use during subscription execution.
    pub logger: Logger,

    /// The store to use.
    pub store: Arc<dyn QueryStore>,

    pub subscription_manager: Arc<dyn SubscriptionManager>,

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

pub fn execute_subscription(
    subscription: Subscription,
    schema: Arc<ApiSchema>,
    options: SubscriptionExecutionOptions,
) -> Result<SubscriptionResult, SubscriptionError> {
    let query = crate::execution::Query::new(
        &options.logger,
        schema,
        None,
        subscription.query,
        options.max_complexity,
        options.max_depth,
    )?;
    execute_prepared_subscription(query, options)
}

pub(crate) fn execute_prepared_subscription(
    query: Arc<crate::execution::Query>,
    options: SubscriptionExecutionOptions,
) -> Result<SubscriptionResult, SubscriptionError> {
    if !query.is_subscription() {
        return Err(SubscriptionError::from(QueryExecutionError::NotSupported(
            "Only subscriptions are supported".to_string(),
        )));
    }

    info!(
        options.logger,
        "Execute subscription";
        "query" => &query.query_text,
    );

    let source_stream = create_source_event_stream(query.clone(), &options)?;
    let response_stream = map_source_to_response_stream(query, options, source_stream);
    Ok(response_stream)
}

fn create_source_event_stream(
    query: Arc<crate::execution::Query>,
    options: &SubscriptionExecutionOptions,
) -> Result<StoreEventStreamBox, SubscriptionError> {
    let resolver = StoreResolver::for_subscription(
        &options.logger,
        query.schema.id().clone(),
        options.store.clone(),
        options.subscription_manager.cheap_clone(),
    );
    let ctx = ExecutionContext {
        logger: options.logger.cheap_clone(),
        resolver,
        query: query.clone(),
        deadline: None,
        max_first: options.max_first,
        max_skip: options.max_skip,
        cache_status: Default::default(),
        load_manager: options.load_manager.cheap_clone(),
        nested_resolver: false,
    };

    let subscription_type = ctx
        .query
        .schema
        .subscription_type
        .as_ref()
        .ok_or(QueryExecutionError::NoRootSubscriptionObjectType)?;

    let grouped_field_set = collect_fields(
        &ctx,
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
    let argument_values = coerce_argument_values(&ctx.query, subscription_type.as_ref(), field)?;

    resolve_field_stream(&ctx, &subscription_type, field, argument_values)
}

fn resolve_field_stream(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    field: &q::Field,
    _argument_values: HashMap<&str, q::Value>,
) -> Result<StoreEventStreamBox, SubscriptionError> {
    ctx.resolver
        .resolve_field_stream(&ctx.query.schema.document(), object_type, field)
        .map_err(SubscriptionError::from)
}

fn map_source_to_response_stream(
    query: Arc<crate::execution::Query>,
    options: SubscriptionExecutionOptions,
    source_stream: StoreEventStreamBox,
) -> QueryResultStream {
    // Create a stream with a single empty event. By chaining this in front
    // of the real events, we trick the subscription into executing its query
    // at least once. This satisfies the GraphQL over Websocket protocol
    // requirement of "respond[ing] with at least one GQL_DATA message", see
    // https://github.com/apollographql/subscriptions-transport-ws/blob/master/PROTOCOL.md#gql_data
    let trigger_stream = futures03::stream::iter(vec![Ok(Arc::new(StoreEvent {
        tag: 0,
        changes: Default::default(),
    }))]);

    let SubscriptionExecutionOptions {
        logger,
        store,
        subscription_manager,
        timeout,
        max_complexity: _,
        max_depth: _,
        max_first,
        max_skip,
        load_manager,
    } = options;

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
                    store.clone(),
                    subscription_manager.cheap_clone(),
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
    store: Arc<dyn QueryStore>,
    subscription_manager: Arc<dyn SubscriptionManager>,
    query: Arc<crate::execution::Query>,
    event: Arc<StoreEvent>,
    timeout: Option<Duration>,
    max_first: u32,
    max_skip: u32,
    load_manager: Arc<dyn QueryLoadManager>,
) -> Arc<QueryResult> {
    debug!(logger, "Execute subscription event"; "event" => format!("{:?}", event));

    let resolver = match StoreResolver::at_block(
        &logger,
        store,
        subscription_manager,
        BlockConstraint::Latest,
        ErrorPolicy::Deny,
        query.schema.id().clone(),
    )
    .await
    {
        Ok(resolver) => resolver,
        Err(e) => return Arc::new(e.into()),
    };

    let block_ptr = resolver.block_ptr.clone();

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
        block_ptr,
    )
    .await
}
