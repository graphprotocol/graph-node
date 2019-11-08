use graphql_parser::{query as q, schema as s, Style};
use std::collections::HashMap;
use std::result::Result;
use std::time::{Duration, Instant};

use graph::prelude::*;

use crate::execution::*;
use crate::query::ast as qast;
use crate::schema::ast as sast;

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
        document: &subscription.query.document,
        fields: vec![],
        variable_values: Arc::new(coerced_variable_values),
        deadline: None,
        max_first: options.max_first,
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
                    )?;
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

fn create_source_event_stream<'a, R>(
    ctx: &'a ExecutionContext<'a, R>,
    selection_set: &q::SelectionSet,
) -> Result<StoreEventStreamBox, SubscriptionError>
where
    R: Resolver,
{
    let subscription_type = sast::get_root_subscription_type(&ctx.schema.document)
        .ok_or(QueryExecutionError::NoRootSubscriptionObjectType)?;

    let grouped_field_set = collect_fields(ctx.clone(), &subscription_type, &selection_set, None);

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

fn resolve_field_stream<'a, R>(
    ctx: &'a ExecutionContext<'a, R>,
    object_type: &'a s::ObjectType,
    field: &'a q::Field,
    _argument_values: HashMap<&q::Name, q::Value>,
) -> Result<StoreEventStreamBox, SubscriptionError>
where
    R: Resolver,
{
    ctx.resolver
        .resolve_field_stream(&ctx.schema.document, object_type, field)
        .map_err(SubscriptionError::from)
}

fn map_source_to_response_stream<'a, R>(
    ctx: &ExecutionContext<'a, R>,
    selection_set: &'a q::SelectionSet,
    source_stream: StoreEventStreamBox,
    timeout: Option<Duration>,
) -> Result<QueryResultStream, SubscriptionError>
where
    R: Resolver + 'static,
{
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
    let trigger_stream = stream::iter_ok(vec![StoreEvent {
        tag: 0,
        changes: Default::default(),
    }]);

    Ok(Box::new(trigger_stream.chain(source_stream).map(
        move |event| {
            execute_subscription_event(
                logger.clone(),
                resolver.clone(),
                schema.clone(),
                document.clone(),
                &selection_set,
                variable_values.clone(),
                event,
                timeout.clone(),
                max_first,
            )
        },
    )))
}

fn execute_subscription_event<R1>(
    logger: Logger,
    resolver: Arc<R1>,
    schema: Arc<Schema>,
    document: q::Document,
    selection_set: &q::SelectionSet,
    variable_values: Arc<HashMap<q::Name, q::Value>>,
    event: StoreEvent,
    timeout: Option<Duration>,
    max_first: u32,
) -> QueryResult
where
    R1: Resolver + 'static,
{
    debug!(logger, "Execute subscription event"; "event" => format!("{:?}", event));

    // Create a fresh execution context with deadline.
    let ctx = ExecutionContext {
        logger: logger,
        resolver: resolver,
        schema: schema,
        document: &document,
        fields: vec![],
        variable_values,
        deadline: timeout.map(|t| Instant::now() + t),
        max_first,
    };

    // We have established that this exists earlier in the subscription execution
    let subscription_type = sast::get_root_subscription_type(&ctx.schema.document).unwrap();

    let result = execute_selection_set(&ctx, selection_set, subscription_type, &None);

    match result {
        Ok(value) => QueryResult::new(Some(value)),
        Err(e) => QueryResult::from(e),
    }
}
