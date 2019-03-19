use graphql_parser::{query as q, schema as s};
use std::collections::HashMap;
use std::result::Result;
use std::time::{Duration, Instant};

use graph::prelude::*;

use crate::execution::*;
use crate::prelude::*;
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
}

pub fn execute_subscription<R>(
    subscription: &Subscription,
    options: SubscriptionExecutionOptions<R>,
) -> Result<SubscriptionResult, SubscriptionError>
where
    R: Resolver + 'static,
{
    info!(options.logger, "Execute subscription");

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

    // Create an introspection type store and resolver
    let introspection_schema = introspection_schema();
    let introspection_resolver =
        IntrospectionResolver::new(&options.logger, &subscription.query.schema);

    // Create a fresh execution context
    let ctx = ExecutionContext {
        logger: options.logger,
        resolver: Arc::new(options.resolver),
        schema: &subscription.query.schema,
        introspection_resolver: Arc::new(introspection_resolver),
        introspection_schema: &introspection_schema,
        introspecting: false,
        document: &subscription.query.document,
        fields: vec![],
        variable_values: Arc::new(coerced_variable_values),
        deadline: None,
    };

    match *operation {
        // Execute top-level `subscription { ... }` expressions
        q::OperationDefinition::Subscription(ref subscription) => {
            let source_stream = create_source_event_stream(&ctx, subscription)?;
            let response_stream =
                map_source_to_response_stream(&ctx, subscription, source_stream, options.timeout)?;
            Ok(response_stream)
        }

        // Everything else (queries, mutations) is unsupported
        _ => Err(SubscriptionError::from(QueryExecutionError::NotSupported(
            "Only subscriptions are supported".to_string(),
        ))),
    }
}

fn create_source_event_stream<'a, R1, R2>(
    ctx: &'a ExecutionContext<'a, R1, R2>,
    operation: &q::Subscription,
) -> Result<StoreEventStreamBox, SubscriptionError>
where
    R1: Resolver,
    R2: Resolver,
{
    let subscription_type = sast::get_root_subscription_type(&ctx.schema.document)
        .ok_or(QueryExecutionError::NoRootSubscriptionObjectType)?;

    let grouped_field_set = collect_fields(
        ctx.clone(),
        &subscription_type,
        &operation.selection_set,
        None,
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
    let argument_values = coerce_argument_values(&ctx, subscription_type, field)?;

    resolve_field_stream(ctx, subscription_type, field, argument_values)
}

fn resolve_field_stream<'a, R1, R2>(
    ctx: &'a ExecutionContext<'a, R1, R2>,
    object_type: &'a s::ObjectType,
    field: &'a q::Field,
    _argument_values: HashMap<&q::Name, q::Value>,
) -> Result<StoreEventStreamBox, SubscriptionError>
where
    R1: Resolver,
    R2: Resolver,
{
    ctx.resolver
        .resolve_field_stream(&ctx.schema.document, object_type, field)
        .map_err(SubscriptionError::from)
}

fn map_source_to_response_stream<'a, R1, R2>(
    ctx: &ExecutionContext<'a, R1, R2>,
    subscription: &'a q::Subscription,
    source_stream: StoreEventStreamBox,
    timeout: Option<Duration>,
) -> Result<QueryResultStream, SubscriptionError>
where
    R1: Resolver + 'static,
    R2: Resolver,
{
    let logger = ctx.logger.clone();
    let resolver = ctx.resolver.clone();
    let schema = ctx.schema.clone();
    let document = ctx.document.clone();
    let subscription = subscription.to_owned();
    let variable_values = ctx.variable_values.clone();

    Ok(Box::new(source_stream.map(move |event| {
        execute_subscription_event(
            logger.clone(),
            resolver.clone(),
            schema.clone(),
            document.clone(),
            subscription.clone(),
            variable_values.clone(),
            event,
            timeout.clone(),
        )
    })))
}

fn execute_subscription_event<R1>(
    logger: Logger,
    resolver: Arc<R1>,
    schema: Schema,
    document: q::Document,
    subscription: q::Subscription,
    variable_values: Arc<HashMap<q::Name, q::Value>>,
    event: StoreEvent,
    timeout: Option<Duration>,
) -> QueryResult
where
    R1: Resolver + 'static,
{
    debug!(logger, "Execute subscription event"; "event" => format!("{:?}", event));

    // Create an introspection type store and resolver
    let introspection_schema = introspection_schema();
    let introspection_resolver = IntrospectionResolver::new(&logger, &schema);

    // Create a fresh execution context with deadline.
    let ctx = ExecutionContext {
        logger: logger,
        resolver: resolver,
        schema: &schema,
        introspection_resolver: Arc::new(introspection_resolver),
        introspection_schema: &introspection_schema,
        introspecting: false,
        document: &document,
        fields: vec![],
        variable_values,
        deadline: timeout.map(|t| Instant::now() + t),
    };

    // We have established that this exists earlier in the subscription execution
    let subscription_type = sast::get_root_subscription_type(&ctx.schema.document).unwrap();

    let result = execute_selection_set(ctx, &subscription.selection_set, subscription_type, &None);

    match result {
        Ok(value) => QueryResult::new(Some(value)),
        Err(e) => QueryResult::from(e),
    }
}
