/// Traits and types for all system components.
pub mod components;

/// Common data types used throughout The Graph.
pub mod data;

/// Utilities.
pub mod util;

/// Extension traits for external types.
pub mod ext;

/// Logging utilities
pub mod log;

/// Module with mocks for different parts of the system.
pub mod mock {
    pub use crate::components::ethereum::MockEthereumAdapter;
}

/// A prelude that makes all system component traits and data types available.
///
/// Add the following code to import all traits and data types listed below at once.
///
/// ```
/// use graph::prelude::*;
/// ```
pub mod prelude {
    pub use bigdecimal;
    pub use ethabi;
    pub use failure::{self, bail, err_msg, format_err, Error, Fail, SyncFailure};
    pub use hex;
    pub use serde_derive::{Deserialize, Serialize};
    pub use serde_json;
    pub use slog::{self, crit, debug, error, info, o, trace, warn, Logger};
    pub use std::fmt::Debug;
    pub use std::iter::FromIterator;
    pub use std::sync::Arc;
    pub use tiny_keccak;
    pub use tokio;
    pub use tokio::prelude::*;
    pub use tokio_executor;
    pub use tokio_timer;
    pub use web3;

    pub use crate::components::ethereum::{
        BlockFinality, BlockStream, BlockStreamBuilder, BlockStreamMetrics, ChainHeadUpdate,
        ChainHeadUpdateListener, ChainHeadUpdateStream, EthereumAdapter, EthereumAdapterError,
        EthereumBlock, EthereumBlockData, EthereumBlockFilter, EthereumBlockPointer,
        EthereumBlockTriggerType, EthereumBlockWithCalls, EthereumBlockWithTriggers, EthereumCall,
        EthereumCallData, EthereumCallFilter, EthereumContractCall, EthereumContractCallError,
        EthereumEventData, EthereumLogFilter, EthereumNetworkIdentifier, EthereumTransactionData,
        EthereumTrigger, LightEthereumBlock, LightEthereumBlockExt, ProviderEthRpcMetrics,
        SubgraphEthRpcMetrics,
    };
    pub use crate::components::graphql::{
        GraphQlRunner, QueryResultFuture, SubscriptionResultFuture,
    };
    pub use crate::components::link_resolver::{JsonStreamValue, JsonValueStream, LinkResolver};
    pub use crate::components::metrics::{
        aggregate::Aggregate, stopwatch::StopwatchMetrics, Collector, Counter, CounterVec, Gauge,
        GaugeVec, Histogram, HistogramOpts, HistogramVec, MetricsRegistry, Opts, PrometheusError,
        Registry,
    };
    pub use crate::components::server::admin::JsonRpcServer;
    pub use crate::components::server::index_node::IndexNodeServer;
    pub use crate::components::server::metrics::MetricsServer;
    pub use crate::components::server::query::GraphQLServer;
    pub use crate::components::server::subscription::SubscriptionServer;
    pub use crate::components::store::{
        AttributeIndexDefinition, ChainStore, EntityCache, EntityChange, EntityChangeOperation,
        EntityFilter, EntityKey, EntityModification, EntityOperation, EntityOrder, EntityQuery,
        EntityRange, EthereumCallCache, MetadataOperation, Store, StoreError, StoreEvent,
        StoreEventStream, StoreEventStreamBox, SubgraphDeploymentStore, TransactionAbortError,
        SUBSCRIPTION_THROTTLE_INTERVAL,
    };
    pub use crate::components::subgraph::{
        BlockState, DataSourceLoader, DataSourceTemplateInfo, HostMetrics, RuntimeHost,
        RuntimeHostBuilder, SubgraphAssignmentProvider, SubgraphInstance, SubgraphInstanceManager,
        SubgraphRegistrar, SubgraphVersionSwitchingMode,
    };
    pub use crate::components::{EventConsumer, EventProducer};

    pub use crate::data::graphql::{SerializableValue, TryFromValue, ValueMap};
    pub use crate::data::query::{
        Query, QueryError, QueryExecutionError, QueryResult, QueryVariables,
    };
    pub use crate::data::schema::Schema;
    pub use crate::data::store::ethereum::*;
    pub use crate::data::store::scalar::{BigDecimal, BigInt, BigIntSign};
    pub use crate::data::store::{
        AssignmentEvent, Attribute, Entity, NodeId, SubgraphEntityPair, SubgraphVersionSummary,
        Value, ValueType,
    };
    pub use crate::data::subgraph::schema::{SubgraphDeploymentEntity, TypedEntity};
    pub use crate::data::subgraph::{
        BlockHandlerFilter, CreateSubgraphResult, DataSource, DataSourceTemplate, Link, MappingABI,
        MappingBlockHandler, MappingCallHandler, MappingEventHandler,
        SubgraphAssignmentProviderError, SubgraphAssignmentProviderEvent, SubgraphDeploymentId,
        SubgraphManifest, SubgraphManifestResolveError, SubgraphManifestValidationError,
        SubgraphName, SubgraphRegistrarError,
    };
    pub use crate::data::subscription::{
        QueryResultStream, Subscription, SubscriptionError, SubscriptionResult,
    };
    pub use crate::ext::futures::{
        CancelGuard, CancelHandle, CancelableError, FutureExtension, SharedCancelGuard,
        StreamExtension,
    };
    pub use crate::log::codes::LogCode;
    pub use crate::log::elastic::{elastic_logger, ElasticDrainConfig, ElasticLoggingConfig};
    pub use crate::log::factory::{
        ComponentLoggerConfig, ElasticComponentLoggerConfig, LoggerFactory,
    };
    pub use crate::log::split::split_logger;
    pub use crate::util::futures::retry;
}
