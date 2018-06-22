use components::EventConsumer;

use super::DataSourceProviderEvent;

/// A `RuntimeManager` manages data source runtimes based on which data sources
/// are available in the system. These are provided through
/// `DataSourceProviderEvent`s.
///
/// When a data source is added, the runtime manager creates and starts
/// one or more runtime hosts for the data source. When a data source is removed,
/// the runtime manager stops and removes the runtime hosts for this data source.
pub trait RuntimeManager: EventConsumer<DataSourceProviderEvent> {}
