use components::EventProducer;
use data::data_sources::DataSourceDefinition;
use data::schema::Schema;

/// Events emitted by [DataSourceProvider](trait.DataSourceProvider.html) implementations.
#[derive(Clone, Debug)]
pub enum DataSourceProviderEvent {
    /// A data source was added to the provider.
    DataSourceAdded(DataSourceDefinition),
    /// A data source was removed from the provider.
    DataSourceRemoved(DataSourceDefinition),
}

/// Schema-only events emitted by a [DataSourceProvider](trait.DataSourceProvider.html).
#[derive(Clone, Debug)]
pub enum SchemaEvent {
    /// A data source with a new schema was added.
    SchemaAdded(Schema),
    /// A data source with an existing schema was removed.
    SchemaRemoved(Schema),
}

/// Common trait for data source providers.
pub trait DataSourceProvider:
    EventProducer<DataSourceProviderEvent> + EventProducer<SchemaEvent>
{
}
