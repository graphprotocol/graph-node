#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Location {
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct DataStructure {
    abi: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Data {
    pub kind: String,
    pub name: String,
    pub address: String,
    pub structure: DataStructure,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct MappingABI {
    pub name: String,
    pub source: Location,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct MappingEventHandler {
    pub event: String,
    pub handler: String,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct Mapping {
    pub kind: String,
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    pub abis: Vec<MappingABI>,
    #[serde(rename = "eventHandlers")]
    pub event_handlers: Vec<MappingEventHandler>,
    pub source: Location,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct DataSet {
    pub data: Data,
    pub mapping: Mapping,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct DataSourceDefinition {
    pub location: String,
    #[serde(rename = "specVersion")]
    pub spec_version: String,
    pub schema: String,
    pub datasets: Vec<DataSet>,
}
