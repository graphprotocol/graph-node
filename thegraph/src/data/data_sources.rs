use serde_yaml;

#[derive(Debug, PartialEq, Deserialize)]
pub struct Location {
    pub path: String,
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct DataStructure {
    abi: String,
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct Data {
    pub kind: String,
    pub name: String,
    pub address: String,
    pub structure: DataStructure,
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct MappingConfigValue {
    pub name: String,
    pub value: serde_yaml::Value,
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct Mapping {
    pub kind: String,
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    pub config: Vec<MappingConfigValue>,
    pub source: Location,
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct DataSet {
    pub data: Data,
    pub mapping: Mapping,
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct DataSourceDefinition {
    #[serde(rename = "specVersion")]
    pub spec_version: String,
    pub schema: String,
    pub datasets: Vec<DataSet>,
}
