use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct Location {
    pub path: PathBuf,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct DataStructure {
    pub abi: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct Data {
    pub kind: String,
    pub name: String,
    pub address: String,
    pub structure: DataStructure,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct MappingABI {
    pub name: String,
    pub source: Location,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct MappingEventHandler {
    pub event: String,
    pub handler: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
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

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct DataSet {
    pub data: Data,
    pub mapping: Mapping,
}

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct DataSourceDefinition {
    pub id: String,
    pub location: String,
    #[serde(rename = "specVersion")]
    pub spec_version: String,
    pub schema: String,
    pub datasets: Vec<DataSet>,
}

impl DataSourceDefinition {
    pub fn resolve_path(&self, path: &Path) -> PathBuf {
        let data_source_file = PathBuf::from(self.location.as_str());
        let parent = data_source_file.parent();
        let is_relative = path.is_relative() || path.starts_with("./");
        match (is_relative, parent) {
            (true, Some(parent_path)) if parent_path.is_dir() => parent_path.join(path),
            _ => path.clone().to_path_buf(),
        }
    }
}
