#[derive(Debug, PartialEq, Deserialize)]
pub struct SourcePath {
    pub path: String,
}

#[derive(Debug, PartialEq, Deserialize)]
pub struct SourceDataSet {
    pub data: SourceData,
    pub mapping: SourceMapping,
}
#[derive(Debug, PartialEq, Deserialize)]
pub struct SourceData {
    pub kind: String,
    pub name: String,
    pub address: String,
    pub structure: SourceDataABI,
}
#[derive(Debug, PartialEq, Deserialize)]
pub struct SourceDataABI {
    pub abi: SourcePath,
}
#[derive(Debug, PartialEq, Deserialize)]
pub struct SourceMapping {
    pub kind: String,
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    pub config: MappingConfig,
    pub source: SourcePath,
}
#[derive(Debug, PartialEq, Deserialize)]
pub struct MappingConfig {
    pub name: String,
    pub value: MappingConfigValue,
}
#[derive(Debug, PartialEq, Deserialize)]
pub struct MappingConfigValue {
    pub path: String,
    #[serde(rename = "contentType")]
    pub content_type: String,
}
