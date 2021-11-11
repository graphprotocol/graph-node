// =============================================
// Recursive expansion of the GraphQLQuery macro
// =============================================

pub mod query {
  # ![allow(dead_code)]
  pub const OPERATION_NAME: & 'static str = "Query";
  pub const QUERY: & 'static str = "query Query ($id: String) {\n    gravatars(where: {id: $id}, subgraphError: allow) {\n        id\n        owner\n        displayName\n        imageUrl\n    }\n}";
  const __QUERY_WORKAROUND: &str = include_str!("/Users/vivelev/Desktop/graph-node/runtime/wasm/src/module/query.graphql");
  use serde::{
    Serialize,Deserialize
  };
  #[allow(dead_code)]
  type Boolean = bool;
  #[allow(dead_code)]
  type Float = f64;
  #[allow(dead_code)]
  type Int = i64;
  #[allow(dead_code)]
  type ID = String;
  type Bytes = super::Bytes;
  #[derive(Debug,Deserialize,Serialize)]
  pub struct QueryGravatars {
    pub id:ID,pub owner:Bytes, #[serde(rename = "displayName")]
    pub display_name:String, #[serde(rename = "imageUrl")]
    pub image_url:String,
  }
  #[derive(Serialize)]
  pub struct Variables {
    pub id:Option<String> ,
  }
  impl Variables{}
  
  #[derive(Debug,Deserialize,Serialize)]
  pub struct ResponseData {
    pub gravatars:Vec<QueryGravatars> ,
  }
  
}impl graphql_client::GraphQLQuery for Query {
  type Variables = query::Variables;
  type ResponseData = query::ResponseData;
  fn build_query(variables:Self::Variables) ->  ::graphql_client::QueryBody<Self::Variables>{
    graphql_client::QueryBody {
      variables,query:query::QUERY,operation_name:query::OPERATION_NAME,
    }
  }
  
}

