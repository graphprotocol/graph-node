use std::sync::Arc;

use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::{Extension, State};
use axum::http::HeaderMap;
use axum::response::{Html, IntoResponse, Json, Response};

use crate::auth::unauthorized_graphql_message;
use crate::handlers::state::AppState;
use crate::schema::GraphmanSchema;

pub async fn graphql_playground_handler() -> impl IntoResponse {
    Html(playground_source(GraphQLPlaygroundConfig::new("/")))
}

pub async fn graphql_request_handler(
    State(state): State<Arc<AppState>>,
    Extension(schema): Extension<GraphmanSchema>,
    headers: HeaderMap,
    req: GraphQLRequest,
) -> Response {
    if !state.auth_token.headers_contain_correct_token(&headers) {
        return Json(unauthorized_graphql_message()).into_response();
    }

    let resp: GraphQLResponse = schema.execute(req.into_inner()).await.into();

    resp.into_response()
}
