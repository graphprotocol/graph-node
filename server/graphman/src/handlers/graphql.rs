use std::sync::Arc;

use async_graphql::http::playground_source;
use async_graphql::http::GraphQLPlaygroundConfig;
use async_graphql_axum::GraphQLRequest;
use async_graphql_axum::GraphQLResponse;
use axum::extract::Extension;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Html;
use axum::response::IntoResponse;
use axum::response::Json;
use axum::response::Response;

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
