use crate::auth::AuthToken;

/// The state that is shared between all request handlers.
pub struct AppState {
    pub auth_token: AuthToken,
}
