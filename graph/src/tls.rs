//! Process-wide TLS crypto provider setup.
//!
//! Our deps pull in `rustls` with both `aws_lc_rs` (via alloy) and `ring`
//! (via object_store/reqwest). With multiple providers linked, `rustls`
//! 0.23 panics on first TLS use unless a default is installed explicitly.
//! Must be called once per process before any TLS connection is built.

/// Install `aws_lc_rs` as the process-wide default `rustls` crypto provider.
/// Idempotent: later calls are no-ops.
pub fn install_default_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}
