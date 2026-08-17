//! Standalone HTTP service that translates comic pages through the
//! in-process Koharu pipeline. See
//! `docs/superpowers/specs/2026-08-16-koharu-server-design.md`.

pub mod error;
pub mod handlers;
pub mod state;
pub mod translate;

use std::sync::Arc;

use axum::{
    Router,
    extract::DefaultBodyLimit,
    routing::{get, post},
};
use tower_http::trace::TraceLayer;

use crate::state::ServerState;

/// Hard cap on request bodies; comic page scans fit well under this.
pub const MAX_UPLOAD_BYTES: usize = 64 * 1024 * 1024;

pub fn router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/translate", post(handlers::translate))
        .route("/translate-path", post(handlers::translate_path))
        .route("/health", get(handlers::health))
        // Guards extractors that read bodies (JSON); the translate routes
        // additionally enforce MAX_UPLOAD_BYTES while draining uploads.
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
