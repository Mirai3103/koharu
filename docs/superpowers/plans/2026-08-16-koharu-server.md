# Koharu Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `koharu-server`, a standalone localhost HTTP service that translates one comic page per request through the in-process pipeline, with models loaded once at boot and a single-worker FIFO queue.

**Architecture:** A new `koharu-server` crate (lib + thin binary) in the workspace. axum 0.8 routes (`POST /translate` multipart, `POST /translate-path` JSON, `GET /health`) share one `Pipeline` whose internal mutex serializes inference; a `tokio::sync::Semaphore` caps waiting+in-flight requests at `--max-queued` (default 16) with immediate 503 beyond that. Per request, a `Session::memory()` holds one page, the pipeline executes `Operation::Full`, and the `Renderer`/`Rasterizer` pair produces the final PNG.

**Tech Stack:** Rust (edition 2024), axum 0.8, tower-http 0.6 (trace), tokio, clap 4, thiserror 2, serde/serde_json, image 0.25, and workspace crates koharu-ml/-pipeline/-scene/-renderer/-rasterizer/-config/-translator.

**Spec:** `docs/superpowers/specs/2026-08-16-koharu-server-design.md`

## Global Constraints

- Work happens on branch `feat/koharu-server` (created in Task 1).
- Per AGENTS.md verification rules: debug profile only, focused tests only (`cargo check -p koharu-server`, `cargo test -p koharu-server ...`). Never run the full workspace suite. End-to-end tests only when the user asks — the E2E test here is gated behind `KOHARU_SERVER_E2E=1` so a plain run passes as "ignored".
- No backward-compatibility shims or forwarding layers (AGENTS.md change policy).
- Do not commit fixtures, model weights, or generated outputs; test images are generated in code.
- axum is added as a workspace dependency at version `0.8` with features `["multipart", "json"]`; tower-http at `0.6` with `["trace"]`.
- `koharu-rasterizer`'s workspace entry is `default-features = false`; the server crate must add `features = ["native"]` (same as koharu-pipeline does).
- **`--config` semantics (resolved with the user):** the flag takes a path to a TOML file; the server parses it directly with `toml::from_str::<PipelineConfig>` (serde defaults fill missing fields via the internal `PipelineFile` shape) and wraps it in `Config::memory`. `koharu_config::load` is NOT used — it only reads the fixed `~/.koharu/config.toml`. `ProvidersConfig` is always `ProvidersConfig::default()` wrapped in `Config::memory`.
- **Spec correction:** the spec claims `Rasterizer` is `Clone`; it is not (`pub struct Rasterizer { gpu: Mutex<GpuState> }`). It is shared through `Arc<ServerState>` instead, which is equivalent for the server's purposes. `Renderer` IS `Clone` but is shared the same way for symmetry.
- **Cancellation is spike-gated (per spec):** handlers race the inference task against a body-stream disconnect detector. After a clean upload EOF, hyper reports `None` on the stream, which parks the detector — so mid-inference disconnects may be unobservable. Task 7 contains the manual spike and the exact removal diff if it proves unreliable.

## File Structure

- `Cargo.toml` (root) — add `koharu-server`, `axum`, `tower-http` workspace dependency entries.
- `crates/koharu-server/Cargo.toml` — crate manifest.
- `crates/koharu-server/src/lib.rs` — crate root: module declarations + `router()` + `MAX_UPLOAD_BYTES`.
- `crates/koharu-server/src/main.rs` — CLI args, tracing, startup sequence, listener. (Spec layout; the lib half exists so `tests/` can import the router.)
- `crates/koharu-server/src/error.rs` — `ApiError` enum + `IntoResponse`.
- `crates/koharu-server/src/state.rs` — `ServerState` + `acquire` queue gate.
- `crates/koharu-server/src/translate.rs` — `SessionCommitter`, `translate_image`, `encode_png`.
- `crates/koharu-server/src/handlers.rs` — the three routes + cancellation wiring (`drain_body`, `race_disconnect`).
- `crates/koharu-server/tests/translate.rs` — gated end-to-end test.

---

### Task 1: Crate scaffolding + workspace registration + CLI

**Files:**
- Modify: `Cargo.toml` (root, `[workspace.dependencies]`)
- Create: `crates/koharu-server/Cargo.toml`
- Create: `crates/koharu-server/src/lib.rs`
- Create: `crates/koharu-server/src/main.rs`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: crate `koharu-server` importable as `koharu_server`; binary parses `--port` (8383), `--bind` (127.0.0.1), `--cpu`, `--config <PATH>`, `--max-queued` (16).

- [x] **Step 1: Create and switch to the feature branch**

```bash
git checkout -b feat/koharu-server || git checkout feat/koharu-server
```

- [x] **Step 2: Register workspace dependencies in the root `Cargo.toml`**

Add to `[workspace.dependencies]` after the `koharu-secrets` line:

```toml
koharu-server = { path = "crates/koharu-server" }
```

Add to the external-dependencies block (next to the other HTTP-adjacent crates):

```toml
axum = { version = "0.8", features = ["multipart", "json"] }
tower-http = { version = "0.6", features = ["trace"] }
```

- [x] **Step 3: Write `crates/koharu-server/Cargo.toml`**

```toml
[package]
name = "koharu-server"
version.workspace = true
edition.workspace = true
description = "Standalone HTTP service for Koharu page translation."
license.workspace = true
authors.workspace = true
readme.workspace = true
homepage.workspace = true
repository.workspace = true
keywords.workspace = true
publish.workspace = true

[dependencies]
anyhow = { workspace = true }
async-trait = { workspace = true }
axum = { workspace = true }
clap = { workspace = true }
futures = { workspace = true }
image = { workspace = true }
koharu-config = { workspace = true }
koharu-ml = { workspace = true }
koharu-pipeline = { workspace = true }
koharu-rasterizer = { workspace = true, features = ["native"] }
koharu-renderer = { workspace = true }
koharu-scene = { workspace = true }
koharu-translator = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
tokio = { workspace = true }
toml = { workspace = true }
tower-http = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }

[dev-dependencies]
reqwest = { workspace = true }
tempfile = { workspace = true }
```

- [x] **Step 4: Write `crates/koharu-server/src/lib.rs` (skeleton) and `src/main.rs` (CLI only)**

`src/lib.rs`:

```rust
//! Standalone HTTP service that translates comic pages through the
//! in-process Koharu pipeline. See
//! `docs/superpowers/specs/2026-08-16-koharu-server-design.md`.
```

`src/main.rs`:

```rust
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(version, about = "Standalone HTTP service for Koharu page translation")]
struct Arguments {
    /// TCP port to listen on.
    #[arg(long, default_value_t = 8383)]
    port: u16,

    /// Bind address.
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,

    /// Force the CPU device.
    #[arg(long)]
    cpu: bool,

    /// Path to a TOML pipeline config.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// Max requests waiting + in flight before rejecting with 503.
    #[arg(long, default_value_t = 16)]
    max_queued: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = Arguments::parse();
    tracing::info!(?arguments, "parsed arguments");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_defaults_match_spec() {
        let arguments = Arguments::try_parse_from(["koharu-server"]).unwrap();
        assert_eq!(arguments.port, 8383);
        assert_eq!(arguments.bind, "127.0.0.1");
        assert!(!arguments.cpu);
        assert!(arguments.config.is_none());
        assert_eq!(arguments.max_queued, 16);
    }

    #[test]
    fn cli_overrides_parse() {
        let arguments = Arguments::try_parse_from([
            "koharu-server",
            "--port",
            "9000",
            "--bind",
            "0.0.0.0",
            "--cpu",
            "--config",
            "pipeline.toml",
            "--max-queued",
            "4",
        ])
        .unwrap();
        assert_eq!(arguments.port, 9000);
        assert_eq!(arguments.bind, "0.0.0.0");
        assert!(arguments.cpu);
        assert_eq!(arguments.config, Some(PathBuf::from("pipeline.toml")));
        assert_eq!(arguments.max_queued, 4);
    }
}
```

- [x] **Step 5: Verify the crate compiles and the CLI tests pass**

Run: `cargo test -p koharu-server --bin koharu-server`
Expected: PASS, 2 tests (`cli_defaults_match_spec`, `cli_overrides_parse`). First run downloads/compiles axum and tower-http; that is expected.

- [x] **Step 6: Commit**

```bash
git add Cargo.toml crates/koharu-server
git commit -m "feat(koharu-server): scaffold crate with CLI arguments"
```

---

### Task 2: `ApiError` and HTTP status mapping

**Files:**
- Create: `crates/koharu-server/src/error.rs`
- Modify: `crates/koharu-server/src/lib.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: `ApiError::{BadRequest(String), Unprocessable(String), QueueFull, Internal(anyhow::Error)}`, helper `ApiError::internal(impl Into<anyhow::Error>)`, `IntoResponse` returning `{"error": "<message>"}` JSON with status 400/422/503/500. All later tasks map failures into `ApiError`.

- [x] **Step 1: Register the module**

In `src/lib.rs`, append:

```rust
pub mod error;
```

- [x] **Step 2: Write the failing tests**

Create `src/error.rs` containing only:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn maps_variants_to_status_codes() {
        assert_eq!(
            ApiError::BadRequest("x".into()).status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ApiError::Unprocessable("x".into()).status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(ApiError::QueueFull.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            ApiError::internal(anyhow::anyhow!("boom")).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[tokio::test]
    async fn response_body_carries_short_message() {
        use axum::response::IntoResponse;
        let response = ApiError::QueueFull.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(&body[..], br#"{"error":"queue full"}"#);
    }
}
```

- [x] **Step 3: Run tests to verify they fail**

Run: `cargo test -p koharu-server --lib error`
Expected: FAIL — compile error, `ApiError` not found in scope.

- [x] **Step 4: Implement `ApiError`**

Insert above the test module in `src/error.rs`:

```rust
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

/// Errors surfaced to HTTP clients. Full details go to `tracing`; responses
/// carry only the short message from `Display`.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("unprocessable: {0}")]
    Unprocessable(String),
    #[error("queue full")]
    QueueFull,
    #[error("internal error")]
    Internal(#[source] anyhow::Error),
}

impl ApiError {
    pub fn internal(error: impl Into<anyhow::Error>) -> Self {
        Self::Internal(error.into())
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unprocessable(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::QueueFull => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        if let Self::Internal(source) = &self {
            tracing::error!(error = %format!("{source:#}"), "request failed");
        }
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}
```

- [x] **Step 5: Run tests to verify they pass**

Run: `cargo test -p koharu-server --lib error`
Expected: PASS, 2 tests.

- [x] **Step 6: Commit**

```bash
git add crates/koharu-server/src/error.rs crates/koharu-server/src/lib.rs
git commit -m "feat(koharu-server): add ApiError with HTTP status mapping"
```

---

### Task 3: `ServerState` and the queue gate

**Files:**
- Create: `crates/koharu-server/src/state.rs`
- Modify: `crates/koharu-server/src/lib.rs`

**Interfaces:**
- Consumes: `ApiError` (Task 2); `koharu_pipeline::{Pipeline, ResourceSnapshot}`, `koharu_renderer::Renderer`, `koharu_rasterizer::Rasterizer` (workspace crates).
- Produces: `ServerState { pipeline, renderer, rasterizer, queue, resources, received, completed, rejected }` with `ServerState::new(pipeline, renderer, rasterizer, max_queued: usize) -> Self`, and free fn `acquire(queue: &Semaphore) -> Result<SemaphorePermit<'_>, ApiError>` used by both route handlers in Task 5.

- [x] **Step 1: Register the module**

In `src/lib.rs`, append:

```rust
pub mod state;
```

- [x] **Step 2: Write the failing test**

Create `src/state.rs` containing only:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_gate_rejects_beyond_limit_and_releases_on_drop() {
        let queue = tokio::sync::Semaphore::new(1);
        let first = acquire(&queue).expect("first permit is available");
        assert!(matches!(acquire(&queue), Err(ApiError::QueueFull)));
        drop(first);
        assert!(acquire(&queue).is_ok());
    }
}
```

- [x] **Step 3: Run test to verify it fails**

Run: `cargo test -p koharu-server --lib state`
Expected: FAIL — compile error, `acquire` and `ApiError` not found in scope.

- [x] **Step 4: Implement `ServerState` and `acquire`**

Insert above the test module in `src/state.rs`:

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use koharu_pipeline::{Pipeline, ResourceSnapshot};
use koharu_rasterizer::Rasterizer;
use koharu_renderer::Renderer;
use tokio::sync::{Semaphore, SemaphorePermit, watch};

use crate::error::ApiError;

/// Shared service state. The pipeline's internal mutex is the single-worker
/// FIFO queue; `queue` only bounds how many requests wait before rejection.
/// `resources` is the pipeline's resource watch channel, started once here.
pub struct ServerState {
    pub pipeline: Pipeline,
    pub renderer: Renderer,
    pub rasterizer: Rasterizer,
    pub queue: Semaphore,
    pub resources: watch::Receiver<ResourceSnapshot>,
    pub received: AtomicU64,
    pub completed: AtomicU64,
    pub rejected: AtomicU64,
}

impl ServerState {
    pub fn new(
        pipeline: Pipeline,
        renderer: Renderer,
        rasterizer: Rasterizer,
        max_queued: usize,
    ) -> Self {
        let resources = pipeline.subscribe_resources();
        Self {
            pipeline,
            renderer,
            rasterizer,
            queue: Semaphore::new(max_queued),
            resources,
            received: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        }
    }

    /// Counts an admitted request; called when a handler finishes.
    pub fn finish(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }
}

/// Admits a request or rejects it with 503 once all `max_queued` permits are
/// held. The permit is held for the whole request: queue wait + inference.
pub fn acquire(queue: &Semaphore) -> Result<SemaphorePermit<'_>, ApiError> {
    queue.try_acquire().map_err(|_| ApiError::QueueFull)
}
```

- [x] **Step 5: Run test to verify it passes**

Run: `cargo test -p koharu-server --lib state`
Expected: PASS, 1 test.

- [x] **Step 6: Commit**

```bash
git add crates/koharu-server/src/state.rs crates/koharu-server/src/lib.rs
git commit -m "feat(koharu-server): add ServerState with bounded queue gate"
```

---

### Task 4: Shared translation function and `SessionCommitter`

**Files:**
- Create: `crates/koharu-server/src/translate.rs`
- Modify: `crates/koharu-server/src/lib.rs`

**Interfaces:**
- Consumes: `ApiError` (Task 2), `ServerState` (Task 3).
- Produces (used by Task 5 handlers):
  - `pub struct SessionCommitter<'a>(pub &'a mut Session)` implementing `koharu_pipeline::Committer`.
  - `pub async fn translate_image(state: Arc<ServerState>, source: Arc<[u8]>, stop: StopToken) -> Result<RgbaImage, ApiError>` — full detect→OCR→translate→inpaint→render→rasterize flow for one page.
  - `pub fn encode_png(image: RgbaImage) -> Result<Vec<u8>, ApiError>`.

- [x] **Step 1: Register the module**

In `src/lib.rs`, append:

```rust
pub mod translate;
```

- [x] **Step 2: Write the failing tests**

Create `src/translate.rs` containing only:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use koharu_pipeline::Stage;
    use koharu_scene::{At, PageDraft};

    #[tokio::test]
    async fn committer_applies_stage_patch_to_session() {
        let mut session = Session::memory().await.unwrap();
        let mut page = None;
        let patch = session
            .snapshot()
            .patch(|edit| {
                page = Some(edit.add_page(PageDraft::new("page", 100.0, 200.0), At::End)?);
                Ok(())
            })
            .unwrap();
        let page = page.unwrap();
        let output = StageOutput {
            page,
            stage: Stage::Detection,
            patch,
        };
        let snapshot = SessionCommitter(&mut session).commit(output).await.unwrap();
        assert_eq!(snapshot.pages().count(), 1);
        assert!(snapshot.page(page).is_ok());
    }

    #[test]
    fn encode_png_produces_decodable_png() {
        let image = RgbaImage::from_pixel(4, 2, image::Rgba([10, 20, 30, 255]));
        let bytes = encode_png(image).unwrap();
        let decoded = image::load_from_memory(&bytes).unwrap();
        assert_eq!((decoded.width(), decoded.height()), (4, 2));
    }

    #[test]
    fn media_type_guesses_from_magic_bytes() {
        let mut png = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgba8(2, 2)
            .write_to(&mut png, image::ImageFormat::Png)
            .unwrap();
        assert_eq!(media_type(&png.into_inner()), "image/png");
        // JPEG SOI + JFIF marker.
        assert_eq!(media_type(&[0xFF, 0xD8, 0xFF, 0xE0]), "image/jpeg");
        assert_eq!(media_type(b"not an image"), "image/png");
    }
}
```

- [x] **Step 3: Run tests to verify they fail**

Run: `cargo test -p koharu-server --lib translate`
Expected: FAIL — compile error, `SessionCommitter`, `encode_png`, `media_type`, `Session`, `StageOutput`, `RgbaImage` not found in scope.

- [x] **Step 4: Implement the translation flow**

Insert above the test module in `src/translate.rs`:

```rust
use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use image::RgbaImage;
use koharu_pipeline::{Committer, Operation, Request, Scope, StageOutput, StopToken};
use koharu_rasterizer::RasterOptions;
use koharu_scene::{AssetInput, AssetMetadata, AssetRole, At, PageDraft, Session, Snapshot};

use crate::error::ApiError;
use crate::state::ServerState;

/// Commits pipeline stage patches into the request-scoped session.
pub struct SessionCommitter<'a>(pub &'a mut Session);

#[async_trait::async_trait]
impl Committer for SessionCommitter<'_> {
    async fn commit(&mut self, output: StageOutput) -> Result<Snapshot> {
        Ok(self.0.commit(output.patch).await?.snapshot)
    }
}

/// Runs the full pipeline on one page and returns the rendered pixels.
/// Mirrors `crates/koharu-pipeline/src/bin/run.rs` lines 131-216.
pub async fn translate_image(
    state: Arc<ServerState>,
    source: Arc<[u8]>,
    stop: StopToken,
) -> Result<RgbaImage, ApiError> {
    let decoded = image::load_from_memory(&source)
        .map_err(|error| ApiError::BadRequest(format!("undecodable image: {error}")))?;
    let media_type = media_type(&source);

    let mut session = Session::memory().await.map_err(ApiError::internal)?;
    let mut page = None;
    let patch = session
        .snapshot()
        .patch(|edit| {
            let id = edit.add_page(
                PageDraft::new(
                    "page",
                    f64::from(decoded.width()),
                    f64::from(decoded.height()),
                ),
                At::End,
            )?;
            edit.set_asset(
                id,
                &AssetRole::new("source")?,
                AssetInput::new(
                    Arc::clone(&source),
                    media_type,
                    AssetMetadata {
                        width: Some(decoded.width()),
                        height: Some(decoded.height()),
                        attributes: BTreeMap::new(),
                    },
                ),
            )?;
            page = Some(id);
            Ok(())
        })
        .map_err(ApiError::internal)?;
    session.commit(patch).await.map_err(ApiError::internal)?;
    let page = page.expect("page ID is assigned by the edit");

    let report = state
        .pipeline
        .execute(
            session.snapshot(),
            Request {
                operation: Operation::Full,
                scope: Scope::Pages(vec![page]),
                stop,
                ..Request::default()
            },
            &mut SessionCommitter(&mut session),
        )
        .await
        .map_err(ApiError::internal)?;
    tracing::info!(
        status = ?report.status,
        elapsed = %report.elapsed.as_secs_f64(),
        "pipeline finished"
    );

    let snapshot = session.snapshot();
    let frame = state
        .renderer
        .render(&snapshot, page)
        .await
        .map_err(ApiError::internal)?;
    let raster_frame = frame.raster_frame().map_err(ApiError::internal)?;
    // Rasterization blocks the thread on GPU readback; keep it off the async
    // workers, mirroring `koharu-app`'s `commands/output.rs::rasterize`.
    let raster = tokio::task::spawn_blocking({
        let state = Arc::clone(&state);
        move || state.rasterizer.rasterize(&raster_frame, RasterOptions::default())
    })
    .await
    .map_err(ApiError::internal)?
    .map_err(ApiError::internal)?;
    Ok(raster.image)
}

/// PNG-encodes a rendered page for the `/translate` response body.
pub fn encode_png(image: RgbaImage) -> Result<Vec<u8>, ApiError> {
    let mut encoded = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgba8(image)
        .write_to(&mut encoded, image::ImageFormat::Png)
        .map_err(ApiError::internal)?;
    Ok(encoded.into_inner())
}

/// The pipeline decodes asset bytes via magic bytes, so this only labels the
/// asset metadata; unknown content defaults to PNG.
fn media_type(bytes: &[u8]) -> &'static str {
    match image::guess_format(bytes) {
        Ok(image::ImageFormat::Jpeg) => "image/jpeg",
        Ok(image::ImageFormat::WebP) => "image/webp",
        _ => "image/png",
    }
}
```

- [x] **Step 5: Run tests to verify they pass**

Run: `cargo test -p koharu-server --lib translate`
Expected: PASS, 3 tests.

- [x] **Step 6: Commit**

```bash
git add crates/koharu-server/src/translate.rs crates/koharu-server/src/lib.rs
git commit -m "feat(koharu-server): add shared translation flow"
```

---

### Task 5: Handlers, router, and cancellation wiring

**Files:**
- Create: `crates/koharu-server/src/handlers.rs`
- Modify: `crates/koharu-server/src/lib.rs`

**Interfaces:**
- Consumes: `ApiError` (Task 2), `ServerState` + `acquire` (Task 3), `translate_image` + `encode_png` (Task 4).
- Produces:
  - `pub async fn health(State<Arc<ServerState>>) -> Json<HealthResponse>` — `{"status":"ok","device":...,"vram_used_bytes":...,"vram_total_bytes":...}`, all three device fields `null` on CPU (the pipeline reports an empty `devices` list for CPU).
  - `pub async fn translate(State<Arc<ServerState>>, Request) -> Result<Response, ApiError>` — multipart field `image` → `image/png` bytes.
  - `pub async fn translate_path(State<Arc<ServerState>>, Request) -> Result<Response, ApiError>` — JSON `{input_path, output_path}` → `{"status":"ok"}`.
  - `pub fn router(state: Arc<ServerState>) -> Router` in `lib.rs`, consumed by Task 6 (`main`) and Task 7 (E2E test).
  - `pub const MAX_UPLOAD_BYTES: usize = 64 * 1024 * 1024` in `lib.rs`.

Notes on the cancellation design (spec section "Cancellation", implemented here; spike in Task 7):
- The handler drains the request body itself (`drain_body`), enforcing `MAX_UPLOAD_BYTES` manually; a mid-upload disconnect surfaces as a stream error and aborts the request.
- Inference runs in a `tokio::spawn`ed task racing a disconnect detector over the drained body stream (`race_disconnect`). On disconnect the handler flips the `StopToken`, lets the job unwind at the next pipeline checkpoint in the background, frees the queue permit, and returns `204 No Content` (the client is gone; hyper discards the response).
- Send/Sync safety of everything crossing the spawned task (`Arc<ServerState>` incl. `Rasterizer`, `Session`, renderer `Frame`, rasterizer `Frame`) is already exercised the same way in `koharu-app` (`commands/output.rs` shares `Arc<Rasterizer>` through `spawn_blocking` and holds renderer frames across `.await` in Tauri commands). If `cargo check` still surfaces a `Send` error, stop and report — do not paper over it with unsafe wrappers.

- [x] **Step 1: Write the failing test**

Create `src/handlers.rs` containing only:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_response_serializes_null_device_fields_on_cpu() {
        let value = serde_json::to_value(HealthResponse {
            status: "ok",
            device: None,
            vram_used_bytes: None,
            vram_total_bytes: None,
        })
        .unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "status": "ok",
                "device": null,
                "vram_used_bytes": null,
                "vram_total_bytes": null,
            })
        );
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p koharu-server --lib handlers`
Expected: FAIL — compile error, `HealthResponse` not found (also: `handlers` module not yet registered in `lib.rs`).

- [x] **Step 3: Register the module and add the router to `src/lib.rs`**

Replace the whole of `src/lib.rs` with:

```rust
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
```

- [x] **Step 4: Implement the handlers**

Insert above the test module in `src/handlers.rs`:

```rust
use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    Json,
    body::{Body, BodyDataStream},
    extract::{FromRequest, Multipart, Request, State},
    http::{Parts, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures::StreamExt;
use koharu_pipeline::StopToken;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;

use crate::error::ApiError;
use crate::state::{self, ServerState};
use crate::translate;
use crate::MAX_UPLOAD_BYTES;

#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
    device: Option<String>,
    vram_used_bytes: Option<u64>,
    vram_total_bytes: Option<u64>,
}

pub async fn health(State(state): State<Arc<ServerState>>) -> Json<HealthResponse> {
    let snapshot = state.resources.borrow();
    let device = snapshot.devices.iter().find(|device| device.selected);
    Json(HealthResponse {
        status: "ok",
        device: device.map(|device| device.name.clone()),
        vram_used_bytes: device.and_then(|device| device.memory_used_bytes),
        vram_total_bytes: device.and_then(|device| device.memory_budget_bytes),
    })
}

#[derive(Deserialize)]
pub struct TranslatePathRequest {
    input_path: PathBuf,
    output_path: PathBuf,
}

#[derive(Serialize)]
pub struct StatusResponse {
    status: &'static str,
}

pub async fn translate(
    State(state): State<Arc<ServerState>>,
    request: Request,
) -> Result<Response, ApiError> {
    state.received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _permit = state::acquire(&state.queue).inspect_err(|_| {
        state.rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    })?;
    let (parts, body) = request.into_parts();
    let mut stream = body.into_data_stream();
    let upload = drain_body(&mut stream).await?;
    let image = multipart_image(parts, upload, &state).await?;

    let stop = StopToken::default();
    let job = tokio::spawn({
        let state = Arc::clone(&state);
        let stop = stop.clone();
        async move {
            let rendered = translate::translate_image(state, image, stop).await?;
            // PNG encoding is CPU-bound; keep it off the async workers.
            tokio::task::spawn_blocking(move || translate::encode_png(rendered))
                .await
                .map_err(ApiError::internal)?
        }
    });
    let outcome = race_disconnect(stream, job, stop).await;
    state.finish();
    match outcome {
        JobOutcome::Finished(result) => {
            Ok(([(header::CONTENT_TYPE, "image/png")], result?).into_response())
        }
        JobOutcome::Disconnected => Ok(StatusCode::NO_CONTENT.into_response()),
    }
}

pub async fn translate_path(
    State(state): State<Arc<ServerState>>,
    request: Request,
) -> Result<Response, ApiError> {
    state.received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _permit = state::acquire(&state.queue).inspect_err(|_| {
        state.rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    })?;
    let (parts, body) = request.into_parts();
    let mut stream = body.into_data_stream();
    let upload = drain_body(&mut stream).await?;
    let Json(payload) =
        Json::<TranslatePathRequest>::from_request(Request::from_parts(parts, Body::from(upload)), &state)
            .await
            .map_err(|rejection| ApiError::BadRequest(rejection.body_text()))?;

    let source = tokio::fs::read(&payload.input_path).await.map_err(|error| {
        ApiError::Unprocessable(format!(
            "cannot read {}: {error}",
            payload.input_path.display()
        ))
    })?;

    let output_path = payload.output_path;
    let stop = StopToken::default();
    let job = tokio::spawn({
        let state = Arc::clone(&state);
        let stop = stop.clone();
        async move { translate::translate_image(state, Arc::from(&source[..]), stop).await }
    });
    let outcome = race_disconnect(stream, job, stop).await;
    state.finish();
    match outcome {
        JobOutcome::Finished(result) => {
            let rendered = result?;
            rendered.save(&output_path).map_err(|error| {
                ApiError::Unprocessable(format!(
                    "cannot write {}: {error}",
                    output_path.display()
                ))
            })?;
            Ok(Json(StatusResponse { status: "ok" }).into_response())
        }
        JobOutcome::Disconnected => Ok(StatusCode::NO_CONTENT.into_response()),
    }
}

/// Reads a request body to memory with a hard cap. A client disconnect during
/// the upload surfaces here as a stream error.
async fn drain_body(stream: &mut BodyDataStream) -> Result<Vec<u8>, ApiError> {
    let mut upload = Vec::new();
    while let Some(frame) = stream.next().await {
        let frame =
            frame.map_err(|error| ApiError::BadRequest(format!("upload failed: {error}")))?;
        if upload.len() + frame.len() > MAX_UPLOAD_BYTES {
            return Err(ApiError::BadRequest("upload too large".into()));
        }
        upload.extend_from_slice(&frame);
    }
    Ok(upload)
}

/// Extracts the `image` file part from a buffered multipart request.
async fn multipart_image(
    parts: Parts,
    upload: Vec<u8>,
    state: &Arc<ServerState>,
) -> Result<Arc<[u8]>, ApiError> {
    let request = Request::from_parts(parts, Body::from(upload));
    let mut multipart = Multipart::from_request(request, state)
        .await
        .map_err(|rejection| ApiError::BadRequest(rejection.body_text()))?;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|rejection| ApiError::BadRequest(rejection.body_text()))?
    {
        if field.name() == Some("image") {
            let bytes = field
                .bytes()
                .await
                .map_err(|rejection| ApiError::BadRequest(rejection.body_text()))?;
            return Ok(Arc::from(&bytes[..]));
        }
    }
    Err(ApiError::BadRequest(
        "missing multipart field `image`".into(),
    ))
}

pub enum JobOutcome<T> {
    Finished(Result<T, ApiError>),
    Disconnected,
}

/// Races a spawned translation job against a disconnect detector. The
/// detector watches the drained body stream: hyper reports `Some(Err(_))`
/// when the connection breaks and `None` after a clean EOF, which parks the
/// detector forever. Whether a post-upload disconnect is observable at all is
/// what the Task 7 spike determines; on disconnect the job keeps running in
/// the background until the pipeline's next stop-token checkpoint so the
/// pipeline mutex unwinds cleanly.
async fn race_disconnect<T: Send + 'static>(
    stream: BodyDataStream,
    job: JoinHandle<Result<T, ApiError>>,
    stop: StopToken,
) -> JobOutcome<T> {
    let mut job = job;
    let disconnected = async move {
        match stream.next().await {
            Some(Err(_)) => {}
            _ => std::future::pending().await,
        }
    };
    tokio::select! {
        joined = &mut job => {
            match joined {
                Ok(result) => JobOutcome::Finished(result),
                Err(error) => JobOutcome::Finished(Err(ApiError::internal(error))),
            }
        }
        _ = disconnected => {
            stop.stop();
            tokio::spawn(async move { let _ = job.await; });
            JobOutcome::Disconnected
        }
    }
}
```

- [x] **Step 5: Run the test and check the crate**

Run: `cargo test -p koharu-server --lib`
Expected: PASS — all 7 lib tests (2 error + 1 state + 3 translate + 1 handlers; the 2 bin tests are a separate target).

- [x] **Step 6: Commit**

```bash
git add crates/koharu-server/src/handlers.rs crates/koharu-server/src/lib.rs
git commit -m "feat(koharu-server): add translate, translate-path, and health routes"
```

---

### Task 6: Startup sequence in `main.rs`

**Files:**
- Modify: `crates/koharu-server/src/main.rs`

**Interfaces:**
- Consumes: `router` (Task 5), `ServerState::new` (Task 3).
- Produces: the runnable binary performing the spec's boot sequence: parse CLI → `koharu_ml::init()` → `device(args.cpu)` → configs → `Pipeline::from_config` → `Renderer`/`Rasterizer` → `ServerState` → serve.

- [x] **Step 1: Write the failing config tests**

In `src/main.rs`, keep the two existing CLI tests and add these tests to the same `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn config_file_parses_with_defaults_for_missing_fields() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pipeline.toml");
        std::fs::write(
            &path,
            "[translation]\ninstructions = \"Keep honorifics.\"\n",
        )
        .unwrap();
        let config = pipeline_config(Some(&path)).unwrap();
        assert_eq!(
            config.translation.instructions.as_deref(),
            Some("Keep honorifics.")
        );
        assert!(matches!(
            config.detection,
            koharu_pipeline::DetectionModel::KoharuLayoutRFDetrSeg2XL(_)
        ));
    }

    #[test]
    fn missing_config_flag_uses_defaults() {
        let config = pipeline_config(None).unwrap();
        assert_eq!(config, koharu_pipeline::PipelineConfig::default());
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p koharu-server --bin koharu-server`
Expected: FAIL — compile error, `pipeline_config` not found in scope.

- [x] **Step 3: Implement the startup sequence**

Replace the `use` block and `main` in `src/main.rs` (keep the `Arguments` struct and tests):

```rust
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, Result};
use clap::Parser;
use koharu_config::Config;
use koharu_pipeline::{Pipeline, PipelineConfig};
use koharu_rasterizer::Rasterizer;
use koharu_renderer::Renderer;
use koharu_server::{router, state::ServerState};
use koharu_translator::ProvidersConfig;
```

```rust
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("koharu=info,tower_http=info")),
        )
        .init();
    let arguments = Arguments::parse();

    koharu_ml::init()
        .await
        .context("failed to initialize native runtimes")?;
    let device = koharu_ml::device(arguments.cpu);
    let pipeline = Pipeline::from_config(
        Config::memory(pipeline_config(arguments.config.as_deref())?),
        Config::memory(ProvidersConfig::default()),
        device,
    )?;
    let state = Arc::new(ServerState::new(
        pipeline,
        Renderer::new()?,
        Rasterizer::new()?,
        arguments.max_queued,
    ));

    let listener = tokio::net::TcpListener::bind((arguments.bind.as_str(), arguments.port))
        .await
        .with_context(|| format!("failed to bind {}:{}", arguments.bind, arguments.port))?;
    tracing::info!(address = %listener.local_addr()?, "koharu-server listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

/// `--config` points at a TOML file holding a pipeline configuration; missing
/// fields fall back to defaults via the `PipelineFile` serde shape. Without
/// the flag the service runs on pure defaults. Config hot-reload is not
/// exposed; restart the service to change models.
fn pipeline_config(path: Option<&Path>) -> Result<PipelineConfig> {
    match path {
        Some(path) => {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            toml::from_str(&text)
                .with_context(|| format!("failed to parse {}", path.display()))
        }
        None => Ok(PipelineConfig::default()),
    }
}
```

(The test module also needs `use std::path::PathBuf;` — already imported at file top; the earlier CLI test uses `PathBuf::from`, which keeps working.)

- [x] **Step 4: Run tests and check**

Run: `cargo test -p koharu-server`
Expected: PASS — 7 lib tests + 4 bin tests.

- [x] **Step 5: Commit**

```bash
git add crates/koharu-server/src/main.rs
git commit -m "feat(koharu-server): add startup sequence and config loading"
```

---

### Task 7: End-to-end test, manual smoke, and cancellation spike

**Files:**
- Create: `crates/koharu-server/tests/translate.rs`

**Interfaces:**
- Consumes: `router` (Task 5), `ServerState::new` (Task 3), full startup pieces (Task 6).
- Produces: gated E2E coverage of `POST /translate` and `GET /health`; manual verification record.

- [x] **Step 1: Write the gated integration test**

Create `tests/translate.rs`:

```rust
//! End-to-end test for the translate service. Runs only with
//! `KOHARU_SERVER_E2E=1` because it downloads models into the local store and
//! needs a working renderer; otherwise it exits as "ignored" so CI without
//! models still passes.

use std::sync::Arc;

use image::RgbaImage;
use koharu_config::Config;
use koharu_pipeline::{Pipeline, PipelineConfig};
use koharu_rasterizer::Rasterizer;
use koharu_renderer::Renderer;
use koharu_server::{router, state::ServerState};
use koharu_translator::ProvidersConfig;

#[tokio::test]
async fn translate_returns_decodable_png() {
    if std::env::var("KOHARU_SERVER_E2E").as_deref() != Ok("1") {
        eprintln!("ignored: set KOHARU_SERVER_E2E=1 to run the end-to-end translate test");
        return;
    }

    koharu_ml::init().await.expect("runtime init");
    let pipeline = Pipeline::from_config(
        Config::memory(PipelineConfig::default()),
        Config::memory(ProvidersConfig::default()),
        koharu_ml::device(false),
    )
    .expect("pipeline");
    let state = Arc::new(ServerState::new(
        pipeline,
        Renderer::new().expect("renderer"),
        Rasterizer::new().expect("rasterizer"),
        16,
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });

    // A blank 64x64 page: detection finds no regions, later stages no-op, and
    // the renderer returns the page unchanged.
    let image = RgbaImage::from_pixel(64, 64, image::Rgba([255, 255, 255, 255]));
    let mut png = std::io::Cursor::new(Vec::new());
    image::DynamicImage::ImageRgba8(image)
        .write_to(&mut png, image::ImageFormat::Png)
        .unwrap();

    let client = reqwest::Client::builder()
        // First run downloads several GB of models.
        .timeout(std::time::Duration::from_secs(1800))
        .build()
        .unwrap();

    let part = reqwest::multipart::Part::bytes(png.into_inner())
        .file_name("page.png")
        .mime_str("image/png")
        .unwrap();
    let form = reqwest::multipart::Form::new().part("image", part);
    let response = client
        .post(format!("http://{address}/translate"))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        response.headers().get(reqwest::header::CONTENT_TYPE).unwrap(),
        "image/png"
    );
    let body = response.bytes().await.unwrap();
    let decoded = image::load_from_memory(&body).expect("response is a decodable image");
    assert_eq!((decoded.width(), decoded.height()), (64, 64));

    let health = client
        .get(format!("http://{address}/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), reqwest::StatusCode::OK);
    let health: serde_json::Value = health.json().await.unwrap();
    assert_eq!(health["status"], "ok");
}
```

- [x] **Step 2: Verify the gate passes without models**

Run: `cargo test -p koharu-server --test translate`
Expected: PASS — the test prints the "ignored" message and returns immediately (no model downloads).

- [x] **Step 3: Commit**

```bash
git add crates/koharu-server/tests/translate.rs
git commit -m "feat(koharu-server): add gated end-to-end translate test"
```

- [x] **Step 4 (MANUAL — requires models; run with the user): End-to-end and smoke verification**

Only when the user asks for an end-to-end run (AGENTS.md):

```bash
KOHARU_SERVER_E2E=1 cargo test -p koharu-server --test translate -- --nocapture
```

Then the real-service smoke:

```bash
cargo run -p koharu-server &
curl -F image=@page.png http://127.0.0.1:8383/translate -o out.png
curl -X POST http://127.0.0.1:8383/translate-path \
  -H 'content-type: application/json' \
  -d '{"input_path": "/abs/page.png", "output_path": "/abs/out.png"}'
curl http://127.0.0.1:8383/health
```

Expected: `out.png` is the translated page, `/translate-path` writes the file and returns `{"status":"ok"}`, `/health` reports the selected device.

- [x] **Step 5 (MANUAL — spike): mid-inference disconnect cancellation**

With the server running against a real page:

```bash
curl -F image=@bigpage.png http://127.0.0.1:8383/translate -o /dev/null &
CURL_PID=$!
sleep 5   # wait until upload is done and inference is running
kill $CURL_PID
```

Watch the server log. The cancellation works if the log shows the pipeline
stopping early (`pipeline finished` with `status = Stopped`) instead of
running to completion.

**If the spike shows the disconnect is NOT observed** (expected, per the
Global Constraints note: hyper reports clean EOF on a drained body and the
detector parks), apply the spec's pre-agreed fallback — drop cancellation:

1. In `src/handlers.rs`, delete `race_disconnect`, `drain_body`, `JobOutcome`, and the now-unused imports (`futures::StreamExt`, `BodyDataStream`, `Parts`, `Body`, `StatusCode`, `JoinHandle`); keep `FromRequest` and `StopToken` (still used below). Then replace both handlers and `multipart_image` with:

```rust
pub async fn translate(
    State(state): State<Arc<ServerState>>,
    request: Request,
) -> Result<Response, ApiError> {
    state.received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _permit = state::acquire(&state.queue).inspect_err(|_| {
        state.rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    })?;
    let image = multipart_image(request, &state).await?;
    let rendered =
        translate::translate_image(Arc::clone(&state), image, StopToken::default()).await?;
    // PNG encoding is CPU-bound; keep it off the async workers.
    let png = tokio::task::spawn_blocking(move || translate::encode_png(rendered))
        .await
        .map_err(ApiError::internal)??;
    state.finish();
    Ok(([(header::CONTENT_TYPE, "image/png")], png).into_response())
}

pub async fn translate_path(
    State(state): State<Arc<ServerState>>,
    request: Request,
) -> Result<Response, ApiError> {
    state.received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _permit = state::acquire(&state.queue).inspect_err(|_| {
        state.rejected.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    })?;
    let Json(payload) = Json::<TranslatePathRequest>::from_request(request, &state)
        .await
        .map_err(|rejection| ApiError::BadRequest(rejection.body_text()))?;
    let source = tokio::fs::read(&payload.input_path).await.map_err(|error| {
        ApiError::Unprocessable(format!(
            "cannot read {}: {error}",
            payload.input_path.display()
        ))
    })?;
    let rendered = translate::translate_image(
        Arc::clone(&state),
        Arc::from(&source[..]),
        StopToken::default(),
    )
    .await?;
    rendered.save(&payload.output_path).map_err(|error| {
        ApiError::Unprocessable(format!(
            "cannot write {}: {error}",
            payload.output_path.display()
        ))
    })?;
    state.finish();
    Ok(Json(StatusResponse { status: "ok" }).into_response())
}

/// Extracts the `image` file part from a multipart request.
async fn multipart_image(
    request: Request,
    state: &Arc<ServerState>,
) -> Result<Arc<[u8]>, ApiError> {
    let mut multipart = Multipart::from_request(request, state)
        .await
        .map_err(|rejection| ApiError::BadRequest(rejection.body_text()))?;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|rejection| ApiError::BadRequest(rejection.body_text()))?
    {
        if field.name() == Some("image") {
            let bytes = field
                .bytes()
                .await
                .map_err(|rejection| ApiError::BadRequest(rejection.body_text()))?;
            return Ok(Arc::from(&bytes[..]));
        }
    }
    Err(ApiError::BadRequest(
        "missing multipart field `image`".into(),
    ))
}
```

2. Keep `translate_image`'s `stop: StopToken` parameter unchanged — the handlers pass `StopToken::default()`, and the parameter is part of the pipeline's request surface rather than cancellation-specific plumbing.

3. Run: `cargo test -p koharu-server` — Expected: PASS, all tests.

4. Commit:

```bash
git add crates/koharu-server/src/handlers.rs
git commit -m "feat(koharu-server): drop disconnect cancellation after unreliable spike"
```

- [x] **Step 6: Record the spike outcome**

Whether cancellation stays or is dropped, note the outcome (device, date, observed behavior) in the merge request description. Do not edit the spec; it already anticipates both outcomes.
