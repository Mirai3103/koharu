# Koharu Server Design

Date: 2026-08-16
Status: Approved (design reviewed in chat)
Branch: `feat/koharu-server`

## Goal

A standalone HTTP web service that receives a comic image and returns the translated
comic image. All models are loaded into VRAM once and reused across requests. Requests
are processed through a single-worker FIFO queue. The service is local/internal: no
authentication, binds to `127.0.0.1`.

## Non-Goals

- Per-request model or language overrides (config is fixed at startup).
- Multiple concurrent inference workers.
- Authentication, rate limiting beyond queue-depth rejection, or public exposure.
- Batch/multi-page endpoints (one image per request).
- Job polling/webhook APIs (sync blocking responses only).

## Placement

New crate `crates/koharu-server/` inside this workspace, registered in the root
`Cargo.toml` as `koharu-server = { path = "crates/koharu-server" }` under
`[workspace.dependencies]`. The workspace `members = ["crates/*"]` glob already
includes it.

Internal dependencies:

- `koharu-ml` — `init()`, `device(cpu)`
- `koharu-pipeline` — `Pipeline`, `PipelineConfig`, `Request`, `Operation`, `Scope`,
  `StopToken`
- `koharu-scene` — `Session::memory()`, `PageDraft`, `AssetRole`
- `koharu-renderer` — `Renderer`
- `koharu-rasterizer` — `Rasterizer`, `RasterOptions`
- `koharu-config` — `Config`
- `koharu-translator` — `ProvidersConfig`

External dependencies (added to workspace):

- `axum` (with `multipart` feature)
- `tower-http` (trace layer; no CORS needed for localhost service)
- Reuse workspace `tokio`, `anyhow`, `tracing`, `tracing-subscriber`, `serde`,
  `serde_json`, `image`, `clap`, `thiserror`.

## Startup (load once, reuse forever)

The binary `koharu-server` performs this sequence exactly once at boot, mirroring
`crates/koharu-pipeline/src/bin/run.rs`:

1. Parse CLI args (see below) with `clap`.
2. `koharu_ml::init().await` — discovers/activates native runtimes, selects device.
3. `device = koharu_ml::device(args.cpu)`.
4. Load `PipelineConfig` and `ProvidersConfig` — from on-disk config via
   `koharu_config::load` when `--config` is given, otherwise defaults wrapped in
   `Config::memory`.
5. `pipeline = Pipeline::from_config(cfg, providers, device)` — `Arc`-backed, `Clone`.
6. `renderer = Renderer::new()?`, `rasterizer = Rasterizer::new()?` — both `Clone`.
7. Build `Arc<ServerState>` containing pipeline, renderer, rasterizer, a queue
   semaphore, and counters. Hand it to axum as shared `State`.

Models are lazily loaded into VRAM by `ModelCell` on the first request through each
stage, then remain resident for the process lifetime. Hot-reload of config is not
exposed; restart the service to change models.

### CLI flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--port` | `8383` | TCP port to listen on |
| `--bind` | `127.0.0.1` | Bind address |
| `--cpu` | off | Force CPU device |
| `--config` | none | Path to a TOML pipeline config |
| `--max-queued` | `16` | Max requests waiting+in-flight before 503 |

## Endpoints

### `POST /translate` (multipart)

- Fields: `image` (required, file part, any format the `image` crate decodes).
- Response: translated page as `image/png` bytes.
- Errors: missing/undecodable image → `400`; pipeline failure → `500`; queue full →
  `503`.

### `POST /translate-path` (JSON)

- Body: `{ "input_path": "...", "output_path": "..." }`.
- Behavior: read the input file, translate, write the rendered PNG to `output_path`.
- Response: `{ "status": "ok" }` on success.
- Errors: unreadable input / unwritable output → `422`; pipeline failure → `500`;
  queue full → `503`.
- Intended for clients on the same machine; paths are used verbatim, no sandboxing.

### `GET /health`

- Response: `{ "status": "ok", "device": "<backend name>", "vram_used_bytes": N,
  "vram_total_bytes": N }` where VRAM figures come from
  `pipeline.subscribe_resources()` (`ResourceSnapshot`). CPU device reports nulls.

There is no `/stats` endpoint.

## Request Flow

Per request, both endpoints share one translation function modeled on
`bin/run.rs:131–216`:

1. Decode bytes → `DynamicImage` (via `image::load_from_memory`).
2. `Session::memory()`; add a `PageDraft` whose `source` asset holds the raw image
   bytes (`AssetRole::new("source")`).
3. Build `Request { operation: Operation::Full, scope: Scope::Pages(vec![page]),
   stop, ..Default::default() }`.
4. `pipeline.execute(snapshot, request, &mut SessionCommitter(&mut session)).await`
   where `SessionCommitter` calls `session.commit(patch).await?.snapshot`.
5. `renderer.render(&snapshot, page).await?` → `frame.raster_frame()?` →
   `rasterizer.rasterize(frame, RasterOptions::default())?` → `raster.image`.
6. Encode PNG: return bytes (`/translate`) or save to `output_path`
   (`/translate-path`).

## Queue

- One worker: every handler calls the same `Pipeline`, whose internal
  `tokio::Mutex` serializes `execute` — this is the FIFO queue.
- `--max-queued` (default 16) is enforced by an `Arc<tokio::sync::Semaphore>` with
  `try_acquire`: requests beyond the limit are rejected immediately with `503` and a
  JSON body `{"error": "queue full"}`. The permit spans the whole request (waiting +
  inference).
- No multiple pipelines, no worker pool.

## Cancellation

Client disconnect cancels the in-flight job cooperatively:

1. Each request spawns `pipeline.execute(...)` on a background task.
2. The handler `tokio::select!`s between the task's completion and a disconnect
   detector that polls the request body for connection close (`Body::poll_close` /
  `into_data_stream` EOF — exact API pinned during implementation against the
   workspace's axum version).
3. On disconnect: flip the job's `StopToken`. The pipeline checks the token at stage
   and page boundaries (`stage_runner.rs`, `execution.rs:227`), stops early, and the
   handler returns without writing a response. The semaphore permit drops, freeing a
   queue slot.
4. Cancellation is best-effort cleanup, not instant kill: an in-flight model forward
   pass finishes before the next checkpoint. A queued request (waiting on the
   pipeline mutex) that gets cancelled simply never runs its execute body.

Implementation note: the disconnect-detection API is the one axum-version-sensitive
piece. If it proves unreliable during the spike, cancellation is dropped (per prior
agreement) rather than worked around with hacks.

## Error Handling

A single `ApiError` enum implementing `IntoResponse`:

| Variant | HTTP | When |
|---------|------|------|
| `BadRequest` | 400 | missing multipart field, undecodable image, malformed JSON |
| `Unprocessable` | 422 | input path unreadable, output path unwritable |
| `QueueFull` | 503 | semaphore `try_acquire` failed |
| `Internal` | 500 | pipeline/renderer/rasterizer error |

Full error details go to `tracing`; responses carry a short message plus the variant.

## Testing

Focused, debug-profile, per AGENTS.md verification rules:

- Unit: `SessionCommitter` adapter; `ApiError` → status code mapping.
- Integration (`crates/koharu-server/tests/translate.rs`): bind the axum router to an
  ephemeral port (`TcpListener::bind("127.0.0.1:0")`), POST a small fixture image to
  `/translate`, assert a decodable PNG comes back. The test runs only when the env
  var `KOHARU_SERVER_E2E=1` is set (models must be downloadable to the local store);
  otherwise it exits with an "ignored" message so CI without models still passes.
- Manual smoke: `curl -F image=@page.png http://127.0.0.1:8383/translate -o out.png`
  and the path endpoint against a real chapter.

## File Layout

- `crates/koharu-server/Cargo.toml`
- `crates/koharu-server/src/main.rs` — CLI, tracing, startup sequence, listener
- `crates/koharu-server/src/state.rs` — `ServerState`
- `crates/koharu-server/src/error.rs` — `ApiError`
- `crates/koharu-server/src/translate.rs` — shared translation function +
  `SessionCommitter`
- `crates/koharu-server/src/handlers.rs` — the three routes + cancellation wiring
- `crates/koharu-server/tests/translate.rs`
- Root `Cargo.toml` — workspace dependency entry

## Change Policy Notes

Per AGENTS.md: no backward compatibility shims, no forwarding layers. The server
crate depends only on published-in-workspace public APIs; if those APIs change, the
server is updated in the same commit.
