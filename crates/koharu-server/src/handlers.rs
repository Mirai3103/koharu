use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    Json,
    body::{Body, BodyDataStream},
    extract::{FromRequest, Multipart, Request, State},
    http::{request::Parts, StatusCode, header},
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
    mut stream: BodyDataStream,
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
