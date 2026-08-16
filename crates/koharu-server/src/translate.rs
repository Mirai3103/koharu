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