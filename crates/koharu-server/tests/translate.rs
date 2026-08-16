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