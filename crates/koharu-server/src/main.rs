use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context as _;
use anyhow::Result;
use clap::Parser;
use koharu_config::Config;
use koharu_pipeline::{Pipeline, PipelineConfig};
use koharu_rasterizer::Rasterizer;
use koharu_renderer::Renderer;
use koharu_server::{router, state::ServerState};
use koharu_translator::ProvidersConfig;

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
}
