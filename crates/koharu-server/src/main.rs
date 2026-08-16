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