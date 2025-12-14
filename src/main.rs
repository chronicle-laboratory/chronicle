use chronicle::cm::unit_options::UnitOptions;
use chronicle::unit::unit::Unit;
use clap::Parser;
use log::info;
use signal::unix;
use std::str::FromStr;
use tokio::signal;
use tracing::metadata::LevelFilter;

#[derive(Parser, Debug)]
#[command(name = "unit")]
struct Args {
    #[arg(short, long, help = "Path to the configuration file", required = true)]
    cm_path: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let options: UnitOptions = args
        .cm_path
        .try_into()
        .expect("failed to parse configuration file.");

    let log_level = options.log.level.clone();
    let log_filter = LevelFilter::from_str(&log_level).expect("failed to parse log level");

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(log_filter.into())
                .from_env_lossy(),
        )
        .with_target(true)
        .init();

    let unit = Unit::new(options)
        .await
        .expect("failed to start chronicle unit");

    #[cfg(unix)]
    let mut sigterm =
        unix::signal(unix::SignalKind::terminate()).expect("failed to install SIGTERM handler");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }

    info!("receive the signal, terminating unit process.. ")
}
