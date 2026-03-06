use crate::banner;
use crate::process;
use chronicle_saga::saga::Saga;
use tracing::{info, warn};
use std::io::IsTerminal;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

const DEFAULT_PID_FILE: &str = "chronicle-saga.pid";

#[derive(clap::Subcommand)]
pub enum SagaAction {
    /// Start the saga server
    Start {
        /// Path to TOML configuration file
        #[arg(short, long)]
        config: Option<String>,

        /// Path to PID file
        #[arg(long, default_value = DEFAULT_PID_FILE)]
        pid_file: String,
    },

    /// Stop a running saga server
    Stop {
        /// Path to PID file
        #[arg(long, default_value = DEFAULT_PID_FILE)]
        pid_file: String,
    },
}

pub async fn run(action: SagaAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        SagaAction::Start { config: _config, pid_file } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| EnvFilter::new("info")),
                )
                .with_ansi(std::io::stderr().is_terminal())
                .with_target(false)
                .with_thread_ids(false)
                .with_thread_names(false)
                .compact()
                .init();

            banner::print_banner("Saga");

            process::write_pid_file(&pid_file)?;

            let catalog = loop {
                let task = tokio::spawn(async move {
                    catalog::build_catalog(&catalog::CatalogOptions::default()).await
                });
                tokio::select! {
                    result = task => match result {
                        Ok(Ok(c)) => break c,
                        Ok(Err(e)) => {
                            warn!(error = %e, "catalog connection failed, retrying in 5s");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                        Err(e) => {
                            warn!(error = %e, "catalog task panicked, retrying in 5s");
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    },
                    _ = tokio::time::sleep(Duration::from_secs(15)) => {
                        warn!("catalog connection timed out after 15s, retrying");
                    }
                }
            };

            let saga = Saga::new(catalog).await?;

            process::wait_for_shutdown().await;

            info!("received shutdown signal");
            saga.stop().await;

            process::remove_pid_file(&pid_file);
        }

        SagaAction::Stop { pid_file } => {
            let pid = process::read_pid_file(&pid_file)?;
            process::send_sigterm(pid)?;
            println!("sent stop signal to saga (pid {})", pid);
        }
    }

    Ok(())
}
