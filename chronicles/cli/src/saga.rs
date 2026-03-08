use crate::banner;
use crate::process;
use chronicle_saga::config::SagaConfig;
use chronicle_saga::saga::Saga;
use tracing::info;
use std::io::IsTerminal;
use tracing_subscriber::EnvFilter;

const DEFAULT_PID_FILE: &str = "chronicle-saga.pid";

#[derive(clap::Subcommand)]
pub enum SagaAction {
    /// Start the saga server
    Start {
        /// Path to TOML configuration file (reads [saga] section)
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
        SagaAction::Start { config: config_path, pid_file } => {
            let config = match &config_path {
                Some(path) => load_saga_config(path)?,
                None => SagaConfig::default(),
            };

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

            let saga = Saga::with_config(config).await?;

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

/// Load [saga] section from a TOML config file.
fn load_saga_config(path: &str) -> Result<SagaConfig, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read config file '{}': {}", path, e))?;
    let table: toml::Value = toml::from_str(&contents)
        .map_err(|e| format!("failed to parse config file '{}': {}", path, e))?;

    match table.get("saga") {
        Some(section) => {
            let config: SagaConfig = section.clone().try_into()
                .map_err(|e| format!("failed to parse [saga] section: {}", e))?;
            Ok(config)
        }
        None => Ok(SagaConfig::default()),
    }
}
