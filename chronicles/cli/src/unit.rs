use crate::banner;
use crate::process;
use chronicled::option::unit_options::UnitOptions;
use chronicled::unit::unit::Unit;
use tracing::info;
use tracing_subscriber::EnvFilter;

const DEFAULT_PID_FILE: &str = "chronicle-unit.pid";

#[derive(clap::Subcommand)]
pub enum UnitAction {
    /// Start the unit server
    Start {
        /// Path to TOML configuration file
        #[arg(short, long)]
        config: Option<String>,

        /// Path to PID file
        #[arg(long, default_value = DEFAULT_PID_FILE)]
        pid_file: String,
    },

    /// Stop a running unit server
    Stop {
        /// Path to PID file
        #[arg(long, default_value = DEFAULT_PID_FILE)]
        pid_file: String,
    },
}

pub async fn run(action: UnitAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        UnitAction::Start { config, pid_file } => {
            let options: UnitOptions = match config {
                Some(path) => {
                    let contents = std::fs::read_to_string(&path)
                        .map_err(|e| format!("failed to read config file '{}': {}", path, e))?;
                    toml::from_str(&contents)
                        .map_err(|e| format!("failed to parse config file '{}': {}", path, e))?
                }
                None => UnitOptions::default(),
            };

            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| EnvFilter::new(&options.log.level)),
                )
                .init();

            banner::print_banner("Unit");

            process::write_pid_file(&pid_file)?;

            let unit = Unit::new(options).await?;

            process::wait_for_shutdown().await;

            info!("shutdown signal received");
            unit.stop().await;

            process::remove_pid_file(&pid_file);
        }

        UnitAction::Stop { pid_file } => {
            let pid = process::read_pid_file(&pid_file)?;
            process::send_sigterm(pid)?;
            println!("sent stop signal to unit (pid {})", pid);
        }
    }

    Ok(())
}
