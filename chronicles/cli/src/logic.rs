use crate::banner;
use crate::process;
use tracing::info;
use tracing_subscriber::EnvFilter;

const DEFAULT_PID_FILE: &str = "chronicle-logic.pid";

#[derive(clap::Subcommand)]
pub enum LogicAction {
    /// Start the schema registry server
    Start {
        /// Path to TOML configuration file
        #[arg(short, long)]
        config: Option<String>,

        /// Path to PID file
        #[arg(long, default_value = DEFAULT_PID_FILE)]
        pid_file: String,
    },

    /// Stop a running schema registry server
    Stop {
        /// Path to PID file
        #[arg(long, default_value = DEFAULT_PID_FILE)]
        pid_file: String,
    },
}

pub async fn run(action: LogicAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        LogicAction::Start { config: _, pid_file } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
                )
                .init();

            banner::print_banner("Logic (Schema Registry)");

            process::write_pid_file(&pid_file)?;

            // TODO: init and start Logic service
            info!("logic service not yet implemented");

            process::wait_for_shutdown().await;

            info!("shutdown signal received");

            process::remove_pid_file(&pid_file);
        }

        LogicAction::Stop { pid_file } => {
            let pid = process::read_pid_file(&pid_file)?;
            process::send_sigterm(pid)?;
            println!("sent stop signal to logic (pid {})", pid);
        }
    }

    Ok(())
}
