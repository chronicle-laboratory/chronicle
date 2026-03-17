use chronicle_cli::unit::UnitAction;
use chronicle_cli::verify::VerifyArgs;
use clap::Parser;

#[derive(Parser)]
#[command(name = "chronicle", about = "Chronicle event streaming CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    Unit {
        #[command(subcommand)]
        action: UnitAction,
    },
    Verify(VerifyArgs),
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Unit { action } => chronicle_cli::unit::run(action).await?,
        Commands::Verify(args) => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                )
                .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
                .with_target(false)
                .with_thread_names(false)
                .compact()
                .init();
            chronicle_cli::verify::run(args).await?;
        }
    }

    Ok(())
}
