use chronicle_cli::unit::UnitAction;
use clap::Parser;

#[derive(Parser)]
#[command(name = "chronicle", about = "Chronicle event streaming CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Manage a unit (storage node) server
    Unit {
        #[command(subcommand)]
        action: UnitAction,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Unit { action } => chronicle_cli::unit::run(action).await?,
    }

    Ok(())
}
