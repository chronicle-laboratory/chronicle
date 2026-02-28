use clap::Parser;
use chronicle_cli::aeon::AeonAction;
use chronicle_cli::logic::LogicAction;
use chronicle_cli::prism::PrismAction;
use chronicle_cli::unit::UnitAction;

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

    /// Manage the schema registry server
    Logic {
        #[command(subcommand)]
        action: LogicAction,
    },

    /// Manage the function server
    Prism {
        #[command(subcommand)]
        action: PrismAction,
    },

    /// Manage the lakehouse column database server
    Aeon {
        #[command(subcommand)]
        action: AeonAction,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Unit { action } => chronicle_cli::unit::run(action).await?,
        Commands::Logic { action } => chronicle_cli::logic::run(action).await?,
        Commands::Prism { action } => chronicle_cli::prism::run(action).await?,
        Commands::Aeon { action } => chronicle_cli::aeon::run(action).await?,
    }

    Ok(())
}
