use clap::Parser;
use chronicle_cli::banner;

#[derive(Parser)]
#[command(name = "chronicle", about = "Chronicle event streaming CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Start a unit (storage node) server
    Unit {
        /// Path to TOML configuration file
        #[arg(short, long)]
        config: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Unit { config: _ } => {
            banner::print_banner("Unit");
            todo!()
        }
    }
}
