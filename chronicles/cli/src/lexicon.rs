use std::io::IsTerminal;
use std::sync::Arc;

use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::banner;
use crate::process;
use chronicle_lexicon::config::LexiconConfig;
use chronicle_lexicon::grpc::LexiconGrpc;
use chronicle_lexicon::proto;
use chronicle_lexicon::service::LexiconService;
use chronicle_lexicon::store::LocalFileStore;

const DEFAULT_PID_FILE: &str = "chronicle-lexicon.pid";

#[derive(clap::Subcommand)]
pub enum LexiconAction {
    /// Start the lexicon (schema registry) server
    Start {
        /// Path to TOML configuration file (reads [lexicon] section)
        #[arg(short, long)]
        config: Option<String>,

        /// gRPC port (overrides config file)
        #[arg(long)]
        grpc_port: Option<u16>,

        /// HTTP port (overrides config file)
        #[arg(long)]
        http_port: Option<u16>,

        /// Local store directory (overrides config file)
        #[arg(long)]
        store_dir: Option<String>,

        /// Path to PID file
        #[arg(long, default_value = DEFAULT_PID_FILE)]
        pid_file: String,
    },

    /// Stop a running lexicon server
    Stop {
        /// Path to PID file
        #[arg(long, default_value = DEFAULT_PID_FILE)]
        pid_file: String,
    },
}

pub async fn run(action: LexiconAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        LexiconAction::Start {
            config: config_path,
            grpc_port,
            http_port,
            store_dir,
            pid_file,
        } => {
            // Load config from file or use defaults.
            let mut config = match config_path {
                Some(path) => load_lexicon_config(&path)?,
                None => LexiconConfig::default(),
            };

            // CLI args override config file values.
            if let Some(port) = grpc_port {
                config.grpc_port = port;
            }
            if let Some(port) = http_port {
                config.http_port = port;
            }
            if let Some(dir) = store_dir {
                config.local_store_dir = dir;
            }

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

            banner::print_banner("Lexicon");

            process::write_pid_file(&pid_file)?;

            let store: Arc<dyn chronicle_lexicon::store::SchemaStore> =
                Arc::new(LocalFileStore::new(&config.local_store_dir)?);
            let service = Arc::new(LexiconService::new(store));

            // gRPC
            let grpc_addr = format!("0.0.0.0:{}", config.grpc_port).parse()?;
            let grpc_service = LexiconGrpc::new(service.clone());

            let grpc_handle = tokio::spawn(async move {
                info!(%grpc_addr, "lexicon gRPC server starting");
                Server::builder()
                    .add_service(proto::lexicon_server::LexiconServer::new(grpc_service))
                    .serve(grpc_addr)
                    .await
                    .unwrap();
            });

            // HTTP
            let http_addr = format!("0.0.0.0:{}", config.http_port);
            let app = chronicle_lexicon::http::router(service);

            let http_handle = tokio::spawn({
                let http_addr = http_addr.clone();
                async move {
                    info!(%http_addr, "lexicon HTTP server starting");
                    let listener = TcpListener::bind(&http_addr).await.unwrap();
                    axum::serve(listener, app).await.unwrap();
                }
            });

            info!(
                grpc_port = config.grpc_port,
                http_port = config.http_port,
                store_dir = %config.local_store_dir,
                "Lexicon started"
            );

            process::wait_for_shutdown().await;

            info!("received shutdown signal");
            grpc_handle.abort();
            http_handle.abort();

            process::remove_pid_file(&pid_file);
        }

        LexiconAction::Stop { pid_file } => {
            let pid = process::read_pid_file(&pid_file)?;
            process::send_sigterm(pid)?;
            println!("sent stop signal to lexicon (pid {})", pid);
        }
    }

    Ok(())
}

/// Load [lexicon] section from a TOML config file.
fn load_lexicon_config(path: &str) -> Result<LexiconConfig, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read config file '{}': {}", path, e))?;
    let table: toml::Value = toml::from_str(&contents)
        .map_err(|e| format!("failed to parse config file '{}': {}", path, e))?;

    match table.get("lexicon") {
        Some(section) => {
            let config: LexiconConfig = section.clone().try_into()
                .map_err(|e| format!("failed to parse [lexicon] section: {}", e))?;
            Ok(config)
        }
        None => Ok(LexiconConfig::default()),
    }
}
