use std::sync::Arc;

use clap::Parser;
use tokio::net::TcpListener;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

use chronicle_lexicon::config::LexiconConfig;
use chronicle_lexicon::grpc::LexiconGrpc;
use chronicle_lexicon::proto;
use chronicle_lexicon::service::LexiconService;
use chronicle_lexicon::store::LocalFileStore;

#[derive(Parser)]
#[command(name = "chronicle-lexicon", about = "Chronicle Schema Registry")]
struct Cli {
    /// 配置文件路径
    #[arg(long, default_value = "")]
    config: String,

    /// gRPC 端口
    #[arg(long)]
    grpc_port: Option<u16>,

    /// HTTP 端口
    #[arg(long)]
    http_port: Option<u16>,

    /// 本地存储目录
    #[arg(long)]
    store_dir: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let mut config = if !cli.config.is_empty() {
        let content = std::fs::read_to_string(&cli.config)?;
        serde_json::from_str::<LexiconConfig>(&content)?
    } else {
        LexiconConfig::default()
    };

    if let Some(port) = cli.grpc_port {
        config.grpc_port = port;
    }
    if let Some(port) = cli.http_port {
        config.http_port = port;
    }
    if let Some(dir) = cli.store_dir {
        config.local_store_dir = dir;
    }

    let store: Arc<dyn chronicle_lexicon::store::SchemaStore> =
        Arc::new(LocalFileStore::new(&config.local_store_dir)?);

    let service = Arc::new(LexiconService::new(store));

    // 启动 gRPC
    let grpc_addr = format!("0.0.0.0:{}", config.grpc_port).parse()?;
    let grpc_service = LexiconGrpc::new(service.clone());

    let grpc_handle = tokio::spawn(async move {
        tracing::info!(%grpc_addr, "gRPC server starting");
        Server::builder()
            .add_service(proto::lexicon_server::LexiconServer::new(grpc_service))
            .serve(grpc_addr)
            .await
            .unwrap();
    });

    // 启动 HTTP
    let http_addr = format!("0.0.0.0:{}", config.http_port);
    let app = chronicle_lexicon::http::router(service);

    let http_handle = tokio::spawn(async move {
        tracing::info!(%http_addr, "HTTP server starting");
        let listener = TcpListener::bind(&http_addr).await.unwrap();
        axum::serve(listener, app).await.unwrap();
    });

    tracing::info!(
        "Lexicon started (gRPC:{}, HTTP:{})",
        config.grpc_port,
        config.http_port
    );

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");

    grpc_handle.abort();
    http_handle.abort();

    Ok(())
}
