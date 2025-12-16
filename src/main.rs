use std::net::SocketAddr;

use app::{AppState, router};
use clap::Parser;
use error::AppError;

mod app;
mod databend;
mod error;
mod logql;
mod sql;

#[derive(Debug, Parser)]
#[command(author, version, about, disable_help_subcommand = true)]
struct Cli {
    #[arg(long = "dsn", env = "DATABEND_DSN")]
    dsn: String,
    #[arg(long, env = "LOGS_TABLE", default_value = "logs")]
    table: String,
    #[arg(long = "bind", env = "BIND_ADDR", default_value = "0.0.0.0:8080")]
    bind: SocketAddr,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(cli).await {
        eprintln!("server failed: {err}");
    }
}

async fn run(cli: Cli) -> Result<(), AppError> {
    let state = AppState::new(cli.dsn, cli.table);
    let app = router(state);

    let listener = tokio::net::TcpListener::bind(cli.bind)
        .await
        .map_err(|err| AppError::Internal(format!("failed to bind listener: {err}")))?;
    println!("databend-loki-adapter listening on {}", cli.bind);
    axum::serve(listener, app)
        .await
        .map_err(|err| AppError::Internal(format!("server error: {err}")))?;
    Ok(())
}
