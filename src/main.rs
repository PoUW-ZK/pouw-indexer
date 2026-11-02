mod block;
mod fixtures;
mod metrics;
mod node;
mod store;
mod sync;

use anyhow::Context;
use dotenv::*;
use std::sync::Arc;
use std::time::Duration;

use axum::{Router, extract::State, http::StatusCode, http::header, routing::get};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::metrics::Metrics;
use crate::node::MemoryNode;
use crate::sync::SyncConfig;

const DEFAULT_SEED_FILE: &str = "tests/fixtures/blocks.borsh.jsonl";

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    metrics: Arc<Metrics>,
}

/// 200 only when the database answers. A node that is down does not fail this check:
/// the indexer keeps serving what it has and shows the lag on /metrics.
async fn health(State(state): State<AppState>) -> StatusCode {
    match sqlx::query("SELECT 1").execute(&state.pool).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// Connect to the database and bring the schema up to date.
async fn connect() -> anyhow::Result<PgPool> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL is not set")?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .context("cannot connect to the database")?;
    sqlx::migrate!()
        .run(&pool)
        .await
        .context("migrations failed")?;
    Ok(pool)
}

async fn metrics(
    State(state): State<AppState>,
) -> ([(header::HeaderName, &'static str); 1], String) {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
}

/// `indexer serve`: run the HTTP server (default command).
async fn serve() -> anyhow::Result<()> {
    let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".into());
    let pool = connect().await?;
    let metrics_registry = Arc::new(Metrics::new());

    // Which node to follow. The HTTP client for the real node is not written yet (the node's
    // block format is still being agreed), so for now only a fixtures file can be followed.
    let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| "node".into());
    let cfg = SyncConfig {
        node_id,
        poll: Duration::from_secs(2),
    };
    if let Ok(path) = std::env::var("NODE_FIXTURES") {
        let node = MemoryNode::from_file(&path)?;
        println!("sync: following the blocks in {path}");
        tokio::spawn(sync::run(pool.clone(), node, cfg, metrics_registry.clone()));
    } else if std::env::var("NODE_URL").is_ok() {
        eprintln!("sync disabled: NODE_URL is set but the HTTP node client is not written yet");
    } else {
        println!("sync disabled: set NODE_FIXTURES (or NODE_URL once the node client exists)");
    }

    let state = AppState {
        pool,
        metrics: metrics_registry,
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    println!("listening on {bind}");
    axum::serve(listener, app).await?;
    Ok(())
}

/// `indexer seed [file]`: load fixture blocks into the database.
async fn seed(path: String) -> anyhow::Result<()> {
    let pool = connect().await?;
    let blocks = fixtures::read_blocks(&path)?;
    let mut added = 0;
    for b in &blocks {
        if store::insert_block(&pool, b).await? {
            added += 1;
        }
    }
    println!(
        "seed: {} blocks read from {path}, {added} new, {} already there",
        blocks.len(),
        blocks.len() - added
    );
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // read variables from a .env file in the current folder, if there is one
    dotenv().ok();
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        None | Some("serve") => serve().await,
        Some("seed") => seed(args.next().unwrap_or_else(|| DEFAULT_SEED_FILE.into())).await,
        Some(other) => anyhow::bail!("unknown command {other:?}. Use `serve` or `seed [file]`"),
    }
}
