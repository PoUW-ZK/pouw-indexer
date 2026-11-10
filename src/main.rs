mod event_source;
mod events;
mod fakechain;
mod metrics;
mod node;
mod store;
mod sync;

use anyhow::Context;
use dotenv::dotenv;
use std::sync::Arc;
use std::time::Duration;

use axum::{Router, extract::State, http::StatusCode, http::header, routing::get};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::event_source::{HttpEventSource, MemoryEventNode};
use crate::events::EventConfig;
use crate::metrics::Metrics;
use crate::node::{HttpNode, MemoryNode};
use crate::sync::SyncConfig;

const DEFAULT_SEED_FILE: &str = "tests/fixtures/blocks.jsonl";

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

/// "a=1,b=2" in an environment variable -> [("a","1"), ("b","2")]. Empty or missing -> [].
fn env_pairs(var: &str) -> Vec<(String, String)> {
    std::env::var(var)
        .unwrap_or_default()
        .split(',')
        .filter_map(|p| p.split_once('='))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect()
}

/// `indexer serve`: run the HTTP server (default command).
async fn serve() -> anyhow::Result<()> {
    let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".into());
    let pool = connect().await?;
    let metrics_registry = Arc::new(Metrics::new());

    // Which node to follow for blocks: NODE_URL is its REST API (http://node-1:8000), or
    // NODE_FIXTURES a file of blocks for running without a node. NODE_ID names it in sync_state.
    let node_id = std::env::var("NODE_ID").unwrap_or_else(|_| "node".into());
    let cfg = SyncConfig {
        node_id,
        poll: Duration::from_secs(2),
    };
    if let Ok(url) = std::env::var("NODE_URL") {
        let node = HttpNode::new(&url)?;
        println!("sync: following the node at {url}");
        tokio::spawn(sync::run(pool.clone(), node, cfg, metrics_registry.clone()));
    } else if let Ok(path) = std::env::var("NODE_FIXTURES") {
        let node = MemoryNode::from_file(&path)?;
        println!("sync: following the blocks in {path}");
        tokio::spawn(sync::run(pool.clone(), node, cfg, metrics_registry.clone()));
    } else {
        println!("sync disabled: set NODE_URL (or NODE_FIXTURES for a file of blocks)");
    }

    // Event streams, one per node: EVENT_SOURCES="node-a=http://node-a:8080,miner-1=http://..."
    // (the name is the node_id the node reports). EVENT_FIXTURES="node-a=path/to/node-a.jsonl"
    // reads a recorded stream instead, for running without nodes.
    let mut event_tasks = 0;
    for (name, url) in env_pairs("EVENT_SOURCES") {
        let src = HttpEventSource::new(&url)?;
        tokio::spawn(events::run(
            pool.clone(),
            src,
            EventConfig::new(&name),
            metrics_registry.clone(),
        ));
        println!("events: reading {name} from {url}");
        event_tasks += 1;
    }
    for (name, path) in env_pairs("EVENT_FIXTURES") {
        let src = MemoryEventNode::from_file(&path)?;
        tokio::spawn(events::run(
            pool.clone(),
            src,
            EventConfig::new(&name),
            metrics_registry.clone(),
        ));
        println!("events: reading {name} from the file {path}");
        event_tasks += 1;
    }
    if event_tasks == 0 {
        println!("events disabled: set EVENT_SOURCES (or EVENT_FIXTURES for a recorded stream)");
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

/// `indexer seed [file]`: load blocks into the database. The file has one block per line
/// in the JSON of the node API (what `gen-fixtures` writes). Without a file the default
/// fixtures file is used.
async fn seed(path: String) -> anyhow::Result<()> {
    let pool = connect().await?;
    let blocks = node::read_block_views(&path)?;
    let mut added = 0;
    for b in &blocks {
        if store::insert_block(&pool, b).await? {
            added += 1;
        }
    }
    // statistics are materialized views since migration 0003
    sqlx::query("SELECT api.refresh_stats()")
        .execute(&pool)
        .await?;
    println!(
        "seed: {} blocks read from {path}, {added} new, {} already there",
        blocks.len(),
        blocks.len() - added
    );
    Ok(())
}

/// `indexer gen-fixtures [count] [file]`: write a fake chain, made with the node's own types.
fn gen_fixtures(count: usize, path: &str) -> anyhow::Result<()> {
    node::write_block_views(path, &fakechain::demo_chain(count))?;
    println!("gen-fixtures: wrote {count} blocks to {path}");
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
        Some("gen-fixtures") => {
            let count = args.next().map_or(Ok(20), |n| n.parse())?;
            gen_fixtures(
                count,
                &args.next().unwrap_or_else(|| DEFAULT_SEED_FILE.into()),
            )
        }
        Some(other) => {
            anyhow::bail!(
                "unknown command {other:?}. Use `serve`, `seed [file]` or `gen-fixtures [count] [file]`"
            )
        }
    }
}
