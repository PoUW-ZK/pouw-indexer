mod block;
mod fixtures;
mod store;

use anyhow::Context;
use axum::{Router, extract::State, http::StatusCode, routing::get};
use dotenv::dotenv;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const DEFAULT_SEED_FILE: &str = "tests/fixtures/blocks.borsh.jsonl";

async fn health(State(pool): State<PgPool>) -> StatusCode {
    match sqlx::query("SELECT 1").execute(&pool).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// Connect to the database and bring the schema up to date.
async fn connect() -> anyhow::Result<PgPool> {
    dotenv().ok();
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

/// `indexer serve`: run the HTTP server (default command).
async fn serve() -> anyhow::Result<()> {
    let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".into());
    let pool = connect().await?;
    let app = Router::new().route("/health", get(health)).with_state(pool);
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
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        None | Some("serve") => serve().await,
        Some("seed") => seed(args.next().unwrap_or_else(|| DEFAULT_SEED_FILE.into())).await,
        Some(other) => anyhow::bail!("unknown command {other:?}. Use `serve` or `seed [file]`"),
    }
}
