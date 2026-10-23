mod block;

use axum::{extract::State, http::StatusCode, routing::get, Router};
use dotenv::dotenv;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

async fn health(State(pool): State<PgPool>) -> StatusCode {
    match sqlx::query("SELECT 1").execute(&pool).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let url = std::env::var("DATABASE_URL")?;
    let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".into());

    let pool = PgPoolOptions::new().max_connections(5).connect(&url).await?;

    sqlx::migrate!().run(&pool).await?;

    let app = Router::new().route("/health", get(health)).with_state(pool);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    println!("listening on {bind}");
    axum::serve(listener, app).await?;
    Ok(())
}