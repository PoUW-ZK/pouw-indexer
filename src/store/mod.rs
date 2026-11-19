//! The only place that talks SQL.
//!
//! The same style everywhere:
//!  * reads are methods of [`Store`]; "maybe absent" is `Ok(None)`, never an error;
//!  * writes are free functions that take the caller's transaction ([`Tx`]), so the caller
//!    decides what is committed together (a page of blocks and its checkpoint, for example);
//!  * the store knows the shape of the tables, not the rules. What is valid is decided by the
//!    callers, apart from checks that the data a node sent adds up.

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use crate::error::StoreError;

mod accounts;
mod blocks;
mod events;

pub use accounts::{delete_account, finish_refresh, upsert_account};
pub use blocks::{insert_block, orphan_above, save_checkpoint};
pub use events::{Parsed, apply_event, insert_events, record_event_gap, set_event_cursor};

pub type Tx<'a> = sqlx::Transaction<'a, sqlx::Postgres>;
pub type Hash = [u8; 32];

/// "I have written the node's chain up to this block."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checkpoint {
    pub height: u64,
    pub hash: Hash,
}

#[derive(Clone)]
pub struct Store {
    pool: PgPool,
}

impl Store {
    /// Connect and bring the schema up to date.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new().max_connections(5).connect(url).await?;
        sqlx::migrate!().run(&pool).await?;
        Ok(Self::new(pool))
    }

    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn begin(&self) -> Result<Tx<'static>, StoreError> {
        Ok(self.pool.begin().await?)
    }

    /// Can the database answer. Called by the Docker healthcheck every 10 seconds.
    pub async fn health(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// The pool, for tests that look at tables directly.
    #[cfg(test)]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

pub(crate) fn num(v: u64) -> Result<i64, StoreError> {
    i64::try_from(v).map_err(|_| StoreError::NotBigint { value: v })
}

pub(crate) fn unsigned(v: i64) -> Result<u64, StoreError> {
    u64::try_from(v).map_err(|_| StoreError::Negative { value: v })
}
