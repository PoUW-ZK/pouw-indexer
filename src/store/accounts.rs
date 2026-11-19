//! Balance and nonce of addresses, copied from the node. See migration 0007.

use super::{Hash, Store, Tx, num};
use crate::error::StoreError;

/// Addresses to copy again, oldest request first, with the time of the request as text.
/// Hand that text back to [`finish_refresh`]: the request is dropped only if nothing asked
/// for the address again in between.
pub struct Refresh {
    pub address: Hash,
    pub requested_at: String,
}

impl Store {
    pub async fn accounts_to_refresh(&self, limit: u32) -> Result<Vec<Refresh>, StoreError> {
        let rows: Vec<(Vec<u8>, String)> =
            sqlx::query_as("SELECT address, requested_at::text FROM account_refresh ORDER BY requested_at LIMIT $1")
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|(a, requested_at)| {
                let address = a.try_into().map_err(|_| StoreError::BadLength {
                    column: "account_refresh.address",
                })?;
                Ok(Refresh {
                    address,
                    requested_at,
                })
            })
            .collect()
    }

    /// Balance and nonce of an address as last copied.
    #[cfg(test)]
    pub async fn account(&self, address: &Hash) -> Result<Option<(u64, u64)>, StoreError> {
        let row: Option<(i64, i64)> =
            sqlx::query_as("SELECT balance, nonce FROM accounts WHERE address = $1")
                .bind(&address[..])
                .fetch_optional(&self.pool)
                .await?;
        row.map(|(b, n)| Ok((super::unsigned(b)?, super::unsigned(n)?)))
            .transpose()
    }
}

pub async fn upsert_account(
    tx: &mut Tx<'_>,
    address: &Hash,
    balance: u64,
    nonce: u64,
    node_height: u64,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO accounts (address, balance, nonce, node_height) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (address) DO UPDATE SET balance = EXCLUDED.balance, nonce = EXCLUDED.nonce, \
             node_height = EXCLUDED.node_height, copied_at = now()",
    )
    .bind(&address[..])
    .bind(num(balance)?)
    .bind(num(nonce)?)
    .bind(num(node_height)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// The node does not know the address (HTTP 404), so there is nothing to show.
pub async fn delete_account(tx: &mut Tx<'_>, address: &Hash) -> Result<(), StoreError> {
    sqlx::query("DELETE FROM accounts WHERE address = $1")
        .bind(&address[..])
        .execute(&mut **tx)
        .await?;
    Ok(())
}

pub async fn finish_refresh(tx: &mut Tx<'_>, done: &Refresh) -> Result<(), StoreError> {
    sqlx::query(
        "DELETE FROM account_refresh WHERE address = $1 AND requested_at = $2::timestamptz",
    )
    .bind(&done.address[..])
    .bind(&done.requested_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
