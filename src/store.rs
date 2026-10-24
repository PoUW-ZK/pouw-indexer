//! Write decoded blocks into Postgres.

use anyhow::Context;
use sqlx::PgPool;

use crate::block::{Block, ProofSystem, TxKind, sha256};

fn num(v: u64) -> anyhow::Result<i64> {
    i64::try_from(v).context("number does not fit in a Postgres BIGINT")
}

fn system_name(s: ProofSystem) -> &'static str {
    match s {
        ProofSystem::Groth16 => "groth16",
        ProofSystem::PlonkKzg => "plonk_kzg",
        ProofSystem::Marlin => "marlin",
    }
}

/// Insert one block and everything inside it, in one database transaction.
/// Returns false if the block was already there (nothing is written then),
/// so running the same input twice is safe.
///
/// Blocks must come in height order: proof requests point to circuits that an
/// earlier block registered, and the database enforces that.
pub async fn insert_block(pool: &PgPool, block: &Block) -> anyhow::Result<bool> {
    let h = &block.header;
    let hash = h.hash();
    let size = borsh::to_vec(block)?.len() as i32;
    let mut tx = pool.begin().await?;

    sqlx::query("INSERT INTO miners (address) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(&h.miner[..])
        .execute(&mut *tx)
        .await?;

    let inserted = sqlx::query(
        "INSERT INTO blocks (hash, height, prev_hash, block_time, miner, difficulty, bucket_count, \
         bucket_id, leaf_count, tx_root, size_bytes) \
         VALUES ($1, $2, $3, to_timestamp($4::bigint::double precision), $5, $6, $7, $8, $9, $10, $11) \
         ON CONFLICT (hash) DO NOTHING",
    )
    .bind(&hash[..])
    .bind(num(h.height)?)
    .bind(&h.prev_hash[..])
    .bind(num(h.timestamp)?)
    .bind(&h.miner[..])
    .bind(num(h.difficulty)?)
    .bind(h.bucket_count as i32)
    .bind(h.bucket_id as i32)
    .bind(h.tx_count as i32)
    .bind(&h.tx_root[..])
    .bind(size)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if inserted == 0 {
        return Ok(false); // already indexed
    }

    // transfers and circuit registrations (block.txs)
    for (pos, stx) in block.txs.iter().enumerate() {
        let b = &stx.body;
        let txid = stx.txid();
        match &b.kind {
            TxKind::Transfer { to, amount } => {
                sqlx::query(
                    "INSERT INTO transfers (block_hash, position, txid, sender, recipient, amount, fee, nonce) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                )
                .bind(&hash[..])
                .bind(pos as i32)
                .bind(&txid[..])
                .bind(&b.sender[..])
                .bind(&to[..])
                .bind(num(*amount)?)
                .bind(num(b.fee)?)
                .bind(num(b.nonce)?)
                .execute(&mut *tx)
                .await?;
            }
            TxKind::RegisterCircuit {
                system,
                constraint_count,
                verifying_key,
                circuit,
            } => {
                sqlx::query(
                    "INSERT INTO circuits (circuit_id, proof_system, constraint_count, verifying_key_bytes, \
                     circuit_bytes, registrant, fee, registered_block, registered_height) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT DO NOTHING",
                )
                .bind(&txid[..])
                .bind(system_name(*system))
                .bind(num(*constraint_count)?)
                .bind(verifying_key.len() as i32)
                .bind(circuit.len() as i32)
                .bind(&b.sender[..])
                .bind(num(b.fee)?)
                .bind(&hash[..])
                .bind(num(h.height)?)
                .execute(&mut *tx)
                .await?;
            }
            TxKind::ProofRequest { .. } => {
                anyhow::bail!(
                    "block {}: a proof request is in txs, it belongs in proof_txs",
                    hex::encode(hash)
                );
            }
        }
    }

    // proof requests with their proofs (block.proof_txs)
    for (pos, ptx) in block.proof_txs.iter().enumerate() {
        let b = &ptx.request.body;
        let TxKind::ProofRequest {
            circuit,
            public_inputs,
            witness,
        } = &b.kind
        else {
            anyhow::bail!(
                "block {}: proof_txs entry {} is not a proof request",
                hex::encode(hash),
                pos
            );
        };
        let proof_hash = ptx.hash();
        let request_txid = ptx.request.txid();
        let witness_hash = sha256(witness);
        // eta and mask are derived values (block.md), not filled in yet
        sqlx::query(
            "INSERT INTO proof_txs (block_hash, position, proof_hash, request_txid, circuit_id, requester, \
             fee, nonce, usage_counter, proof_bytes, witness_bytes, witness_sha256, public_input_count) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
        )
        .bind(&hash[..])
        .bind(pos as i32)
        .bind(&proof_hash[..])
        .bind(&request_txid[..])
        .bind(&circuit[..])
        .bind(&b.sender[..])
        .bind(num(b.fee)?)
        .bind(num(b.nonce)?)
        .bind(num(ptx.usage_counter)?)
        .bind(ptx.proof.len() as i32)
        .bind(witness.len() as i32)
        .bind(&witness_hash[..])
        .bind(public_inputs.len() as i32)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::read_blocks;

    const FIXTURES: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/blocks.borsh.jsonl"
    );

    async fn count(pool: &PgPool, table: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[sqlx::test]
    async fn twenty_fixture_blocks_are_in_the_db(pool: PgPool) {
        let blocks = read_blocks(FIXTURES).unwrap();
        assert_eq!(blocks.len(), 20);
        for b in &blocks {
            assert!(insert_block(&pool, b).await.unwrap());
        }
        assert_eq!(count(&pool, "blocks").await, 20);
        assert_eq!(
            count(&pool, "proof_txs").await,
            blocks.iter().map(|b| b.proof_txs.len() as i64).sum::<i64>()
        );
        let transfers = blocks
            .iter()
            .flat_map(|b| &b.txs)
            .filter(|t| matches!(t.body.kind, TxKind::Transfer { .. }))
            .count() as i64;
        assert_eq!(count(&pool, "transfers").await, transfers);
        assert_eq!(count(&pool, "circuits").await, 3);
        // chain links: every block except genesis points to a stored block
        let orphans: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM blocks b WHERE b.height > 0 AND NOT EXISTS \
             (SELECT 1 FROM blocks p WHERE p.hash = b.prev_hash)",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(orphans, 0);
    }

    #[sqlx::test]
    async fn inserting_twice_changes_nothing(pool: PgPool) {
        let blocks = read_blocks(FIXTURES).unwrap();
        for b in &blocks {
            insert_block(&pool, b).await.unwrap();
        }
        let before = (
            count(&pool, "blocks").await,
            count(&pool, "proof_txs").await,
            count(&pool, "transfers").await,
        );
        for b in &blocks {
            assert!(
                !insert_block(&pool, b).await.unwrap(),
                "second insert must report 'already there'"
            );
        }
        let after = (
            count(&pool, "blocks").await,
            count(&pool, "proof_txs").await,
            count(&pool, "transfers").await,
        );
        assert_eq!(before, after);
    }
}
