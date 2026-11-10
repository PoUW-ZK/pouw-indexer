//! Write blocks, as the node API serves them, into Postgres.

use anyhow::{Context, bail, ensure};
use pouw_api::view::{BlockView, SignedTxView};
use pouw_types::crypto::sha256;
use pouw_types::{Block, ProofTx, TxKind, block_id, encoded_len, txid};
use sqlx::{PgConnection, PgPool};

fn num(v: u64) -> anyhow::Result<i64> {
    i64::try_from(v).context("number does not fit in a Postgres BIGINT")
}

/// The block as the node stores it, rebuilt from its view.
fn to_block(v: &BlockView) -> Block {
    Block {
        header: v.header,
        txs: v.txs.iter().map(|t| t.tx.clone()).collect(),
        proof_txs: v
            .proof_txs
            .iter()
            .map(|p| ProofTx {
                request: p.request.tx.clone(),
                usage_counter: p.usage_counter,
                proof: p.proof.clone(),
            })
            .collect(),
    }
}

/// Do not trust the transport: the id, every txid and the Merkle root must match the data.
/// The genesis block is exempt from the root check: its `tx_root` commits to the allocations
/// of the chain spec, which are not transactions.
fn check_consistent(v: &BlockView, block: &Block) -> anyhow::Result<()> {
    let height = v.header.height.get();
    ensure!(
        block_id(&v.header) == v.id,
        "block {height}: the id does not match the header"
    );
    let all: Vec<&SignedTxView> = v
        .txs
        .iter()
        .chain(v.proof_txs.iter().map(|p| &p.request))
        .collect();
    for t in all {
        ensure!(
            txid(&t.tx) == t.txid,
            "block {height}: a txid does not match its transaction"
        );
    }
    if height > 0 {
        ensure!(
            block.compute_tx_root() == v.header.tx_root,
            "block {height}: tx_root does not match the transactions"
        );
    }
    Ok(())
}

/// Insert one block in its own transaction. See `insert_block_tx`.
#[allow(dead_code)]
pub async fn insert_block(pool: &PgPool, block: &BlockView) -> anyhow::Result<bool> {
    let mut tx = pool.begin().await?;
    let inserted = insert_block_tx(&mut tx, block).await?;
    tx.commit().await?;
    Ok(inserted)
}

/// Write a circuit registration. If the circuit is already known but the block that
/// registered it was dropped by a reorganisation, point it at this block instead.
async fn upsert_circuit(
    conn: &mut PgConnection,
    block_hash: &[u8; 32],
    height: u64,
    t: &SignedTxView,
) -> anyhow::Result<()> {
    let TxKind::RegisterCircuit {
        system,
        constraint_count,
        verifying_key,
        circuit,
    } = &t.tx.body.kind
    else {
        return Ok(());
    };
    sqlx::query(
        "INSERT INTO circuits (circuit_id, proof_system, constraint_count, verifying_key_bytes, \
         circuit_bytes, registrant, fee, registered_block, registered_height) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         ON CONFLICT (circuit_id) DO UPDATE SET registered_block = EXCLUDED.registered_block, \
             registered_height = EXCLUDED.registered_height, fee = EXCLUDED.fee \
         WHERE circuits.registered_block <> EXCLUDED.registered_block \
           AND EXISTS (SELECT 1 FROM blocks o WHERE o.hash = circuits.registered_block AND NOT o.canonical)",
    )
    .bind(&t.txid.hash().0[..])
    .bind(system.as_str())
    .bind(num(constraint_count.0)?)
    .bind(verifying_key.len() as i32)
    .bind(circuit.len() as i32)
    .bind(&t.tx.body.sender.0[..])
    .bind(num(t.tx.body.fee.0)?)
    .bind(&block_hash[..])
    .bind(num(height)?)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Insert one block and everything inside it, using the caller's transaction, so many
/// blocks and a checkpoint can be committed together.
/// Returns true if the block is new. A block that is already there writes nothing,
/// except that one a reorganisation had dropped is made canonical again.
///
/// Blocks must come in height order: proof requests point to circuits that an
/// earlier block registered, and the database enforces that.
pub async fn insert_block_tx(conn: &mut PgConnection, view: &BlockView) -> anyhow::Result<bool> {
    let h = &view.header;
    let height = h.height.get();
    let hash = view.id.hash().0;
    let block = to_block(view);
    check_consistent(view, &block)?;
    let size = encoded_len(&block) as i32;

    sqlx::query("INSERT INTO miners (address) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(&h.miner.0[..])
        .execute(&mut *conn)
        .await?;

    let inserted = sqlx::query(
        "INSERT INTO blocks (hash, height, prev_hash, block_time, miner, tx_root, size_bytes, leaf_count, \
         chain_id, system_id, target, pow_nonce) \
         VALUES ($1, $2, $3, to_timestamp($4::bigint::double precision), $5, $6, $7, $8, $9, $10, $11, $12::numeric) \
         ON CONFLICT (hash) DO NOTHING",
    )
    .bind(&hash[..])
    .bind(num(height)?)
    .bind(&h.parent.hash().0[..])
    .bind(num(h.timestamp.get())?)
    .bind(&h.miner.0[..])
    .bind(&h.tx_root.0[..])
    .bind(size)
    .bind(block.leaf_count() as i32)
    .bind(i64::from(h.chain_id.get()))
    .bind(num(h.system_id.get())?)
    .bind(&h.target.0[..])
    .bind(h.nonce.get().to_string()) // a u64 can be above BIGINT, so it goes in as NUMERIC
    .execute(&mut *conn)
    .await?
    .rows_affected();

    if inserted == 0 {
        // Known block. After a reorganisation it may have been orphaned and the node's chain
        // went back to it: make it canonical again and re-point its circuits.
        let revived = sqlx::query("UPDATE blocks SET canonical = TRUE, orphaned_at = NULL WHERE hash = $1 AND NOT canonical")
            .bind(&hash[..])
            .execute(&mut *conn)
            .await?
            .rows_affected();
        if revived > 0 {
            for t in &view.txs {
                upsert_circuit(conn, &hash, height, t).await?;
            }
        }
        return Ok(false);
    }

    // transfers and circuit registrations (block.txs)
    for (pos, t) in view.txs.iter().enumerate() {
        let b = &t.tx.body;
        match &b.kind {
            TxKind::Transfer { to, amount } => {
                sqlx::query(
                    "INSERT INTO transfers (block_hash, position, txid, sender, recipient, amount, fee, nonce) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                )
                .bind(&hash[..])
                .bind(pos as i32)
                .bind(&t.txid.hash().0[..])
                .bind(&b.sender.0[..])
                .bind(&to.0[..])
                .bind(num(amount.0)?)
                .bind(num(b.fee.0)?)
                .bind(num(b.nonce.0)?)
                .execute(&mut *conn)
                .await?;
            }
            TxKind::RegisterCircuit { .. } => upsert_circuit(conn, &hash, height, t).await?,
            _ => bail!("block {height}: entry {pos} of txs is not a transfer or a registration"),
        }
    }

    // proof requests with their proofs (block.proof_txs)
    for (pos, p) in view.proof_txs.iter().enumerate() {
        let b = &p.request.tx.body;
        let TxKind::ProofRequest {
            circuit,
            public_inputs,
            witness,
        } = &b.kind
        else {
            bail!("block {height}: proof_txs entry {pos} is not a proof request");
        };
        sqlx::query(
            "INSERT INTO proof_txs (block_hash, position, proof_hash, request_txid, circuit_id, requester, \
             fee, nonce, usage_counter, proof_bytes, witness_bytes, witness_sha256, public_input_count, eta, mask) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
        )
        .bind(&hash[..])
        .bind(pos as i32)
        .bind(&p.proof_hash.hash().0[..])
        .bind(&p.request_txid.hash().0[..])
        .bind(&circuit.hash().0[..])
        .bind(&b.sender.0[..])
        .bind(num(b.fee.0)?)
        .bind(num(b.nonce.0)?)
        .bind(num(p.usage_counter.0)?)
        .bind(p.proof.len() as i32)
        .bind(witness.len() as i32)
        .bind(&sha256(&[witness]).0[..])
        .bind(public_inputs.len() as i32)
        .bind(&p.eta.0[..])
        .bind(&p.mask.0[..])
        .execute(&mut *conn)
        .await?;
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakechain::demo_chain;

    async fn count(pool: &PgPool, table: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[sqlx::test]
    async fn twenty_blocks_are_in_the_db(pool: PgPool) {
        let blocks = demo_chain(20);
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
            .filter(|t| matches!(t.tx.body.kind, TxKind::Transfer { .. }))
            .count() as i64;
        assert_eq!(count(&pool, "transfers").await, transfers);
        assert_eq!(count(&pool, "circuits").await, 3);
        // derived fields from the node are stored
        assert_eq!(
            count(&pool, "proof_txs WHERE eta IS NULL OR mask IS NULL").await,
            0
        );
        // every block except the genesis points to a stored block
        let orphans: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM blocks b WHERE b.height > 0 AND NOT EXISTS \
             (SELECT 1 FROM blocks p WHERE p.hash = b.prev_hash)",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(orphans, 0);
        // header fields of the node's format
        assert_eq!(
            count(
                &pool,
                "blocks WHERE target IS NULL OR pow_nonce IS NULL OR chain_id IS NULL"
            )
            .await,
            0
        );
    }

    #[sqlx::test]
    async fn inserting_twice_changes_nothing(pool: PgPool) {
        let blocks = demo_chain(20);
        for b in &blocks {
            insert_block(&pool, b).await.unwrap();
        }
        let before = (
            count(&pool, "blocks").await,
            count(&pool, "proof_txs").await,
            count(&pool, "transfers").await,
        );
        for b in &blocks {
            assert!(!insert_block(&pool, b).await.unwrap());
        }
        let after = (
            count(&pool, "blocks").await,
            count(&pool, "proof_txs").await,
            count(&pool, "transfers").await,
        );
        assert_eq!(before, after);
    }

    #[sqlx::test]
    async fn a_block_that_does_not_add_up_is_refused(pool: PgPool) {
        let mut blocks = demo_chain(3);
        // tamper with the data, keep the id: the node's answer is inconsistent
        blocks[2].header.nonce = pouw_types::U64::new(blocks[2].header.nonce.get() ^ 1);
        assert!(insert_block(&pool, &blocks[0]).await.is_ok());
        assert!(insert_block(&pool, &blocks[2]).await.is_err());
        assert_eq!(count(&pool, "blocks").await, 1);
    }

    #[test]
    fn the_json_the_node_serves_round_trips() {
        // what the HTTP client will read is what the node's own types write
        let b = demo_chain(3).pop().unwrap();
        let json = serde_json::to_string(&b).unwrap();
        assert!(json.contains("\"parent\":\"blk:"), "{json}");
        let back: BlockView = serde_json::from_str(&json).unwrap();
        assert_eq!(back, b);
    }
}
