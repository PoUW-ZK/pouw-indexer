//! Blocks and everything inside them, and the checkpoint of a followed node.

use pouw_api::event::NodeId;
use pouw_api::view::{BlockView, SignedTxView};
use pouw_types::crypto::sha256;
use pouw_types::{Block, ProofTx, TxKind, block_id, encoded_len, txid};
use sqlx::Row;

use super::{Checkpoint, Hash, Store, Tx, num, unsigned};
use crate::error::{BlockError, BlockField, StoreError};

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
fn check_consistent(v: &BlockView, block: &Block) -> Result<(), StoreError> {
    let height = v.header.height.get();
    if block_id(&v.header) != v.id {
        return Err(BlockError::IdMismatch { height }.into());
    }
    for t in v.txs.iter().chain(v.proof_txs.iter().map(|p| &p.request)) {
        if txid(&t.tx) != t.txid {
            return Err(BlockError::TxidMismatch { height }.into());
        }
    }
    if height > 0 && block.compute_tx_root() != v.header.tx_root {
        return Err(BlockError::TxRootMismatch { height }.into());
    }
    Ok(())
}

/// Write a circuit registration. If the circuit is already known but the block that
/// registered it was dropped by a reorganisation, point it at this block instead.
async fn upsert_circuit(
    tx: &mut Tx<'_>,
    block_hash: &Hash,
    height: u64,
    t: &SignedTxView,
) -> Result<(), StoreError> {
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
    .bind(too_large(verifying_key.len(), height, BlockField::VerifyingKey)?)
    .bind(too_large(circuit.len(), height, BlockField::Circuit)?)
    .bind(&t.tx.body.sender.0[..])
    .bind(num(t.tx.body.fee.0)?)
    .bind(&block_hash[..])
    .bind(num(height)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// A length or a count as the INTEGER of a column. Too large is bad data, not a panic.
fn too_large<T>(n: T, height: u64, field: BlockField) -> Result<i32, BlockError>
where
    i32: TryFrom<T>,
{
    i32::try_from(n).map_err(|_| BlockError::TooLarge { height, field })
}

/// Sum of fees. A sum that does not fit in a u64 is bad data, not a panic.
fn add_fee(total: u64, fee: u64, height: u64) -> Result<u64, BlockError> {
    total
        .checked_add(fee)
        .ok_or(BlockError::FeeOverflow { height })
}

/// The transfers of one block, column by column, ready for `UNNEST`.
#[derive(Default)]
struct TransferRows {
    pos: Vec<i32>,
    txid: Vec<Vec<u8>>,
    sender: Vec<Vec<u8>>,
    recipient: Vec<Vec<u8>>,
    amount: Vec<i64>,
    fee: Vec<i64>,
    nonce: Vec<i64>,
    /// Sum of the fees, for the block counters.
    fees: u64,
}

impl TransferRows {
    fn push(
        &mut self,
        height: u64,
        pos: i32,
        t: &SignedTxView,
        to: Hash,
        amount: u64,
    ) -> Result<(), StoreError> {
        let b = &t.tx.body;
        self.pos.push(pos);
        self.txid.push(t.txid.hash().0.to_vec());
        self.sender.push(b.sender.0.to_vec());
        self.recipient.push(to.to_vec());
        self.amount.push(num(amount)?);
        self.fee.push(num(b.fee.0)?);
        self.nonce.push(num(b.nonce.0)?);
        self.fees = add_fee(self.fees, b.fee.0, height)?;
        Ok(())
    }

    async fn write(&self, tx: &mut Tx<'_>, block_hash: &Hash) -> Result<(), StoreError> {
        if self.pos.is_empty() {
            return Ok(());
        }
        sqlx::query(
            "INSERT INTO transfers (block_hash, position, txid, sender, recipient, amount, fee, nonce) \
             SELECT $1::bytea, u.position, u.txid, u.sender, u.recipient, u.amount, u.fee, u.nonce \
             FROM UNNEST($2::int4[], $3::bytea[], $4::bytea[], $5::bytea[], $6::int8[], $7::int8[], $8::int8[]) \
                  AS u (position, txid, sender, recipient, amount, fee, nonce)",
        )
        .bind(&block_hash[..])
        .bind(&self.pos)
        .bind(&self.txid)
        .bind(&self.sender)
        .bind(&self.recipient)
        .bind(&self.amount)
        .bind(&self.fee)
        .bind(&self.nonce)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }
}

/// Queue every address the block touches to be copied from the node again.
async fn touch_accounts(tx: &mut Tx<'_>, block_hash: &Hash) -> Result<(), StoreError> {
    sqlx::query("SELECT touch_block_accounts($1)")
        .bind(&block_hash[..])
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Insert one block and everything inside it, in the caller's transaction, so many blocks and
/// a checkpoint can be committed together.
/// Returns true if the block is new. A block that is already there writes nothing, except
/// that one a reorganisation had dropped is made canonical again.
///
/// Blocks must come in height order: proof requests point to circuits that an earlier block
/// registered, and the database enforces that.
pub async fn insert_block(tx: &mut Tx<'_>, view: &BlockView) -> Result<bool, StoreError> {
    let h = &view.header;
    let height = h.height.get();
    let hash = view.id.hash().0;
    let block = to_block(view);
    check_consistent(view, &block)?;
    let size = too_large(encoded_len(&block), height, BlockField::Size)?;
    let leaves = too_large(block.leaf_count(), height, BlockField::Entries)?;

    sqlx::query("INSERT INTO miners (address) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(&h.miner.0[..])
        .execute(&mut **tx)
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
    .bind(leaves)
    .bind(i64::from(h.chain_id.get()))
    .bind(num(h.system_id.get())?)
    .bind(&h.target.0[..])
    .bind(h.nonce.get().to_string()) // a u64 can be above BIGINT, so it goes in as NUMERIC
    .execute(&mut **tx)
    .await?
    .rows_affected();

    if inserted == 0 {
        // Known block. After a reorganisation it may have been orphaned and the node's chain
        // went back to it: make it canonical again and re-point its circuits.
        let revived = sqlx::query(
            "UPDATE blocks SET canonical = TRUE, orphaned_at = NULL WHERE hash = $1 AND NOT canonical",
        )
        .bind(&hash[..])
        .execute(&mut **tx)
        .await?
        .rows_affected();
        if revived > 0 {
            for t in &view.txs {
                upsert_circuit(tx, &hash, height, t).await?;
            }
            touch_accounts(tx, &hash).await?;
        }
        return Ok(false);
    }

    // Transfers and circuit registrations (block.txs). A block can hold hundreds of transfers,
    // so they are collected and written with one statement instead of one round trip each.
    // Registrations are few and go one by one. The counters are written at the end.
    let mut transfers = TransferRows::default();
    let mut registrations = 0usize;
    let mut registration_fees = 0u64;
    for (pos, t) in view.txs.iter().enumerate() {
        let b = &t.tx.body;
        match &b.kind {
            TxKind::Transfer { to, amount } => transfers.push(
                height,
                too_large(pos, height, BlockField::Position)?,
                t,
                to.0,
                amount.0,
            )?,
            TxKind::RegisterCircuit { .. } => {
                upsert_circuit(tx, &hash, height, t).await?;
                registrations += 1;
                registration_fees = add_fee(registration_fees, b.fee.0, height)?;
            }
            _ => {
                return Err(BlockError::UnexpectedTx {
                    height,
                    position: pos,
                }
                .into());
            }
        }
    }
    transfers.write(tx, &hash).await?;

    // proof requests with their proofs (block.proof_txs), at most a few per block
    let mut proof_fees = 0u64;
    for (pos, p) in view.proof_txs.iter().enumerate() {
        let b = &p.request.tx.body;
        proof_fees = add_fee(proof_fees, b.fee.0, height)?;
        let TxKind::ProofRequest {
            circuit,
            public_inputs,
            witness,
        } = &b.kind
        else {
            return Err(BlockError::NotAProofRequest {
                height,
                position: pos,
            }
            .into());
        };
        sqlx::query(
            "INSERT INTO proof_txs (block_hash, position, proof_hash, request_txid, circuit_id, requester, \
             fee, nonce, usage_counter, proof_bytes, witness_bytes, witness_sha256, public_input_count, eta, mask) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
        )
        .bind(&hash[..])
        .bind(too_large(pos, height, BlockField::Position)?)
        .bind(&p.proof_hash.hash().0[..])
        .bind(&p.request_txid.hash().0[..])
        .bind(&circuit.hash().0[..])
        .bind(&b.sender.0[..])
        .bind(num(b.fee.0)?)
        .bind(num(b.nonce.0)?)
        .bind(num(p.usage_counter.0)?)
        .bind(too_large(p.proof.len(), height, BlockField::ProofBytes)?)
        .bind(too_large(witness.len(), height, BlockField::WitnessBytes)?)
        .bind(&sha256(&[witness]).0[..])
        .bind(too_large(public_inputs.len(), height, BlockField::PublicInputs)?)
        .bind(&p.eta.0[..])
        .bind(&p.mask.0[..])
        .execute(&mut **tx)
        .await?;
    }

    // The numbers that api.blocks and network_stats read (migration 0010).
    let fee_total = add_fee(
        add_fee(transfers.fees, registration_fees, height)?,
        proof_fees,
        height,
    )?;
    sqlx::query(
        "UPDATE blocks SET transfer_count = $2, registration_count = $3, proof_count = $4, \
         transfer_fees = $5, fee_total = $6, \
         useful_work = (SELECT coalesce(sum(ci.constraint_count), 0)::bigint \
                          FROM proof_txs p JOIN circuits ci ON ci.circuit_id = p.circuit_id \
                         WHERE p.block_hash = $1) \
         WHERE hash = $1",
    )
    .bind(&hash[..])
    .bind(too_large(
        transfers.pos.len(),
        height,
        BlockField::Transfers,
    )?)
    .bind(too_large(registrations, height, BlockField::Registrations)?)
    .bind(too_large(view.proof_txs.len(), height, BlockField::Proofs)?)
    .bind(num(transfers.fees)?)
    .bind(num(fee_total)?)
    .execute(&mut **tx)
    .await?;

    touch_accounts(tx, &hash).await?;
    Ok(true)
}

/// A reorganisation dropped every canonical block above `height`. They stay in the database,
/// marked, and the addresses they touched are queued to be copied from the node again.
/// Returns how many blocks were dropped.
pub async fn orphan_above(tx: &mut Tx<'_>, height: u64) -> Result<u64, StoreError> {
    sqlx::query("SELECT touch_block_accounts(hash) FROM blocks WHERE canonical AND height > $1")
        .bind(num(height)?)
        .execute(&mut **tx)
        .await?;
    let dropped = sqlx::query(
        "UPDATE blocks SET canonical = FALSE, orphaned_at = now() WHERE canonical AND height > $1",
    )
    .bind(num(height)?)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    Ok(dropped)
}

pub async fn save_checkpoint(
    tx: &mut Tx<'_>,
    node: &NodeId,
    at: Checkpoint,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO sync_state (node_id, height, hash) VALUES ($1, $2, $3) \
         ON CONFLICT (node_id) DO UPDATE SET height = EXCLUDED.height, hash = EXCLUDED.hash, updated_at = now()",
    )
    .bind(node.as_str())
    .bind(num(at.height)?)
    .bind(&at.hash[..])
    .execute(&mut **tx)
    .await?;
    Ok(())
}

impl Store {
    pub async fn checkpoint(&self, node: &NodeId) -> Result<Option<Checkpoint>, StoreError> {
        let row = sqlx::query("SELECT height, hash FROM sync_state WHERE node_id = $1")
            .bind(node.as_str())
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else { return Ok(None) };
        let (height, hash): (i64, Vec<u8>) = (row.get(0), row.get(1));
        let hash = hash.try_into().map_err(|_| StoreError::BadLength {
            column: "sync_state.hash",
        })?;
        Ok(Some(Checkpoint {
            height: unsigned(height)?,
            hash,
        }))
    }

    /// Hash of our canonical block at a height.
    pub async fn canonical_hash_at(&self, height: u64) -> Result<Option<Hash>, StoreError> {
        let h: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT hash FROM blocks WHERE height = $1 AND canonical")
                .bind(num(height)?)
                .fetch_optional(&self.pool)
                .await?;
        h.map(|h| {
            h.try_into().map_err(|_| StoreError::BadLength {
                column: "blocks.hash",
            })
        })
        .transpose()
    }

    /// Refresh the materialized views of the explorer API (`api.stats`, `api.miner_shares`,
    /// `api.useful_work`, migration 0003). Run it after the transaction that changed the
    /// chain has committed, never inside it.
    pub async fn refresh_stats(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT api.refresh_stats()")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakechain::demo_chain;

    async fn put(store: &Store, b: &BlockView) -> Result<bool, StoreError> {
        let mut tx = store.begin().await?;
        let inserted = insert_block(&mut tx, b).await?;
        tx.commit().await?;
        Ok(inserted)
    }

    async fn count(store: &Store, table: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(store.pool())
            .await
            .unwrap()
    }

    #[sqlx::test]
    async fn twenty_blocks_are_in_the_db(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let blocks = demo_chain(20);
        for b in &blocks {
            assert!(put(&store, b).await.unwrap());
        }
        assert_eq!(count(&store, "blocks").await, 20);
        assert_eq!(
            count(&store, "proof_txs").await,
            blocks.iter().map(|b| b.proof_txs.len() as i64).sum::<i64>()
        );
        let transfers = blocks
            .iter()
            .flat_map(|b| &b.txs)
            .filter(|t| matches!(t.tx.body.kind, TxKind::Transfer { .. }))
            .count() as i64;
        assert_eq!(count(&store, "transfers").await, transfers);
        assert_eq!(count(&store, "circuits").await, 3);
        // derived fields from the node are stored
        assert_eq!(
            count(&store, "proof_txs WHERE eta IS NULL OR mask IS NULL").await,
            0
        );
        // every block except the genesis points to a stored block
        let unlinked = "blocks b WHERE b.height > 0 AND NOT EXISTS (SELECT 1 FROM blocks p WHERE p.hash = b.prev_hash)";
        assert_eq!(count(&store, unlinked).await, 0);
        // header fields of the node's format
        assert_eq!(
            count(
                &store,
                "blocks WHERE target IS NULL OR pow_nonce IS NULL OR chain_id IS NULL"
            )
            .await,
            0
        );
        // every block queued its addresses for copying from the node
        assert!(count(&store, "account_refresh").await >= 3);
    }

    #[sqlx::test]
    async fn inserting_twice_changes_nothing(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let blocks = demo_chain(20);
        for b in &blocks {
            put(&store, b).await.unwrap();
        }
        let before = (
            count(&store, "blocks").await,
            count(&store, "proof_txs").await,
            count(&store, "transfers").await,
        );
        for b in &blocks {
            assert!(!put(&store, b).await.unwrap());
        }
        let after = (
            count(&store, "blocks").await,
            count(&store, "proof_txs").await,
            count(&store, "transfers").await,
        );
        assert_eq!(before, after);
    }

    #[sqlx::test]
    async fn a_block_that_does_not_add_up_is_refused(pool: sqlx::PgPool) {
        let store = Store::new(pool);
        let mut blocks = demo_chain(3);
        // tamper with the data, keep the id: the node's answer is inconsistent
        blocks[2].header.nonce = pouw_types::U64::new(blocks[2].header.nonce.get() ^ 1);
        assert!(put(&store, &blocks[0]).await.is_ok());
        assert!(matches!(
            put(&store, &blocks[2]).await,
            Err(StoreError::Block(BlockError::IdMismatch { height: 2 }))
        ));
        assert_eq!(count(&store, "blocks").await, 1);
    }

    #[test]
    fn the_json_the_node_serves_round_trips() {
        // what the HTTP client reads is what the node's own types write
        let b = demo_chain(3).pop().unwrap();
        let json = serde_json::to_string(&b).unwrap();
        assert!(json.contains("\"parent\":\"blk:"), "{json}");
        let back: BlockView = serde_json::from_str(&json).unwrap();
        assert_eq!(back, b);
    }
}
