//! Keep the database in step with a node: follow its chain, resume after a restart,
//! and undo the blocks the node dropped in a reorganisation.
//!
//! The idea in one paragraph: the checkpoint (`sync_state`) says "I have written the
//! node's chain up to this block". Each round asks the node for the next page of blocks.
//! If the first block of that page does not point at the checkpoint block, the node's chain
//! changed under us: walk back to the newest block both sides still agree on (the common
//! ancestor), mark everything above it as orphaned (kept, never deleted), and continue from
//! there. A page and the new checkpoint are written in one transaction, so a crash at any
//! moment leaves the database consistent.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail};
use sqlx::{PgConnection, PgPool, Row};

use crate::metrics::Metrics;
use crate::node::{MAX_BLOCKS_PER_CALL, NodeClient};
use crate::store;

pub struct SyncConfig {
    /// Name of the node we follow, the key of its row in `sync_state`.
    pub node_id: String,
    /// How long to wait when we are caught up.
    pub poll: Duration,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Progress {
    /// Nothing new.
    CaughtUp,
    /// A page of blocks was written.
    Advanced { blocks: usize },
    /// The node's chain changed; `depth` blocks were marked orphaned.
    Reorg { depth: u64 },
}

type Checkpoint = Option<(u64, [u8; 32])>;

async fn load_checkpoint(pool: &PgPool, node_id: &str) -> anyhow::Result<Checkpoint> {
    let row = sqlx::query("SELECT height, hash FROM sync_state WHERE node_id = $1")
        .bind(node_id)
        .fetch_optional(pool)
        .await?;
    let Some(row) = row else { return Ok(None) };
    let height: i64 = row.get(0);
    let hash: Vec<u8> = row.get(1);
    let hash: [u8; 32] = hash
        .try_into()
        .map_err(|_| anyhow!("sync_state.hash is not 32 bytes"))?;
    Ok(Some((height as u64, hash)))
}

async fn save_checkpoint(
    conn: &mut PgConnection,
    node_id: &str,
    height: u64,
    hash: &[u8; 32],
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO sync_state (node_id, height, hash) VALUES ($1, $2, $3) \
         ON CONFLICT (node_id) DO UPDATE SET height = EXCLUDED.height, hash = EXCLUDED.hash, updated_at = now()",
    )
    .bind(node_id)
    .bind(height as i64)
    .bind(&hash[..])
    .execute(&mut *conn)
    .await?;
    Ok(())
}

async fn stored_hash_at(pool: &PgPool, height: u64) -> anyhow::Result<Option<[u8; 32]>> {
    let row: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT hash FROM blocks WHERE height = $1 AND canonical")
            .bind(height as i64)
            .fetch_optional(pool)
            .await?;
    Ok(row.and_then(|h| h.try_into().ok()))
}

/// The node's chain no longer contains our checkpoint block. Find the newest height where
/// our canonical block and the node's block are the same, orphan everything above it and
/// move the checkpoint back there.
async fn reorganise<N: NodeClient>(
    pool: &PgPool,
    node: &N,
    cfg: &SyncConfig,
    m: &Metrics,
    checkpoint_height: u64,
    node_height: u64,
) -> anyhow::Result<Progress> {
    let mut h = checkpoint_height.min(node_height);
    let (ancestor_height, ancestor_hash) = loop {
        let ours = stored_hash_at(pool, h).await?;
        let theirs = node
            .blocks(h, h)
            .await?
            .into_iter()
            .next()
            .map(|b| b.id.hash().0);
        if let Some(same) = ours.filter(|o| Some(*o) == theirs) {
            break (h, same);
        }
        if h == 0 {
            bail!("the node's chain shares no block with the database, not even the genesis");
        }
        h -= 1;
    };

    let mut tx = pool.begin().await?;
    sqlx::query(
        "UPDATE blocks SET canonical = FALSE, orphaned_at = now() WHERE canonical AND height > $1",
    )
    .bind(ancestor_height as i64)
    .execute(&mut *tx)
    .await?;
    save_checkpoint(&mut tx, &cfg.node_id, ancestor_height, &ancestor_hash).await?;
    tx.commit().await?;

    let depth = checkpoint_height - ancestor_height;
    m.reorgs.inc();
    m.last_reorg_depth.set(depth as i64);
    m.indexed_height.set(ancestor_height as i64);
    Ok(Progress::Reorg { depth })
}

/// One round: write one page of blocks, or handle a reorganisation, or report caught up.
pub async fn sync_step<N: NodeClient>(
    pool: &PgPool,
    node: &N,
    cfg: &SyncConfig,
    m: &Metrics,
) -> anyhow::Result<Progress> {
    let head = node.head().await.context("asking the node for its head")?;
    m.node_height.set(head.height as i64);
    let checkpoint = load_checkpoint(pool, &cfg.node_id).await?;

    let next = match checkpoint {
        None => 0,
        Some((h, hash)) => {
            m.indexed_height.set(h as i64);
            m.lag_blocks.set(head.height as i64 - h as i64);
            if h >= head.height {
                if h == head.height && hash == head.hash {
                    return Ok(Progress::CaughtUp);
                }
                // the node's chain is shorter than ours, or has another block at our height
                return reorganise(pool, node, cfg, m, h, head.height).await;
            }
            h + 1
        }
    };

    let to = head.height.min(next + MAX_BLOCKS_PER_CALL as u64 - 1);
    let blocks = node
        .blocks(next, to)
        .await
        .with_context(|| format!("fetching blocks {next}..{to}"))?;
    if blocks.is_empty() {
        bail!("the node returned no blocks for heights {next}..{to}");
    }

    // The page must be one unbroken chain, and must continue from our checkpoint.
    for (i, b) in blocks.iter().enumerate() {
        if b.header.height.get() != next + i as u64 {
            bail!(
                "the node returned height {} where {} was expected",
                b.header.height.get(),
                next + i as u64
            );
        }
        if i > 0 && b.header.parent.hash().0 != blocks[i - 1].id.hash().0 {
            bail!(
                "block {} does not point at block {}",
                b.header.height.get(),
                b.header.height.get() - 1
            );
        }
    }
    // the page must continue from our checkpoint block, otherwise the node's chain changed
    if let Some((h, _)) = checkpoint.filter(|(_, hash)| blocks[0].header.parent.hash().0 != *hash) {
        return reorganise(pool, node, cfg, m, h, head.height).await;
    }

    let started = Instant::now();
    let mut tx = pool.begin().await?;
    let mut added = 0u64;
    for b in &blocks {
        if store::insert_block_tx(&mut tx, b).await? {
            added += 1;
        }
    }
    let last = blocks.last().expect("checked non-empty");
    save_checkpoint(
        &mut tx,
        &cfg.node_id,
        last.header.height.get(),
        &last.id.hash().0,
    )
    .await?;
    tx.commit().await?;

    m.batch_seconds.observe(started.elapsed().as_secs_f64());
    m.blocks_written.inc_by(added);
    m.indexed_height.set(last.header.height.get() as i64);
    m.lag_blocks
        .set(head.height as i64 - last.header.height.get() as i64);
    Ok(Progress::Advanced {
        blocks: blocks.len(),
    })
}

/// Keep going until there is nothing new (used by tests and by `seed`-like one-shot runs).
#[allow(dead_code)]
pub async fn sync_until_caught_up<N: NodeClient>(
    pool: &PgPool,
    node: &N,
    cfg: &SyncConfig,
    m: &Metrics,
) -> anyhow::Result<()> {
    for _ in 0..100_000 {
        if sync_step(pool, node, cfg, m).await? == Progress::CaughtUp {
            return Ok(());
        }
    }
    bail!("sync did not settle after 100000 rounds")
}

/// The background loop of `serve`. Never returns. Errors (node down, bad data) are
/// counted, logged and retried with a growing pause, so the HTTP server stays up.
pub async fn run<N: NodeClient>(pool: PgPool, node: N, cfg: SyncConfig, m: Arc<Metrics>) {
    let mut failures = 0u32;
    loop {
        match sync_step(&pool, &node, &cfg, &m).await {
            Ok(Progress::CaughtUp) => {
                failures = 0;
                tokio::time::sleep(cfg.poll).await;
            }
            Ok(Progress::Advanced { blocks }) => {
                failures = 0;
                println!("sync: wrote {blocks} blocks");
            }
            Ok(Progress::Reorg { depth }) => {
                failures = 0;
                println!("sync: the node reorganised, {depth} blocks marked orphaned");
            }
            Err(e) => {
                m.node_errors.inc();
                failures += 1;
                eprintln!("sync error (attempt {failures}): {e:#}");
                tokio::time::sleep(Duration::from_secs(1 << failures.min(5))).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fakechain::{ChainBuilder, demo_chain};
    use crate::node::MemoryNode;
    use pouw_api::view::BlockView;

    fn cfg() -> SyncConfig {
        SyncConfig {
            node_id: "test-node".into(),
            poll: Duration::from_millis(10),
        }
    }

    async fn count(pool: &PgPool, sql: &str) -> i64 {
        sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
    }

    async fn canonical_hashes(pool: &PgPool) -> Vec<Vec<u8>> {
        sqlx::query_scalar("SELECT hash FROM blocks WHERE canonical ORDER BY height")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    fn hashes(chain: &[BlockView]) -> Vec<Vec<u8>> {
        chain.iter().map(|b| b.id.hash().0.to_vec()).collect()
    }

    /// Two branches that share blocks 0..=10: `a` has 20 blocks, `b` has 14 (blocks 11..=13 differ).
    fn two_branches() -> (Vec<BlockView>, Vec<BlockView>) {
        let mut builder = ChainBuilder::new(42);
        let mut a = builder.take(11);
        let mut rival = builder.reseeded(7);
        a.extend(builder.take(9));
        let mut b = a[..=10].to_vec();
        b.extend(rival.take(3));
        (a, b)
    }

    #[sqlx::test]
    async fn restart_in_the_middle_does_not_duplicate(pool: PgPool) {
        let chain = demo_chain(20);
        let node = MemoryNode::new(chain[..7].to_vec());
        let m = Metrics::new();
        sync_until_caught_up(&pool, &node, &cfg(), &m)
            .await
            .unwrap();
        assert_eq!(count(&pool, "SELECT count(*) FROM blocks").await, 7);

        // "restart": the node has more blocks now, and we sync again from the checkpoint
        node.set_chain(chain.clone());
        sync_until_caught_up(&pool, &node, &cfg(), &m)
            .await
            .unwrap();
        // and once more with nothing new
        sync_until_caught_up(&pool, &node, &cfg(), &m)
            .await
            .unwrap();

        assert_eq!(canonical_hashes(&pool).await, hashes(&chain));
        let proofs: i64 = chain.iter().map(|b| b.proof_txs.len() as i64).sum();
        assert_eq!(count(&pool, "SELECT count(*) FROM proof_txs").await, proofs);
        assert_eq!(count(&pool, "SELECT count(*) FROM circuits").await, 3);
        assert_eq!(count(&pool, "SELECT height FROM sync_state").await, 19);
    }

    #[sqlx::test]
    async fn the_database_follows_a_reorganisation_and_back(pool: PgPool) {
        let (a, b) = two_branches();
        assert_eq!((a.len(), b.len()), (20, 14));
        let node = MemoryNode::new(a.clone());
        let m = Metrics::new();

        sync_until_caught_up(&pool, &node, &cfg(), &m)
            .await
            .unwrap();
        assert_eq!(canonical_hashes(&pool).await, hashes(&a));

        // the node switches to branch b
        node.set_chain(b.clone());
        sync_until_caught_up(&pool, &node, &cfg(), &m)
            .await
            .unwrap();
        assert_eq!(
            canonical_hashes(&pool).await,
            hashes(&b),
            "database matches the node"
        );
        // the dropped blocks are kept, marked, with their transactions
        assert_eq!(
            count(&pool, "SELECT count(*) FROM blocks WHERE NOT canonical").await,
            9
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM blocks WHERE NOT canonical AND orphaned_at IS NOT NULL"
            )
            .await,
            9
        );
        let in_orphans: i64 = a[11..].iter().map(|x| x.proof_txs.len() as i64).sum();
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM proof_txs p JOIN blocks bl ON bl.hash = p.block_hash WHERE NOT bl.canonical"
            )
            .await,
            in_orphans
        );
        assert_eq!(count(&pool, "SELECT height FROM sync_state").await, 13);
        let text = m.render();
        assert!(text.contains("pouw_indexer_reorgs_total 1"), "{text}");
        assert!(text.contains("pouw_indexer_last_reorg_depth 9"), "{text}");

        // and the node goes back to branch a: orphaned blocks become canonical again
        node.set_chain(a.clone());
        sync_until_caught_up(&pool, &node, &cfg(), &m)
            .await
            .unwrap();
        assert_eq!(canonical_hashes(&pool).await, hashes(&a));
        assert_eq!(
            count(&pool, "SELECT count(*) FROM blocks WHERE NOT canonical").await,
            3
        );
        assert_eq!(count(&pool, "SELECT count(*) FROM blocks").await, 23);
    }

    #[sqlx::test]
    async fn a_shorter_node_chain_orphans_the_extra_blocks(pool: PgPool) {
        let a = demo_chain(20);
        let node = MemoryNode::new(a.clone());
        let m = Metrics::new();
        sync_until_caught_up(&pool, &node, &cfg(), &m)
            .await
            .unwrap();

        node.set_chain(a[..=12].to_vec());
        sync_until_caught_up(&pool, &node, &cfg(), &m)
            .await
            .unwrap();
        assert_eq!(canonical_hashes(&pool).await, hashes(&a[..=12]));
        assert_eq!(
            count(&pool, "SELECT count(*) FROM blocks WHERE NOT canonical").await,
            7
        );
    }

    #[sqlx::test]
    async fn ten_thousand_blocks_are_written_fast(pool: PgPool) {
        let node = MemoryNode::new(ChainBuilder::new(1).take_empty(10_000));
        let m = Metrics::new();

        let started = Instant::now();
        sync_until_caught_up(&pool, &node, &cfg(), &m)
            .await
            .unwrap();
        let took = started.elapsed();
        println!("10000 blocks written in {took:?}");

        assert_eq!(
            count(&pool, "SELECT count(*) FROM blocks WHERE canonical").await,
            10_000
        );
        assert!(took < Duration::from_secs(120), "took {took:?}");
        assert!(
            m.render()
                .contains("pouw_indexer_blocks_written_total 10000")
        );
    }
}
