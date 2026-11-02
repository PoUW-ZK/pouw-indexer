//! What the indexer needs from a node, and an in-memory node for tests and local runs.
//!
//! The real HTTP client will implement `NodeClient` once the node's block format is
//! confirmed (see node-api.md). Everything else only depends on this trait.

use std::future::Future;
use std::sync::{Arc, Mutex};

use anyhow::Context;

use crate::block::Block;
use crate::fixtures;

/// The node's current chain tip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    pub height: u64,
    pub hash: [u8; 32],
}

/// The node API serves at most this many blocks per call.
pub const MAX_BLOCKS_PER_CALL: usize = 100;

pub trait NodeClient: Send + Sync {
    /// Tip of the node's canonical chain.
    fn head(&self) -> impl Future<Output = anyhow::Result<Head>> + Send;
    /// Canonical blocks with `from <= height <= to`, in height order, at most
    /// `MAX_BLOCKS_PER_CALL`. Heights the node does not have are left out.
    fn blocks(&self, from: u64, to: u64)
    -> impl Future<Output = anyhow::Result<Vec<Block>>> + Send;
}

/// A node that lives in memory. `set_chain` swaps the whole chain, which is how tests
/// simulate a reorganisation. `from_file` serves a fixtures file, for running the indexer
/// without a node.
#[derive(Clone, Default)]
pub struct MemoryNode {
    chain: Arc<Mutex<Vec<Block>>>,
}

// Parts of this are only used by tests and by local runs with NODE_FIXTURES.
#[allow(dead_code)]
impl MemoryNode {
    pub fn new(chain: Vec<Block>) -> Self {
        Self {
            chain: Arc::new(Mutex::new(chain)),
        }
    }

    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        Ok(Self::new(fixtures::read_blocks(path)?))
    }

    pub fn set_chain(&self, chain: Vec<Block>) {
        *self.chain.lock().expect("lock") = chain;
    }
}

impl NodeClient for MemoryNode {
    async fn head(&self) -> anyhow::Result<Head> {
        let chain = self.chain.lock().expect("lock");
        let last = chain.last().context("the node has no blocks")?;
        Ok(Head {
            height: last.header.height,
            hash: last.header.hash(),
        })
    }

    async fn blocks(&self, from: u64, to: u64) -> anyhow::Result<Vec<Block>> {
        let chain = self.chain.lock().expect("lock");
        Ok(chain
            .iter()
            .filter(|b| (from..=to).contains(&b.header.height))
            .take(MAX_BLOCKS_PER_CALL)
            .cloned()
            .collect())
    }
}
