//! What the indexer needs from a node: its head and ranges of blocks, as the node API serves
//! them (`pouw_api::view`). A real HTTP client and an in-memory node for tests and local runs.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use pouw_api::view::{BlockView, BlocksPage, HeaderView};

/// The node's current chain tip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    pub height: u64,
    pub hash: [u8; 32],
}

/// The node API serves at most this many blocks per call.
pub const MAX_BLOCKS_PER_CALL: usize = pouw_api::view::MAX_BLOCKS_PER_CALL as usize;

pub trait NodeClient: Send + Sync {
    /// Tip of the node's canonical chain.
    fn head(&self) -> impl Future<Output = anyhow::Result<Head>> + Send;
    /// Canonical blocks with `from <= height <= to`, in height order, at most
    /// `MAX_BLOCKS_PER_CALL`. Heights the node does not have are left out.
    fn blocks(
        &self,
        from: u64,
        to: u64,
    ) -> impl Future<Output = anyhow::Result<Vec<BlockView>>> + Send;
}

pub fn read_block_views(path: &str) -> anyhow::Result<Vec<BlockView>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("cannot read {path}"))?;
    text.lines()
        .enumerate()
        .filter(|(_, l)| !l.trim().is_empty())
        .map(|(i, l)| {
            serde_json::from_str(l).with_context(|| format!("{path} line {}: not a block", i + 1))
        })
        .collect()
}

pub fn write_block_views(path: &str, blocks: &[BlockView]) -> anyhow::Result<()> {
    let mut out = String::new();
    for b in blocks {
        out.push_str(&serde_json::to_string(b)?);
        out.push('\n');
    }
    std::fs::write(path, out).with_context(|| format!("cannot write {path}"))
}

// ------------------------------------------------------------------------------ real client

/// The node's REST API (`GET /v1/blocks/head`, `GET /v1/blocks?from=&to=`).
pub struct HttpNode {
    client: reqwest::Client,
    base: String,
}

impl HttpNode {
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(60))
            .build()?;
        Ok(Self {
            client,
            base: base_url.trim_end_matches('/').to_string(),
        })
    }

    async fn get<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, u64)],
    ) -> anyhow::Result<T> {
        let resp = self
            .client
            .get(format!("{}{path}", self.base))
            .query(query)
            .send()
            .await
            .with_context(|| format!("GET {path} failed"))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("GET {path}: the node answered {status}: {body}");
        }
        resp.json()
            .await
            .with_context(|| format!("GET {path}: the answer is not the expected JSON"))
    }
}

impl NodeClient for HttpNode {
    async fn head(&self) -> anyhow::Result<Head> {
        let h: HeaderView = self.get(pouw_api::path::HEAD, &[]).await?;
        Ok(Head {
            height: h.header.height.get(),
            hash: h.id.hash().0,
        })
    }

    async fn blocks(&self, from: u64, to: u64) -> anyhow::Result<Vec<BlockView>> {
        let page: BlocksPage = self
            .get(pouw_api::path::BLOCKS, &[("from", from), ("to", to)])
            .await?;
        Ok(page.blocks)
    }
}

// ------------------------------------------------------------------------------ in memory

/// A node that lives in memory. `set_chain` swaps the whole chain, which is how tests
/// simulate a reorganisation. `from_file` serves a file of blocks, for running the indexer
/// without a node.
#[derive(Clone, Default)]
pub struct MemoryNode {
    chain: Arc<Mutex<Vec<BlockView>>>,
}

// Parts of this are only used by tests and by local runs with NODE_FIXTURES.
#[allow(dead_code)]
impl MemoryNode {
    pub fn new(chain: Vec<BlockView>) -> Self {
        Self {
            chain: Arc::new(Mutex::new(chain)),
        }
    }

    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        Ok(Self::new(read_block_views(path)?))
    }

    pub fn set_chain(&self, chain: Vec<BlockView>) {
        *self.chain.lock().expect("lock") = chain;
    }
}

impl NodeClient for MemoryNode {
    async fn head(&self) -> anyhow::Result<Head> {
        let chain = self.chain.lock().expect("lock");
        let last = chain
            .last()
            .ok_or_else(|| anyhow!("the node has no blocks"))?;
        Ok(Head {
            height: last.header.height.get(),
            hash: last.id.hash().0,
        })
    }

    async fn blocks(&self, from: u64, to: u64) -> anyhow::Result<Vec<BlockView>> {
        let chain = self.chain.lock().expect("lock");
        Ok(chain
            .iter()
            .filter(|b| (from..=to).contains(&b.header.height.get()))
            .take(MAX_BLOCKS_PER_CALL)
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::{
        Json, Router,
        extract::{Query, State},
        routing::get,
    };
    use sqlx::PgPool;

    use super::*;
    use crate::fakechain::ChainBuilder;
    use crate::metrics::Metrics;
    use crate::sync::{SyncConfig, sync_until_caught_up};

    type Chain = Arc<Vec<BlockView>>;

    /// Serve a chain like the node API does: `/v1/blocks/head` and `/v1/blocks?from=&to=` (max 100).
    async fn serve(chain: Vec<BlockView>) -> String {
        async fn head(State(c): State<Chain>) -> Json<HeaderView> {
            let b = c.last().expect("not empty");
            Json(HeaderView {
                id: b.id,
                header: b.header,
            })
        }
        async fn blocks(
            State(c): State<Chain>,
            Query(q): Query<HashMap<String, u64>>,
        ) -> Json<BlocksPage> {
            let (from, to) = (q["from"], q["to"]);
            let blocks = c
                .iter()
                .filter(|b| (from..=to).contains(&b.header.height.get()))
                .take(MAX_BLOCKS_PER_CALL)
                .cloned()
                .collect();
            Json(BlocksPage { blocks })
        }
        let app = Router::new()
            .route("/v1/blocks/head", get(head))
            .route("/v1/blocks", get(blocks))
            .with_state(Arc::new(chain));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    #[sqlx::test]
    async fn the_http_node_is_followed_over_several_pages(pool: PgPool) {
        // 250 blocks need three requests of at most 100
        let chain = ChainBuilder::new(5).take_empty(250);
        let url = serve(chain.clone()).await;
        let node = HttpNode::new(&url).unwrap();

        let head = node.head().await.unwrap();
        assert_eq!((head.height, head.hash), (249, chain[249].id.hash().0));
        assert_eq!(node.blocks(0, 1000).await.unwrap().len(), 100);

        let cfg = SyncConfig {
            node_id: "http".into(),
            poll: Duration::from_millis(10),
        };
        sync_until_caught_up(&pool, &node, &cfg, &Metrics::new())
            .await
            .unwrap();
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM blocks WHERE canonical")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 250);
    }

    #[tokio::test]
    async fn a_node_that_is_down_is_an_error_not_a_panic() {
        let dead = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            format!("http://{}", l.local_addr().unwrap())
        };
        assert!(HttpNode::new(&dead).unwrap().head().await.is_err());
    }
}
