//! What the indexer needs from a node: its head, ranges of blocks and accounts, as the node API
//! serves them (`pouw_api::view`). A real HTTP client and an in-memory node for tests.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use pouw_api::view::{AccountView, BlockView, BlocksPage, HeaderView};
use pouw_types::crypto::Address;
use pouw_types::{Amount, Nonce};

use crate::error::NodeError;
use crate::store::Hash;

/// The node's current chain tip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Head {
    pub height: u64,
    pub hash: Hash,
}

/// The node API serves at most this many blocks per call.
pub const MAX_BLOCKS_PER_CALL: usize = pouw_api::view::MAX_BLOCKS_PER_CALL as usize;

pub trait NodeClient: Send {
    /// Tip of the node's canonical chain.
    fn head(&mut self) -> impl Future<Output = Result<Head, NodeError>> + Send;
    /// Canonical blocks with `from <= height <= to`, in height order, at most
    /// `MAX_BLOCKS_PER_CALL`. Heights the node does not have are left out.
    fn blocks(
        &mut self,
        from: u64,
        to: u64,
    ) -> impl Future<Output = Result<Vec<Arc<BlockView>>, NodeError>> + Send;
    /// Balance and nonce of an address, `None` for an address the chain has not seen.
    fn account(
        &mut self,
        address: Hash,
    ) -> impl Future<Output = Result<Option<AccountView>, NodeError>> + Send;
    /// Many accounts, answers in the order of `addresses`. The default asks one after another;
    /// the HTTP client asks several at once, because after a block full of transfers there are
    /// hundreds of addresses to copy and one request at a time would put the indexer behind.
    fn accounts(
        &mut self,
        addresses: &[Hash],
    ) -> impl Future<Output = Result<Vec<Option<AccountView>>, NodeError>> + Send {
        async move {
            let mut out = Vec::with_capacity(addresses.len());
            for a in addresses {
                out.push(self.account(*a).await?);
            }
            Ok(out)
        }
    }
}

/// How many account requests the HTTP client keeps open at once.
const ACCOUNT_REQUESTS_AT_ONCE: usize = 16;

// ------------------------------------------------------------------------------ real client

/// The node's REST API (`GET /v1/blocks/head`, `/v1/blocks?from=&to=`, `/v1/accounts/{addr}`).
pub struct HttpNode {
    client: reqwest::Client,
    base: String,
}

impl HttpNode {
    pub fn new(base_url: &str) -> Result<Self, NodeError> {
        // the node answers in Borsh unless asked for JSON
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::ACCEPT,
            reqwest::header::HeaderValue::from_static("application/json"),
        );
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(60))
            .build()?;
        Ok(Self {
            client,
            base: base_url.trim_end_matches('/').to_string(),
        })
    }

    async fn get(&self, path: &str, query: &[(&str, u64)]) -> Result<reqwest::Response, NodeError> {
        Ok(self
            .client
            .get(format!("{}{path}", self.base))
            .query(query)
            .send()
            .await?)
    }

    /// The answer if it is a success, otherwise the status and body as an error.
    async fn expect_ok(
        resp: reqwest::Response,
        path: &str,
    ) -> Result<reqwest::Response, NodeError> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let body = resp.text().await.unwrap_or_default();
        Err(NodeError::Status {
            path: path.to_string(),
            status: status.as_u16(),
            body,
        })
    }
}

impl NodeClient for HttpNode {
    async fn head(&mut self) -> Result<Head, NodeError> {
        let path = pouw_api::path::HEAD;
        let resp = Self::expect_ok(self.get(path, &[]).await?, path).await?;
        let h: HeaderView = resp.json().await?;
        Ok(Head {
            height: h.header.height.get(),
            hash: h.id.hash().0,
        })
    }

    async fn blocks(&mut self, from: u64, to: u64) -> Result<Vec<Arc<BlockView>>, NodeError> {
        let path = pouw_api::path::BLOCKS;
        let resp =
            Self::expect_ok(self.get(path, &[("from", from), ("to", to)]).await?, path).await?;
        let page: BlocksPage = resp.json().await?;
        Ok(page.blocks.into_iter().map(Arc::new).collect())
    }

    async fn account(&mut self, address: Hash) -> Result<Option<AccountView>, NodeError> {
        fetch_account(&self.client, &self.base, address).await
    }

    async fn accounts(
        &mut self,
        addresses: &[Hash],
    ) -> Result<Vec<Option<AccountView>>, NodeError> {
        let mut answers: Vec<Option<Option<AccountView>>> =
            (0..addresses.len()).map(|_| None).collect();
        let mut running = tokio::task::JoinSet::new();
        let mut next = 0;
        let mut received = 0;
        while received < addresses.len() {
            while next < addresses.len() && running.len() < ACCOUNT_REQUESTS_AT_ONCE {
                let (client, base, address, index) = (
                    self.client.clone(),
                    self.base.clone(),
                    addresses[next],
                    next,
                );
                running.spawn(async move { (index, fetch_account(&client, &base, address).await) });
                next += 1;
            }
            // The first failure ends the call: dropping the set cancels the rest.
            let (index, answer) = running
                .join_next()
                .await
                .expect("a request is running while answers are missing")
                .map_err(|e| NodeError::Other(format!("account request: {e}")))?;
            answers[index] = Some(answer?);
            received += 1;
        }
        Ok(answers
            .into_iter()
            .map(|a| a.expect("every address was answered"))
            .collect())
    }
}

/// `GET /v1/accounts/{addr}`: `None` when the node does not know the address (404).
async fn fetch_account(
    client: &reqwest::Client,
    base: &str,
    address: Hash,
) -> Result<Option<AccountView>, NodeError> {
    let path = pouw_api::path::ACCOUNT.replace("{addr}", &Address(address).to_string());
    let resp = client.get(format!("{base}{path}")).send().await?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    Ok(Some(HttpNode::expect_ok(resp, &path).await?.json().await?))
}

// ------------------------------------------------------------------------------ in memory

/// A node that lives in memory, for tests and for running without a node. The fields are public:
/// a test simulates a reorganisation by assigning another `chain`, and sets what
/// `GET /v1/accounts/{addr}` answers in `balances`. Blocks are shared (`Arc`), so answering a
/// request copies pointers, not blocks.
#[derive(Default)]
pub struct MemoryNode {
    pub chain: Vec<Arc<BlockView>>,
    /// address -> (balance, nonce)
    pub balances: HashMap<Hash, (u64, u64)>,
}

/// A chain in the form `MemoryNode::chain` holds it.
pub fn share(chain: Vec<BlockView>) -> Vec<Arc<BlockView>> {
    chain.into_iter().map(Arc::new).collect()
}

impl MemoryNode {
    pub fn new(chain: Vec<BlockView>) -> Self {
        Self {
            chain: share(chain),
            balances: HashMap::new(),
        }
    }
}

// Nothing here waits, so the methods return a ready future instead of being `async fn`s.
impl NodeClient for MemoryNode {
    fn head(&mut self) -> impl Future<Output = Result<Head, NodeError>> + Send {
        std::future::ready(
            self.chain
                .last()
                .ok_or(NodeError::NoBlocks)
                .map(|last| Head {
                    height: last.header.height.get(),
                    hash: last.id.hash().0,
                }),
        )
    }

    fn blocks(
        &mut self,
        from: u64,
        to: u64,
    ) -> impl Future<Output = Result<Vec<Arc<BlockView>>, NodeError>> + Send {
        std::future::ready(Ok::<_, NodeError>(
            self.chain
                .iter()
                .filter(|b| (from..=to).contains(&b.header.height.get()))
                .take(MAX_BLOCKS_PER_CALL)
                .map(Arc::clone)
                .collect(),
        ))
    }

    fn account(
        &mut self,
        address: Hash,
    ) -> impl Future<Output = Result<Option<AccountView>, NodeError>> + Send {
        std::future::ready(Ok::<_, NodeError>(self.balances.get(&address).map(
            |&(balance, nonce)| AccountView {
                address: Address(address),
                balance: Amount(balance),
                nonce: Nonce(nonce),
            },
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use axum::routing::get;
    use axum::{Json, Router};

    use super::*;
    use crate::fakechain::ChainBuilder;
    use crate::metrics::Metrics;
    use crate::store::Store;
    use crate::sync::{SyncConfig, sync_until_caught_up};

    type Chain = Arc<Vec<BlockView>>;

    /// Serve a chain like the node API does: head, blocks (max 100 per call) and one account.
    async fn serve(chain: Vec<BlockView>, known: Hash) -> String {
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
        async fn account(
            State(known): State<Hash>,
            Path(addr): Path<String>,
        ) -> Result<Json<AccountView>, StatusCode> {
            if addr == Address(known).to_string() {
                Ok(Json(AccountView {
                    address: Address(known),
                    balance: Amount(500),
                    nonce: Nonce(3),
                }))
            } else {
                Err(StatusCode::NOT_FOUND)
            }
        }
        let app = Router::new()
            .route("/v1/blocks/head", get(head))
            .route("/v1/blocks", get(blocks))
            .with_state(Arc::new(chain))
            .route("/v1/accounts/{addr}", get(account).with_state(known));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    #[sqlx::test]
    async fn the_http_node_is_followed_over_several_pages(pool: sqlx::PgPool) {
        // 250 blocks need three requests of at most 100
        let chain = ChainBuilder::new(5).take_empty(250);
        let url = serve(chain.clone(), [9; 32]).await;
        let mut node = HttpNode::new(&url).unwrap();

        let head = node.head().await.unwrap();
        assert_eq!((head.height, head.hash), (249, chain[249].id.hash().0));
        assert_eq!(node.blocks(0, 1000).await.unwrap().len(), 100);

        let store = Store::new(pool);
        let cfg = SyncConfig {
            node: "http".parse().unwrap(),
            poll: Duration::from_millis(10),
        };
        sync_until_caught_up(&store, &mut node, &cfg, &Metrics::new())
            .await
            .unwrap();
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM blocks WHERE canonical")
            .fetch_one(store.pool())
            .await
            .unwrap();
        assert_eq!(n, 250);
    }

    #[tokio::test]
    async fn accounts_are_read_and_an_unknown_address_is_none() {
        let url = serve(ChainBuilder::new(5).take_empty(3), [9; 32]).await;
        let mut node = HttpNode::new(&url).unwrap();
        let a = node.account([9; 32]).await.unwrap().expect("known address");
        assert_eq!((a.balance.0, a.nonce.0), (500, 3));
        assert!(node.account([8; 32]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn many_accounts_are_read_at_once_and_come_back_in_order() {
        let url = serve(ChainBuilder::new(5).take_empty(3), [9; 32]).await;
        let mut node = HttpNode::new(&url).unwrap();
        // 40 addresses, more than the requests kept open at once; every fifth one is known
        let asked: Vec<Hash> = (0..40u8)
            .map(|i| if i % 5 == 0 { [9; 32] } else { [100 + i; 32] })
            .collect();
        let got = node.accounts(&asked).await.unwrap();
        assert_eq!(got.len(), 40);
        for (i, a) in got.iter().enumerate() {
            assert_eq!(a.is_some(), i % 5 == 0, "address number {i}");
        }
        assert!(node.accounts(&[]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_node_that_is_down_is_an_error_not_a_panic() {
        let dead = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            format!("http://{}", l.local_addr().unwrap())
        };
        assert!(matches!(
            HttpNode::new(&dead).unwrap().head().await,
            Err(NodeError::Request(_))
        ));
    }

    #[test]
    fn an_address_is_written_with_its_prefix_in_the_url() {
        assert!(Address([1; 32]).to_string().starts_with("addr:"));
    }
}
