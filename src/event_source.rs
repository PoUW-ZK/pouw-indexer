//! Where events come from: the node's `GET /v1/events` (contracts/node-events.md).
//! `EventSource` is the interface, `HttpEventSource` the real client and
//! `MemoryEventNode` a mock that behaves like the contract, for tests and local runs.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, anyhow};
use serde::{Deserialize, Serialize};

/// One event, in the envelope every node uses.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Event {
    pub seq: u64,
    /// RFC 3339 UTC, by the clock of the node that wrote it.
    pub time: String,
    pub node_id: String,
    pub kind: String,
    pub data: serde_json::Value,
}

/// A response of `GET /v1/events`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventPage {
    pub events: Vec<Event>,
    pub next: u64,
}

#[derive(Debug)]
pub enum FetchError {
    /// HTTP 410: `after` is older than the oldest event the node still keeps.
    CursorExpired { oldest_seq: u64 },
    /// Anything else: connection refused, timeout, a 5xx, a body we cannot read.
    Other(anyhow::Error),
}

pub trait EventSource: Send + Sync {
    /// Events with `seq > after`, at most `limit`. With `wait_ms > 0` the node may hold the
    /// request open until something new happens (long poll).
    fn fetch(
        &self,
        after: u64,
        limit: u32,
        wait_ms: u32,
    ) -> impl Future<Output = Result<EventPage, FetchError>> + Send;
}

pub fn read_events(path: &str) -> anyhow::Result<Vec<Event>> {
    let text = std::fs::read_to_string(path).with_context(|| format!("cannot read {path}"))?;
    text.lines()
        .enumerate()
        .filter(|(_, l)| !l.trim().is_empty())
        .map(|(i, l)| {
            serde_json::from_str(l).with_context(|| format!("{path} line {}: not an event", i + 1))
        })
        .collect()
}

// ------------------------------------------------------------------------------ real client

pub struct HttpEventSource {
    client: reqwest::Client,
    url: String,
}

impl HttpEventSource {
    pub fn new(base_url: &str) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .build()?;
        Ok(Self {
            client,
            url: format!("{}/v1/events", base_url.trim_end_matches('/')),
        })
    }
}

impl EventSource for HttpEventSource {
    async fn fetch(&self, after: u64, limit: u32, wait_ms: u32) -> Result<EventPage, FetchError> {
        let other = FetchError::Other;
        let resp = self
            .client
            .get(&self.url)
            .query(&[
                ("after", after),
                ("limit", limit as u64),
                ("wait_ms", wait_ms as u64),
            ])
            // the node may legitimately hold the request for wait_ms
            .timeout(Duration::from_millis(wait_ms as u64 + 15_000))
            .send()
            .await
            .map_err(|e| other(anyhow!(e).context("request to the node failed")))?;
        let status = resp.status();
        if status.as_u16() == 410 {
            let body: serde_json::Value = resp.json().await.map_err(|e| other(anyhow!(e)))?;
            let oldest = body["error"]["details"]["oldest_seq"]
                .as_u64()
                .ok_or_else(|| {
                    other(anyhow!("HTTP 410 without error.details.oldest_seq: {body}"))
                })?;
            return Err(FetchError::CursorExpired { oldest_seq: oldest });
        }
        if !status.is_success() {
            return Err(other(anyhow!("the node answered HTTP {status}")));
        }
        resp.json::<EventPage>()
            .await
            .map_err(|e| other(anyhow!(e).context("the node's answer is not an event page")))
    }
}

// ------------------------------------------------------------------------------ mock

struct Inner {
    events: Vec<Event>,
    visible: usize,
    retention: Option<usize>,
    fail_next: u32,
}

/// A node's event log in memory, with the semantics of node-events.md:
/// `seq` is the position (1, 2, 3, ...), `after` and `limit` select a window, `next` is the
/// last seq returned (or `after` when empty), and an `after` older than the retained part
/// gives `CursorExpired`. It does not really wait for `wait_ms`: an empty answer comes back
/// at once, as if the wait had run out.
///
/// For tests: `set_visible` makes the log grow, `with_retention` drops old events from view,
/// `fail_next(n)` makes the next n requests fail like a dropped connection.
#[derive(Clone)]
pub struct MemoryEventNode {
    inner: Arc<Mutex<Inner>>,
}

// Most of this is used by tests and by local runs with EVENT_FIXTURES only.
#[allow(dead_code)]
impl MemoryEventNode {
    pub fn new(events: Vec<Event>) -> Self {
        let visible = events.len();
        Self {
            inner: Arc::new(Mutex::new(Inner {
                events,
                visible,
                retention: None,
                fail_next: 0,
            })),
        }
    }

    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        Ok(Self::new(read_events(path)?))
    }

    pub fn set_visible(&self, n: usize) {
        let mut i = self.inner.lock().expect("lock");
        i.visible = n.min(i.events.len());
    }

    pub fn with_retention(self, keep_last: usize) -> Self {
        self.inner.lock().expect("lock").retention = Some(keep_last);
        self
    }

    pub fn fail_next(&self, n: u32) {
        self.inner.lock().expect("lock").fail_next = n;
    }
}

impl EventSource for MemoryEventNode {
    async fn fetch(&self, after: u64, limit: u32, _wait_ms: u32) -> Result<EventPage, FetchError> {
        let mut i = self.inner.lock().expect("lock");
        if i.fail_next > 0 {
            i.fail_next -= 1;
            return Err(FetchError::Other(anyhow!(
                "connection reset by peer (injected by the test)"
            )));
        }
        let limit = limit.clamp(1, 1000) as usize; // the contract's maximum is 1000
        let oldest = match i.retention {
            Some(keep) => (i.visible.saturating_sub(keep) + 1) as u64,
            None => 1,
        };
        if after + 1 < oldest {
            return Err(FetchError::CursorExpired { oldest_seq: oldest });
        }
        let start = (after as usize).min(i.visible);
        let end = (start + limit).min(i.visible);
        let events: Vec<Event> = i.events[start..end].to_vec();
        let next = events.last().map_or(after, |e| e.seq);
        Ok(EventPage { events, next })
    }
}
