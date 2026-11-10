//! Store the events a node tells us about (contracts/node-events.md).
//!
//! Rules this module keeps:
//!  * A page of events, the typed rows made from them and the new cursor are written in ONE
//!    transaction. A crash leaves either all of it or none, so a restart neither repeats an
//!    event nor skips one.
//!  * Every event is also kept as received in `node_events`, keyed by (node_id, seq). Replaying a
//!    page is harmless, and an event we do not understand is not lost.
//!  * The node promises seq 1, 2, 3 with no gaps. If a page breaks that, we stop with an
//!    error instead of storing something wrong.
//!  * A node that dropped old events (HTTP 410) costs us observations, never blocks: the lost
//!    range is written to `event_gaps` and we continue from the oldest event it still has.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow, bail};
use serde::Deserialize;
use sqlx::{PgConnection, PgPool};

use crate::event_source::{Event, EventSource, FetchError};
use crate::metrics::Metrics;

pub struct EventConfig {
    /// The `node_id` this source must report. It is also the key of the cursor.
    pub node_id: String,
    /// Events asked per request (the node's maximum is 1000).
    pub page: u32,
    /// How long a long-poll request may be held by the node.
    pub long_poll_ms: u32,
    /// Pause between requests when there was nothing new, and in polling mode.
    pub poll_interval: Duration,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    /// Consecutive failures before we stop long-polling and poll with short requests.
    pub fallback_after: u32,
    /// Consecutive good polls before we try long-polling again.
    pub recover_after: u32,
}

impl EventConfig {
    pub fn new(node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            page: 1000,
            long_poll_ms: 20_000,
            poll_interval: Duration::from_secs(2),
            backoff_base: Duration::from_secs(1),
            backoff_max: Duration::from_secs(30),
            fallback_after: 3,
            recover_after: 10,
        }
    }
}

// ------------------------------------------------------------------------------ parsing

fn hex_id(s: &str, prefix: &str) -> anyhow::Result<[u8; 32]> {
    let h = s
        .strip_prefix(prefix)
        .with_context(|| format!("{s:?} does not start with {prefix:?}"))?;
    let v = hex::decode(h).with_context(|| format!("{s:?} is not hex"))?;
    v.try_into().map_err(|_| anyhow!("{s:?} is not 32 bytes"))
}

/// A field element: "0x" and 62 hex digits (31 bytes).
fn field_element(s: &str) -> anyhow::Result<[u8; 31]> {
    let h = s
        .strip_prefix("0x")
        .with_context(|| format!("{s:?} does not start with 0x"))?;
    let v = hex::decode(h).with_context(|| format!("{s:?} is not hex"))?;
    v.try_into().map_err(|_| anyhow!("{s:?} is not 31 bytes"))
}

fn int(v: u64) -> anyhow::Result<i64> {
    i64::try_from(v).context("number does not fit in a Postgres BIGINT")
}

#[derive(Deserialize)]
struct TxReceived {
    txid: String,
    tx_kind: String,
    sender: String,
    fee: u64,
}
#[derive(Deserialize)]
struct TxRejected {
    txid: Option<String>,
    reason_code: String,
    message: String,
}
#[derive(Deserialize)]
struct ProofProduced {
    txid: String,
    circuit: String,
    system: String,
    prove_ms: u64,
    proof_hash: String,
}
#[derive(Deserialize)]
struct LotteryResult {
    txid: String,
    proof_hash: String,
    circuit: String,
    won: bool,
    eta: String,
    mask: String,
    usage_counter: u64,
}
#[derive(Deserialize)]
struct BlockReceived {
    #[serde(alias = "id")]
    hash: String,
    height: u64,
    from_peer: String,
}
#[derive(Deserialize)]
struct BlockVerified {
    #[serde(alias = "id")]
    hash: String,
    height: u64,
    verify_ms: u64,
    proof_count: u64,
    ok: bool,
}
#[derive(Deserialize)]
struct BlockAccepted {
    #[serde(alias = "id")]
    hash: String,
    height: u64,
}
#[derive(Deserialize)]
struct BlockRejected {
    #[serde(alias = "id")]
    hash: String,
    height: Option<u64>,
    reason_code: String,
}
#[derive(Deserialize)]
struct BlockOrphaned {
    #[serde(alias = "id")]
    hash: String,
    height: u64,
}
#[derive(Deserialize)]
struct MempoolSnapshot {
    pending_txs: u64,
    pending_proof_requests: u64,
    by_circuit: serde_json::Value,
}

/// An event turned into database-ready values. Building it checks everything (id lengths,
/// numbers that fit), so a bad event is caught here and never fails the SQL.
enum Parsed {
    TxReceived {
        txid: [u8; 32],
        kind: String,
        sender: [u8; 32],
        fee: i64,
    },
    TxRejected {
        txid: Option<[u8; 32]>,
        reason: String,
        message: String,
    },
    ProofProduced {
        txid_text: String,
        txid: [u8; 32],
        circuit: [u8; 32],
        system: String,
        prove_ms: i64,
        proof_hash: [u8; 32],
    },
    Lottery {
        txid: [u8; 32],
        proof_hash: [u8; 32],
        circuit: [u8; 32],
        won: bool,
        eta: [u8; 31],
        mask: [u8; 31],
        counter: i64,
    },
    BlockReceived {
        hash: [u8; 32],
        height: i64,
        from_peer: String,
    },
    BlockVerified {
        hash: [u8; 32],
        height: i64,
        verify_ms: i64,
        proofs: i64,
        ok: bool,
    },
    BlockAccepted {
        hash: [u8; 32],
        height: i64,
    },
    BlockRejected {
        hash: [u8; 32],
        height: Option<i64>,
        reason: String,
    },
    BlockOrphaned {
        hash: [u8; 32],
        height: i64,
    },
    Snapshot {
        pending_txs: i64,
        pending_requests: i64,
        by_circuit: serde_json::Value,
    },
    /// Kept only in node_events: proof_request_started, proof_verified, peer_*, node_started,
    /// and any kind this version does not know.
    RawOnly,
}

fn parse(ev: &Event) -> anyhow::Result<Parsed> {
    let d = ev.data.clone();
    Ok(match ev.kind.as_str() {
        "tx_received" => {
            let t: TxReceived = serde_json::from_value(d)?;
            if !matches!(
                t.tx_kind.as_str(),
                "transfer" | "proof_request" | "register_circuit"
            ) {
                bail!("unknown tx_kind {:?}", t.tx_kind);
            }
            Parsed::TxReceived {
                txid: hex_id(&t.txid, "tx:")?,
                kind: t.tx_kind,
                sender: hex_id(&t.sender, "addr:")?,
                fee: int(t.fee)?,
            }
        }
        "tx_rejected" => {
            let t: TxRejected = serde_json::from_value(d)?;
            let txid = t.txid.as_deref().map(|s| hex_id(s, "tx:")).transpose()?;
            Parsed::TxRejected {
                txid,
                reason: t.reason_code,
                message: t.message,
            }
        }
        "proof_produced" => {
            let t: ProofProduced = serde_json::from_value(d)?;
            Parsed::ProofProduced {
                txid: hex_id(&t.txid, "tx:")?,
                txid_text: t.txid,
                circuit: hex_id(&t.circuit, "circ:")?,
                system: t.system,
                prove_ms: int(t.prove_ms)?,
                proof_hash: hex_id(&t.proof_hash, "proof:")?,
            }
        }
        "lottery_result" => {
            let t: LotteryResult = serde_json::from_value(d)?;
            Parsed::Lottery {
                txid: hex_id(&t.txid, "tx:")?,
                proof_hash: hex_id(&t.proof_hash, "proof:")?,
                circuit: hex_id(&t.circuit, "circ:")?,
                won: t.won,
                eta: field_element(&t.eta)?,
                mask: field_element(&t.mask)?,
                counter: int(t.usage_counter)?,
            }
        }
        "block_received" => {
            let t: BlockReceived = serde_json::from_value(d)?;
            Parsed::BlockReceived {
                hash: hex_id(&t.hash, "blk:")?,
                height: int(t.height)?,
                from_peer: t.from_peer,
            }
        }
        "block_verified" => {
            let t: BlockVerified = serde_json::from_value(d)?;
            Parsed::BlockVerified {
                hash: hex_id(&t.hash, "blk:")?,
                height: int(t.height)?,
                verify_ms: int(t.verify_ms)?,
                proofs: int(t.proof_count)?,
                ok: t.ok,
            }
        }
        "block_accepted" => {
            let t: BlockAccepted = serde_json::from_value(d)?;
            Parsed::BlockAccepted {
                hash: hex_id(&t.hash, "blk:")?,
                height: int(t.height)?,
            }
        }
        "block_rejected" => {
            let t: BlockRejected = serde_json::from_value(d)?;
            Parsed::BlockRejected {
                hash: hex_id(&t.hash, "blk:")?,
                height: t.height.map(int).transpose()?,
                reason: t.reason_code,
            }
        }
        "block_orphaned" => {
            let t: BlockOrphaned = serde_json::from_value(d)?;
            Parsed::BlockOrphaned {
                hash: hex_id(&t.hash, "blk:")?,
                height: int(t.height)?,
            }
        }
        "mempool_snapshot" => {
            let t: MempoolSnapshot = serde_json::from_value(d)?;
            Parsed::Snapshot {
                pending_txs: int(t.pending_txs)?,
                pending_requests: int(t.pending_proof_requests)?,
                by_circuit: t.by_circuit,
            }
        }
        _ => Parsed::RawOnly,
    })
}

// ------------------------------------------------------------------------------ writing

async fn apply(conn: &mut PgConnection, node: &str, ev: &Event, p: &Parsed) -> sqlx::Result<()> {
    let t = ev.time.as_str();
    match p {
        Parsed::TxReceived {
            txid,
            kind,
            sender,
            fee,
        } => {
            sqlx::query(
                "INSERT INTO tx_observations (node_id, txid, tx_kind, sender, fee, received_at) \
                 VALUES ($1, $2, $3, $4, $5, $6::timestamptz) ON CONFLICT (node_id, txid) DO NOTHING",
            )
            .bind(node).bind(&txid[..]).bind(kind).bind(&sender[..]).bind(fee).bind(t)
            .execute(&mut *conn).await?;
        }
        Parsed::TxRejected {
            txid,
            reason,
            message,
        } => {
            sqlx::query(
                "INSERT INTO tx_rejections (node_id, seq, txid, reason_code, message, rejected_at) \
                 VALUES ($1, $2, $3, $4, $5, $6::timestamptz) ON CONFLICT (node_id, seq) DO NOTHING",
            )
            .bind(node).bind(ev.seq as i64).bind(txid.as_ref().map(|x| x.to_vec())).bind(reason).bind(message).bind(t)
            .execute(&mut *conn).await?;
        }
        Parsed::ProofProduced {
            txid_text,
            txid,
            circuit,
            system,
            prove_ms,
            proof_hash,
        } => {
            // started_at: the proof_request_started this node logged earlier for the same request
            sqlx::query(
                "INSERT INTO proof_attempts (node_id, proof_hash, request_txid, circuit_id, proof_system, \
                     started_at, produced_at, prove_ms) \
                 VALUES ($1, $2, $3, $4, $5, \
                     (SELECT e.time FROM node_events e WHERE e.node_id = $1 AND e.kind = 'proof_request_started' \
                        AND e.data ->> 'txid' = $8 AND e.seq < $9 ORDER BY e.seq DESC LIMIT 1), \
                     $6::timestamptz, $7) \
                 ON CONFLICT (node_id, proof_hash) DO UPDATE SET request_txid = EXCLUDED.request_txid, \
                     circuit_id = EXCLUDED.circuit_id, proof_system = EXCLUDED.proof_system, \
                     started_at = COALESCE(proof_attempts.started_at, EXCLUDED.started_at), \
                     produced_at = EXCLUDED.produced_at, prove_ms = EXCLUDED.prove_ms",
            )
            .bind(node).bind(&proof_hash[..]).bind(&txid[..]).bind(&circuit[..]).bind(system)
            .bind(t).bind(prove_ms).bind(txid_text).bind(ev.seq as i64)
            .execute(&mut *conn).await?;
        }
        Parsed::Lottery {
            txid,
            proof_hash,
            circuit,
            won,
            eta,
            mask,
            counter,
        } => {
            sqlx::query(
                "INSERT INTO proof_attempts (node_id, proof_hash, request_txid, circuit_id, won, eta, mask, usage_counter) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
                 ON CONFLICT (node_id, proof_hash) DO UPDATE SET won = EXCLUDED.won, eta = EXCLUDED.eta, \
                     mask = EXCLUDED.mask, usage_counter = EXCLUDED.usage_counter",
            )
            .bind(node).bind(&proof_hash[..]).bind(&txid[..]).bind(&circuit[..]).bind(won)
            .bind(&eta[..]).bind(&mask[..]).bind(counter)
            .execute(&mut *conn).await?;
        }
        Parsed::BlockReceived {
            hash,
            height,
            from_peer,
        } => {
            sqlx::query(
                "INSERT INTO block_observations (node_id, block_hash, height, received_at, from_peer) \
                 VALUES ($1, $2, $3, $4::timestamptz, $5) \
                 ON CONFLICT (node_id, block_hash) DO UPDATE SET height = EXCLUDED.height, \
                     received_at = COALESCE(block_observations.received_at, EXCLUDED.received_at), \
                     from_peer = COALESCE(block_observations.from_peer, EXCLUDED.from_peer)",
            )
            .bind(node).bind(&hash[..]).bind(height).bind(t).bind(from_peer)
            .execute(&mut *conn).await?;
        }
        Parsed::BlockVerified {
            hash,
            height,
            verify_ms,
            proofs,
            ok,
        } => {
            sqlx::query(
                "INSERT INTO block_observations (node_id, block_hash, height, verified_at, verify_ms, verified_ok, verified_proofs) \
                 VALUES ($1, $2, $3, $4::timestamptz, $5, $6, $7) \
                 ON CONFLICT (node_id, block_hash) DO UPDATE SET height = EXCLUDED.height, \
                     verified_at = EXCLUDED.verified_at, verify_ms = EXCLUDED.verify_ms, \
                     verified_ok = EXCLUDED.verified_ok, verified_proofs = EXCLUDED.verified_proofs",
            )
            .bind(node).bind(&hash[..]).bind(height).bind(t).bind(verify_ms).bind(ok).bind(proofs)
            .execute(&mut *conn).await?;
        }
        Parsed::BlockAccepted { hash, height } => {
            // accepted again after an orphaning: the block is part of the chain once more
            sqlx::query(
                "INSERT INTO block_observations (node_id, block_hash, height, accepted_at) \
                 VALUES ($1, $2, $3, $4::timestamptz) \
                 ON CONFLICT (node_id, block_hash) DO UPDATE SET height = EXCLUDED.height, \
                     accepted_at = EXCLUDED.accepted_at, orphaned_at = NULL",
            )
            .bind(node)
            .bind(&hash[..])
            .bind(height)
            .bind(t)
            .execute(&mut *conn)
            .await?;
        }
        Parsed::BlockRejected {
            hash,
            height,
            reason,
        } => {
            sqlx::query(
                "INSERT INTO block_observations (node_id, block_hash, height, rejected_at, reject_reason) \
                 VALUES ($1, $2, $3, $4::timestamptz, $5) \
                 ON CONFLICT (node_id, block_hash) DO UPDATE SET \
                     height = COALESCE(EXCLUDED.height, block_observations.height), \
                     rejected_at = EXCLUDED.rejected_at, reject_reason = EXCLUDED.reject_reason",
            )
            .bind(node).bind(&hash[..]).bind(height).bind(t).bind(reason)
            .execute(&mut *conn).await?;
        }
        Parsed::BlockOrphaned { hash, height } => {
            sqlx::query(
                "INSERT INTO block_observations (node_id, block_hash, height, orphaned_at) \
                 VALUES ($1, $2, $3, $4::timestamptz) \
                 ON CONFLICT (node_id, block_hash) DO UPDATE SET height = EXCLUDED.height, orphaned_at = EXCLUDED.orphaned_at",
            )
            .bind(node).bind(&hash[..]).bind(height).bind(t)
            .execute(&mut *conn).await?;
        }
        Parsed::Snapshot {
            pending_txs,
            pending_requests,
            by_circuit,
        } => {
            sqlx::query(
                "INSERT INTO mempool_snapshots (node_id, observed_at, pending_txs, pending_proof_requests, by_circuit) \
                 VALUES ($1, $2::timestamptz, $3, $4, $5) ON CONFLICT DO NOTHING",
            )
            .bind(node).bind(t).bind(pending_txs).bind(pending_requests).bind(by_circuit)
            .execute(&mut *conn).await?;
        }
        Parsed::RawOnly => {}
    }
    Ok(())
}

async fn set_cursor(conn: &mut PgConnection, node: &str, seq: u64) -> sqlx::Result<()> {
    // GREATEST: the cursor never moves back, even if a page is replayed
    sqlx::query(
        "INSERT INTO ingest_cursors (node_id, last_seq) VALUES ($1, $2) \
         ON CONFLICT (node_id) DO UPDATE SET last_seq = GREATEST(ingest_cursors.last_seq, EXCLUDED.last_seq)",
    )
    .bind(node)
    .bind(seq as i64)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub async fn load_cursor(pool: &PgPool, node: &str) -> anyhow::Result<u64> {
    let v: Option<i64> =
        sqlx::query_scalar("SELECT last_seq FROM ingest_cursors WHERE node_id = $1")
            .bind(node)
            .fetch_optional(pool)
            .await?;
    Ok(v.unwrap_or(0) as u64)
}

#[derive(Debug, PartialEq, Eq)]
pub struct PageStats {
    pub stored: usize,
    /// Events of a known kind whose content did not make sense. Kept raw, no typed row.
    pub bad: usize,
}

/// Store a page that must continue right after `after`: seq after+1, after+2, ...
/// Safe to call again with the same events.
pub async fn store_page(
    pool: &PgPool,
    node: &str,
    after: u64,
    events: &[Event],
) -> anyhow::Result<PageStats> {
    for (i, e) in events.iter().enumerate() {
        let want = after + 1 + i as u64;
        if e.seq != want {
            bail!(
                "{node}: expected event {want} but got {} (a node must not skip or repeat)",
                e.seq
            );
        }
        if e.node_id != node {
            bail!(
                "{node}: got an event of node {:?}, is the source address right?",
                e.node_id
            );
        }
        if e.time.len() < 20 || !e.time.ends_with('Z') || !e.time.contains('T') {
            bail!(
                "{node}: event {} has a time that is not RFC 3339 UTC: {:?}",
                e.seq,
                e.time
            );
        }
    }
    let Some(last) = events.last() else {
        return Ok(PageStats { stored: 0, bad: 0 });
    };

    let mut tx = pool.begin().await?;
    // 1. the events as received, many rows in one statement
    let seqs: Vec<i64> = events.iter().map(|e| e.seq as i64).collect();
    let times: Vec<&str> = events.iter().map(|e| e.time.as_str()).collect();
    let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
    let data: Vec<String> = events.iter().map(|e| e.data.to_string()).collect();
    sqlx::query(
        "INSERT INTO node_events (node_id, seq, time, kind, data) \
         SELECT $1, u.s, u.t::timestamptz, u.k, u.d::jsonb \
         FROM UNNEST($2::bigint[], $3::text[], $4::text[], $5::text[]) AS u(s, t, k, d) \
         ON CONFLICT (node_id, seq) DO NOTHING",
    )
    .bind(node)
    .bind(&seqs)
    .bind(&times)
    .bind(&kinds)
    .bind(&data)
    .execute(&mut *tx)
    .await?;

    // 2. the typed rows
    let mut bad = 0;
    for e in events {
        match parse(e) {
            Ok(p) => apply(&mut tx, node, e, &p).await?,
            Err(err) => {
                bad += 1;
                eprintln!(
                    "events: {node} seq {} ({}): kept raw, not understood: {err:#}",
                    e.seq, e.kind
                );
            }
        }
    }

    // 3. the cursor, in the same transaction
    set_cursor(&mut tx, node, last.seq).await?;
    tx.commit().await?;
    Ok(PageStats {
        stored: events.len(),
        bad,
    })
}

// ------------------------------------------------------------------------------ one request

#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    /// A page was stored.
    Stored(usize),
    /// Nothing new.
    Empty,
    /// The node had dropped events from..=to before we asked. Recorded, cursor moved on.
    Gap { from: u64, to: u64 },
}

/// Ask once (after the cursor) and store what comes back.
pub async fn ingest_step<S: EventSource>(
    pool: &PgPool,
    src: &S,
    cfg: &EventConfig,
    m: &Metrics,
    wait_ms: u32,
) -> anyhow::Result<Step> {
    let cursor = load_cursor(pool, &cfg.node_id).await?;
    m.event_cursor
        .with_label_values(&[&cfg.node_id])
        .set(cursor as i64);
    match src.fetch(cursor, cfg.page, wait_ms).await {
        Ok(page) if page.events.is_empty() => Ok(Step::Empty),
        Ok(page) => {
            let stats = store_page(pool, &cfg.node_id, cursor, &page.events).await?;
            m.events_ingested.inc_by(stats.stored as u64);
            m.events_unparsed.inc_by(stats.bad as u64);
            m.event_cursor
                .with_label_values(&[&cfg.node_id])
                .set((cursor + stats.stored as u64) as i64);
            Ok(Step::Stored(stats.stored))
        }
        Err(FetchError::CursorExpired { oldest_seq }) => {
            // The spec: blocks cover what blocks can say; the observations in the dropped
            // window are lost. We write the loss down and continue from the oldest event.
            let from = cursor + 1;
            let to = oldest_seq.saturating_sub(1);
            if to < from {
                bail!("the node says cursor {cursor} expired but its oldest event is {oldest_seq}");
            }
            let mut tx = pool.begin().await?;
            sqlx::query("INSERT INTO event_gaps (node_id, from_seq, to_seq) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
                .bind(&cfg.node_id)
                .bind(from as i64)
                .bind(to as i64)
                .execute(&mut *tx)
                .await?;
            set_cursor(&mut tx, &cfg.node_id, to).await?;
            tx.commit().await?;
            m.event_gaps.inc();
            Ok(Step::Gap { from, to })
        }
        Err(FetchError::Other(e)) => Err(e),
    }
}

/// Keep asking until the node has nothing new.
#[allow(dead_code)]
pub async fn ingest_until_caught_up<S: EventSource>(
    pool: &PgPool,
    src: &S,
    cfg: &EventConfig,
    m: &Metrics,
) -> anyhow::Result<()> {
    for _ in 0..100_000 {
        if ingest_step(pool, src, cfg, m, 0).await? == Step::Empty {
            return Ok(());
        }
    }
    bail!("event ingestion did not settle")
}

// ------------------------------------------------------------------------------ the loop

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// Requests the node may hold open until something happens.
    LongPoll,
    /// Short requests every `poll_interval`. Used while long requests keep failing
    /// (a proxy that cuts them, for example), until short ones work reliably again.
    Poll,
}

/// The background task of `serve` for one node. Never returns. A failure (node down,
/// connection dropped, bad answer) is counted and retried after a growing pause; after
/// `fallback_after` failures in a row it switches to plain polling, and goes back to long
/// polling after `recover_after` good polls. The cursor lives in the database, so every
/// retry continues exactly where the last stored page ended.
pub async fn run<S: EventSource>(pool: PgPool, src: S, cfg: EventConfig, m: Arc<Metrics>) {
    let mut mode = Mode::LongPoll;
    let (mut failures, mut good_polls) = (0u32, 0u32);
    loop {
        let started = Instant::now();
        let wait_ms = if mode == Mode::LongPoll {
            cfg.long_poll_ms
        } else {
            0
        };
        match ingest_step(&pool, &src, &cfg, &m, wait_ms).await {
            Ok(step) => {
                failures = 0;
                if let Step::Gap { from, to } = step {
                    eprintln!(
                        "events: {}: the node no longer had events {from}..={to}, recorded as a gap",
                        cfg.node_id
                    );
                }
                if mode == Mode::Poll {
                    good_polls += 1;
                    if good_polls >= cfg.recover_after {
                        println!(
                            "events: {}: connection is stable, back to long polling",
                            cfg.node_id
                        );
                        mode = Mode::LongPoll;
                        good_polls = 0;
                        m.event_polling_mode
                            .with_label_values(&[&cfg.node_id])
                            .set(0);
                    }
                }
                if step == Step::Empty {
                    // never spin: wait out the rest of the interval (a long poll already took it)
                    if let Some(rest) = cfg.poll_interval.checked_sub(started.elapsed()) {
                        tokio::time::sleep(rest).await;
                    }
                }
            }
            Err(e) => {
                m.event_source_errors.inc();
                failures += 1;
                good_polls = 0;
                eprintln!("events: {} (failure {failures}): {e:#}", cfg.node_id);
                if failures >= cfg.fallback_after && mode == Mode::LongPoll {
                    println!(
                        "events: {}: {failures} failures in a row, switching to plain polling",
                        cfg.node_id
                    );
                    mode = Mode::Poll;
                    m.event_polling_mode
                        .with_label_values(&[&cfg.node_id])
                        .set(1);
                }
                let pause = cfg
                    .backoff_base
                    .saturating_mul(1 << failures.min(6))
                    .min(cfg.backoff_max);
                tokio::time::sleep(pause).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use crate::event_source::{EventSource, HttpEventSource, MemoryEventNode, read_events};

    /// A recorded stream of the miner (it has proofs, lottery results and block events).
    const MINER_1: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/mock_data/pouw-fixtures/events/miner-1.jsonl"
    );

    fn fast_cfg(node: &str) -> EventConfig {
        EventConfig {
            poll_interval: Duration::from_millis(5),
            backoff_base: Duration::from_millis(2),
            backoff_max: Duration::from_millis(20),
            ..EventConfig::new(node)
        }
    }

    async fn count(pool: &PgPool, sql: &str) -> i64 {
        sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
    }

    fn kind_count(events: &[Event], kind: &str) -> i64 {
        events.iter().filter(|e| e.kind == kind).count() as i64
    }

    /// seq 1..=n, each exactly once
    async fn assert_no_gaps_no_duplicates(pool: &PgPool, node: &str, n: i64) {
        let row: (i64, i64, i64, i64) = sqlx::query_as(
            "SELECT count(*), count(DISTINCT seq), min(seq), max(seq) FROM node_events WHERE node_id = $1",
        )
        .bind(node)
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(row, (n, n, 1, n));
        assert_eq!(load_cursor(pool, node).await.unwrap() as i64, n);
    }

    #[sqlx::test]
    async fn the_recorded_stream_is_stored_in_every_table(pool: PgPool) {
        let events = read_events(MINER_1).unwrap();
        let n = events.len() as i64;
        let node = MemoryEventNode::new(events.clone());
        let m = Metrics::new();
        ingest_until_caught_up(&pool, &node, &fast_cfg("miner-1"), &m)
            .await
            .unwrap();

        assert_no_gaps_no_duplicates(&pool, "miner-1", n).await;
        assert_eq!(
            count(&pool, "SELECT count(*) FROM tx_observations").await,
            kind_count(&events, "tx_received")
        );
        assert_eq!(
            count(&pool, "SELECT count(*) FROM tx_rejections").await,
            kind_count(&events, "tx_rejected")
        );
        assert_eq!(
            count(&pool, "SELECT count(*) FROM mempool_snapshots").await,
            kind_count(&events, "mempool_snapshot")
        );
        assert_eq!(
            count(&pool, "SELECT count(*) FROM proof_attempts").await,
            kind_count(&events, "proof_produced")
        );
        // losing proofs are the wasted work
        let lost = events
            .iter()
            .filter(|e| e.kind == "lottery_result" && e.data["won"] == false)
            .count() as i64;
        assert!(lost > 0, "the fixture should contain losing proofs");
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM proof_attempts WHERE won = false"
            )
            .await,
            lost
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM proof_attempts WHERE started_at IS NULL OR won IS NULL"
            )
            .await,
            0
        );
        let blocks: HashSet<&str> = events
            .iter()
            .filter(|e| e.kind.starts_with("block_"))
            .map(|e| e.data["hash"].as_str().unwrap())
            .collect();
        assert_eq!(
            count(&pool, "SELECT count(*) FROM block_observations").await,
            blocks.len() as i64
        );
        assert_eq!(
            count(
                &pool,
                "SELECT count(*) FROM block_observations WHERE orphaned_at IS NOT NULL"
            )
            .await,
            kind_count(&events, "block_orphaned")
        );
        assert!(
            m.render()
                .contains(&format!("pouw_indexer_events_ingested_total {n}"))
        );
    }

    #[sqlx::test]
    async fn restart_and_replay_change_nothing(pool: PgPool) {
        let events = read_events(MINER_1).unwrap();
        let n = events.len();
        let node = MemoryEventNode::new(events.clone());
        let m = Metrics::new();
        let cfg = fast_cfg("miner-1");

        // the node has only the first part when we start ...
        node.set_visible(200);
        ingest_until_caught_up(&pool, &node, &cfg, &m)
            .await
            .unwrap();
        assert_no_gaps_no_duplicates(&pool, "miner-1", 200).await;
        // ... the indexer "restarts" (state is only in the database) and the node has grown
        node.set_visible(n);
        ingest_until_caught_up(&pool, &node, &cfg, &m)
            .await
            .unwrap();
        ingest_until_caught_up(&pool, &node, &cfg, &m)
            .await
            .unwrap();
        assert_no_gaps_no_duplicates(&pool, "miner-1", n as i64).await;
        let attempts = count(&pool, "SELECT count(*) FROM proof_attempts").await;

        // the same page delivered again (a retry after a lost answer) is harmless
        store_page(&pool, "miner-1", 0, &events[..50])
            .await
            .unwrap();
        store_page(&pool, "miner-1", 100, &events[100..160])
            .await
            .unwrap();
        assert_no_gaps_no_duplicates(&pool, "miner-1", n as i64).await;
        assert_eq!(
            count(&pool, "SELECT count(*) FROM proof_attempts").await,
            attempts
        );
    }

    #[sqlx::test]
    async fn a_page_that_skips_an_event_is_refused(pool: PgPool) {
        let events = read_events(MINER_1).unwrap();
        let mut holey = events[..10].to_vec();
        holey.remove(4);
        assert!(store_page(&pool, "miner-1", 0, &holey).await.is_err());
        // an event of another node is refused too (wrong address configured)
        assert!(store_page(&pool, "node-a", 0, &events[..5]).await.is_err());
        // nothing was written
        assert_eq!(count(&pool, "SELECT count(*) FROM node_events").await, 0);
        assert_eq!(load_cursor(&pool, "miner-1").await.unwrap(), 0);
    }

    #[sqlx::test]
    async fn odd_events_are_kept_raw_and_do_not_stop_ingestion(pool: PgPool) {
        let ev = |seq: u64, kind: &str, data: serde_json::Value| Event {
            seq,
            time: "2026-10-19T14:00:00.000Z".into(),
            node_id: "n".into(),
            kind: kind.into(),
            data,
        };
        let events = vec![
            ev(1, "something_from_the_future", serde_json::json!({"x": 1})),
            ev(
                2,
                "tx_received",
                serde_json::json!({"txid": "tx:zz", "tx_kind": "transfer", "sender": "addr:00", "fee": 1}),
            ),
            ev(3, "peer_connected", serde_json::json!({"peer_id": "p"})),
        ];
        let stats = store_page(&pool, "n", 0, &events).await.unwrap();
        assert_eq!(stats, PageStats { stored: 3, bad: 1 });
        assert_eq!(count(&pool, "SELECT count(*) FROM node_events").await, 3);
        assert_eq!(
            count(&pool, "SELECT count(*) FROM tx_observations").await,
            0
        );
        assert_eq!(load_cursor(&pool, "n").await.unwrap(), 3);
    }

    #[sqlx::test]
    async fn events_the_node_dropped_are_recorded_as_a_gap(pool: PgPool) {
        let events = read_events(MINER_1).unwrap();
        let n = events.len() as u64;
        let node = MemoryEventNode::new(events).with_retention(100);
        let m = Metrics::new();
        ingest_until_caught_up(&pool, &node, &fast_cfg("miner-1"), &m)
            .await
            .unwrap();

        let oldest = n - 99;
        let gap: (i64, i64) =
            sqlx::query_as("SELECT from_seq, to_seq FROM event_gaps WHERE node_id = 'miner-1'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(gap, (1, oldest as i64 - 1));
        assert_eq!(count(&pool, "SELECT count(*) FROM node_events").await, 100);
        assert_eq!(
            count(&pool, "SELECT min(seq) FROM node_events").await,
            oldest as i64
        );
        assert_eq!(load_cursor(&pool, "miner-1").await.unwrap(), n);
        assert!(m.render().contains("pouw_indexer_event_gaps_total 1"));
    }

    #[sqlx::test]
    async fn a_dropped_connection_is_retried_and_falls_back_to_polling(pool: PgPool) {
        let events = read_events(MINER_1).unwrap();
        let n = events.len() as i64;
        let node = MemoryEventNode::new(events);
        node.fail_next(4); // four failed requests in a row, then the connection works again
        let m = Arc::new(Metrics::new());
        // never go back to long polling in this test, so the mode can be checked at the end
        let cfg = EventConfig {
            recover_after: 1_000_000,
            ..fast_cfg("miner-1")
        };

        let task = tokio::spawn(run(pool.clone(), node, cfg, m.clone()));
        let deadline = Instant::now() + Duration::from_secs(15);
        while load_cursor(&pool, "miner-1").await.unwrap() as i64 != n {
            assert!(Instant::now() < deadline, "the loop did not recover");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        task.abort();

        assert_no_gaps_no_duplicates(&pool, "miner-1", n).await;
        let text = m.render();
        assert!(
            text.contains("pouw_indexer_event_source_errors_total 4"),
            "{text}"
        );
        assert!(
            text.contains("pouw_indexer_event_polling_mode{node_id=\"miner-1\"} 1"),
            "{text}"
        );
    }

    /// Serve a MemoryEventNode the way a real node would: GET /v1/events, JSON, 410 when expired.
    async fn serve(node: MemoryEventNode) -> String {
        use axum::{
            Json, Router,
            extract::{Query, State},
            http::StatusCode,
            routing::get,
        };
        async fn handler(
            State(node): State<MemoryEventNode>,
            Query(q): Query<HashMap<String, u64>>,
        ) -> (StatusCode, Json<serde_json::Value>) {
            let after = q.get("after").copied().unwrap_or(0);
            let limit = q.get("limit").copied().unwrap_or(100) as u32;
            match node.fetch(after, limit, 0).await {
                Ok(page) => (StatusCode::OK, Json(serde_json::to_value(page).unwrap())),
                Err(FetchError::CursorExpired { oldest_seq }) => (
                    StatusCode::GONE,
                    Json(serde_json::json!({"error": {"code": "cursor_expired",
                        "message": format!("oldest available seq is {oldest_seq}"),
                        "details": {"oldest_seq": oldest_seq}}})),
                ),
                Err(FetchError::Other(_)) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({})),
                ),
            }
        }
        let app = Router::new()
            .route("/v1/events", get(handler))
            .with_state(node);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    #[sqlx::test]
    async fn the_http_client_reads_pages_and_reports_outages(pool: PgPool) {
        let events = read_events(MINER_1).unwrap();
        let n = events.len() as i64;
        let url = serve(MemoryEventNode::new(events.clone())).await;
        let src = HttpEventSource::new(&url).unwrap();

        // pages continue where the cursor is
        let first = src.fetch(0, 5, 0).await.unwrap();
        assert_eq!((first.events.len(), first.next), (5, 5));
        let second = src.fetch(5, 5, 0).await.unwrap();
        assert_eq!(second.events[0].seq, 6);
        // and it all ends up in the database through the HTTP client
        let m = Metrics::new();
        ingest_until_caught_up(&pool, &src, &fast_cfg("miner-1"), &m)
            .await
            .unwrap();
        assert_no_gaps_no_duplicates(&pool, "miner-1", n).await;

        // 410 becomes CursorExpired with the oldest seq
        let old_url = serve(MemoryEventNode::new(events).with_retention(50)).await;
        match HttpEventSource::new(&old_url)
            .unwrap()
            .fetch(0, 10, 0)
            .await
        {
            Err(FetchError::CursorExpired { oldest_seq }) => assert_eq!(oldest_seq as i64, n - 49),
            other => panic!("expected CursorExpired, got {other:?}"),
        }

        // a node that is not there: an ordinary error, not a panic
        let dead = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            format!("http://{}", l.local_addr().unwrap())
        }; // the listener is dropped here, nothing listens on that port any more
        assert!(matches!(
            HttpEventSource::new(&dead).unwrap().fetch(0, 10, 0).await,
            Err(FetchError::Other(_))
        ));
    }
}

#[cfg(test)]
mod node_format_tests {
    use super::*;

    /// Events as the node's own types write them (pouw-api, event.rs): block events carry `id`,
    /// and `block_accepted` has no bucket fields.
    #[sqlx::test]
    async fn events_in_the_nodes_own_format_are_understood(pool: PgPool) {
        let blk = format!("blk:{}", "ab".repeat(32));
        let miner = format!("addr:{}", "cd".repeat(32));
        let ev = |seq: u64, kind: &str, data: serde_json::Value| Event {
            seq,
            time: "2026-11-08T10:00:00.000Z".into(),
            node_id: "node-1".into(),
            kind: kind.into(),
            data,
        };
        let events = vec![
            ev(
                1,
                "block_received",
                serde_json::json!({"id": blk, "height": 7, "from_peer": "p1"}),
            ),
            ev(
                2,
                "block_verified",
                serde_json::json!({"id": blk, "height": 7, "verify_ms": 4, "proof_count": 1, "ok": true}),
            ),
            ev(
                3,
                "block_accepted",
                serde_json::json!({"id": blk, "height": 7, "miner": miner, "tx_count": 2, "proof_count": 1}),
            ),
            ev(
                4,
                "block_orphaned",
                serde_json::json!({"id": blk, "height": 7}),
            ),
        ];
        let stats = store_page(&pool, "node-1", 0, &events).await.unwrap();
        assert_eq!(stats, PageStats { stored: 4, bad: 0 });
        let row: (i64, bool, bool) = sqlx::query_as(
            "SELECT height, accepted_at IS NOT NULL, orphaned_at IS NOT NULL FROM block_observations",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row, (7, true, true));
    }
}
