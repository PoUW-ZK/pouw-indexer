-- Events from nodes (contracts/node-events.md): things a node saw that are not in blocks.
-- Additive only: the API views of 0002 read other tables and are not touched.
--
-- Design: every event is first stored as it came, in node_events, keyed by (node_id, seq).
-- That key makes a replayed page harmless (no duplicates) and makes a missing seq visible.
-- The typed tables below are a projection of that log for the explorer and dashboards.
-- An event the indexer does not understand stays in node_events, nothing is lost.

CREATE TABLE node_events (
    node_id    TEXT NOT NULL,
    seq        BIGINT NOT NULL CHECK (seq >= 1),
    time       TIMESTAMPTZ NOT NULL,                -- the clock of that node
    kind       TEXT NOT NULL,
    data       JSONB NOT NULL,
    stored_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (node_id, seq)
);
CREATE INDEX node_events_kind_time ON node_events (kind, time);
-- finds the proof_request_started that belongs to a proof_produced (same txid)
CREATE INDEX node_events_started_txid ON node_events ((data ->> 'txid'), seq)
    WHERE kind = 'proof_request_started';

-- Events a node no longer had when we asked (HTTP 410). Lost observations are recorded,
-- not hidden: seq from_seq..to_seq of this node will never be in node_events.
CREATE TABLE event_gaps (
    node_id      TEXT NOT NULL,
    from_seq     BIGINT NOT NULL,
    to_seq       BIGINT NOT NULL,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (node_id, from_seq)
);

-- tx_rejected. txid is NULL when the node could not even parse the bytes.
CREATE TABLE tx_rejections (
    node_id      TEXT NOT NULL,
    seq          BIGINT NOT NULL,
    txid         BYTEA CHECK (octet_length(txid) = 32),
    reason_code  TEXT NOT NULL,
    message      TEXT NOT NULL,
    rejected_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (node_id, seq)
);
CREATE INDEX tx_rejections_reason ON tx_rejections (reason_code);

-- What one node did with one block: block_received, block_verified, block_accepted,
-- block_rejected, block_orphaned. One row per (node, block), filled in as events arrive.
CREATE TABLE block_observations (
    node_id          TEXT NOT NULL,
    block_hash       BYTEA NOT NULL CHECK (octet_length(block_hash) = 32),
    height           BIGINT,                         -- block_rejected may not know it
    received_at      TIMESTAMPTZ,
    from_peer        TEXT,
    verified_at      TIMESTAMPTZ,
    verify_ms        BIGINT,
    verified_ok      BOOLEAN,
    verified_proofs  BIGINT,
    accepted_at      TIMESTAMPTZ,
    rejected_at      TIMESTAMPTZ,
    reject_reason    TEXT,
    orphaned_at      TIMESTAMPTZ,
    PRIMARY KEY (node_id, block_hash)
);
CREATE INDEX block_observations_hash ON block_observations (block_hash);

-- A lottery_result can be stored before we know the proof system, so it may be empty.
ALTER TABLE proof_attempts ALTER COLUMN proof_system DROP NOT NULL;
