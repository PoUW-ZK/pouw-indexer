-- pouw-indexer schema v0
--
-- Built from pouw-docs contracts/block.md (what is in a block) and contracts/node-events.md
-- (what a node tells the indexer). Issue #6 lists the tables: blocks, proof txs, miners,
-- circuits, mempool, miner attempts.
--
-- Conventions
--   * Hashes, ids and addresses are stored as raw bytes (BYTEA, 32 bytes). The API adds the
--     "tx:", "blk:", "addr:" prefixes and hex when it prints them.
--   * u64 values are stored as BIGINT. A value above 9.2e18 would be refused by Postgres.
--   * A block can be orphaned (reorg). It is kept, with canonical = FALSE, not deleted.
--     Rows that belong to a block are keyed by (block_hash, position), because the same
--     transaction can sit in two competing blocks.

-- ---------------------------------------------------------------- who mined
CREATE TABLE miners (
    address     BYTEA PRIMARY KEY CHECK (octet_length(address) = 32),
    first_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------- blocks
CREATE TABLE blocks (
    hash          BYTEA PRIMARY KEY CHECK (octet_length(hash) = 32),
    height        BIGINT NOT NULL CHECK (height >= 0),
    prev_hash     BYTEA NOT NULL CHECK (octet_length(prev_hash) = 32),
    block_time    TIMESTAMPTZ NOT NULL,                 -- header.timestamp (unix seconds)
    miner         BYTEA NOT NULL REFERENCES miners (address),
    difficulty    BIGINT NOT NULL,
    bucket_count  INTEGER NOT NULL CHECK (bucket_count >= 1),
    bucket_id     INTEGER NOT NULL CHECK (bucket_id >= 0),
    leaf_count    INTEGER NOT NULL CHECK (leaf_count >= 0),   -- header.tx_count = txs + proof_txs
    tx_root       BYTEA NOT NULL CHECK (octet_length(tx_root) = 32),
    size_bytes    INTEGER NOT NULL,
    canonical     BOOLEAN NOT NULL DEFAULT TRUE,        -- FALSE once a reorg dropped it
    indexed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- at most one canonical block per height
CREATE UNIQUE INDEX blocks_one_canonical_per_height ON blocks (height) WHERE canonical;
CREATE INDEX blocks_height ON blocks (height DESC);
CREATE INDEX blocks_miner ON blocks (miner, height DESC);
CREATE INDEX blocks_prev_hash ON blocks (prev_hash);

-- ---------------------------------------------------------------- circuits
-- One row per register_circuit transaction. circuit_id is the txid of that transaction.
-- The big blobs (verifying key, circuit) are not stored, only their sizes (see ADR-0011).
-- Limitation of v0: if the registering block is orphaned, the row stays.
CREATE TABLE circuits (
    circuit_id            BYTEA PRIMARY KEY CHECK (octet_length(circuit_id) = 32),
    proof_system          TEXT NOT NULL CHECK (proof_system IN ('groth16', 'plonk_kzg', 'marlin')),
    constraint_count      BIGINT NOT NULL,
    verifying_key_bytes   INTEGER NOT NULL,
    circuit_bytes         INTEGER NOT NULL,
    registrant            BYTEA NOT NULL CHECK (octet_length(registrant) = 32),
    fee                   BIGINT NOT NULL,
    registered_block      BYTEA NOT NULL,               -- hash of the block that carried it
    registered_height     BIGINT NOT NULL
);
CREATE INDEX circuits_system ON circuits (proof_system);

-- ---------------------------------------------------------------- transfers
CREATE TABLE transfers (
    block_hash  BYTEA NOT NULL REFERENCES blocks (hash) ON DELETE CASCADE,
    position    INTEGER NOT NULL,                        -- index inside block.txs
    txid        BYTEA NOT NULL CHECK (octet_length(txid) = 32),
    sender      BYTEA NOT NULL,
    recipient   BYTEA NOT NULL,
    amount      BIGINT NOT NULL,
    fee         BIGINT NOT NULL,
    nonce       BIGINT NOT NULL,
    PRIMARY KEY (block_hash, position)
);
CREATE INDEX transfers_txid ON transfers (txid);
CREATE INDEX transfers_sender ON transfers (sender);
CREATE INDEX transfers_recipient ON transfers (recipient);

-- ---------------------------------------------------------------- proof transactions
-- One row per entry of block.proof_txs: a client request plus the proof that answered it.
-- The witness bytes are not stored, only their length and hash (ADR-0011 allows this).
CREATE TABLE proof_txs (
    block_hash          BYTEA NOT NULL REFERENCES blocks (hash) ON DELETE CASCADE,
    position            INTEGER NOT NULL,                -- index inside block.proof_txs
    proof_hash          BYTEA NOT NULL CHECK (octet_length(proof_hash) = 32),
    request_txid        BYTEA NOT NULL CHECK (octet_length(request_txid) = 32),
    circuit_id          BYTEA NOT NULL REFERENCES circuits (circuit_id),
    requester           BYTEA NOT NULL,
    fee                 BIGINT NOT NULL,
    nonce               BIGINT NOT NULL,
    usage_counter       BIGINT NOT NULL,
    proof_bytes         INTEGER NOT NULL,
    witness_bytes       INTEGER NOT NULL,
    witness_sha256      BYTEA NOT NULL CHECK (octet_length(witness_sha256) = 32),
    public_input_count  INTEGER NOT NULL,
    eta                 BYTEA CHECK (octet_length(eta) = 31),    -- derived, see block.md
    mask                BYTEA CHECK (octet_length(mask) = 31),   -- derived, see block.md
    PRIMARY KEY (block_hash, position)
);
CREATE INDEX proof_txs_proof_hash ON proof_txs (proof_hash);
CREATE INDEX proof_txs_request ON proof_txs (request_txid);
CREATE INDEX proof_txs_circuit ON proof_txs (circuit_id);
CREATE INDEX proof_txs_requester ON proof_txs (requester);

-- ---------------------------------------------------------------- miner attempts
-- From node events proof_produced and lottery_result of one miner node. A row with
-- won = FALSE is a losing proof, that is the wasted work. The two events arrive separately,
-- so won stays NULL until lottery_result comes.
CREATE TABLE proof_attempts (
    node_id        TEXT NOT NULL,
    proof_hash     BYTEA NOT NULL CHECK (octet_length(proof_hash) = 32),
    request_txid   BYTEA NOT NULL,
    circuit_id     BYTEA NOT NULL,
    proof_system   TEXT NOT NULL,
    started_at     TIMESTAMPTZ,                          -- proof_request_started
    produced_at    TIMESTAMPTZ,                          -- proof_produced
    prove_ms       BIGINT,
    won            BOOLEAN,                              -- lottery_result
    eta            BYTEA CHECK (octet_length(eta) = 31),
    mask           BYTEA CHECK (octet_length(mask) = 31),
    usage_counter  BIGINT,
    PRIMARY KEY (node_id, proof_hash)
);
CREATE INDEX proof_attempts_circuit ON proof_attempts (circuit_id);
CREATE INDEX proof_attempts_lost ON proof_attempts (node_id) WHERE won = FALSE;

-- ---------------------------------------------------------------- mempool
-- Transactions a node accepted (event tx_received). Requests that appear here and never
-- reach a block are the unanswered ones, the indexer finds them by comparing with blocks.
CREATE TABLE tx_observations (
    node_id      TEXT NOT NULL,
    txid         BYTEA NOT NULL CHECK (octet_length(txid) = 32),
    tx_kind      TEXT NOT NULL CHECK (tx_kind IN ('transfer', 'proof_request', 'register_circuit')),
    sender       BYTEA NOT NULL,
    fee          BIGINT NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL,                   -- the clock of that node
    PRIMARY KEY (node_id, txid)
);
CREATE INDEX tx_observations_txid ON tx_observations (txid);

-- Event mempool_snapshot, every 10 s per node.
CREATE TABLE mempool_snapshots (
    node_id                 TEXT NOT NULL,
    observed_at             TIMESTAMPTZ NOT NULL,
    pending_txs             BIGINT NOT NULL,
    pending_proof_requests  BIGINT NOT NULL,
    by_circuit              JSONB NOT NULL,              -- {"<circuit id>": count}
    PRIMARY KEY (node_id, observed_at)
);

-- ---------------------------------------------------------------- where we stopped
-- Last event seq processed for each node. Written in the same transaction as the events.
CREATE TABLE ingest_cursors (
    node_id   TEXT PRIMARY KEY,
    last_seq  BIGINT NOT NULL CHECK (last_seq >= 0)
);