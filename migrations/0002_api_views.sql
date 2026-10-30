-- The public API of the explorer: views in schema "api" over the pouw-indexer tables.
-- Hasura tracks only these views. The frontend never sees the indexer tables, so a change
-- of the indexer schema means changing these views, not the frontend.
--
-- The views and pouw-dashboard/hasura/metadata change together: a change is a new
-- migration here (an applied one never changes) plus the same change in the copy
-- pouw-dashboard/hasura/seeds/default/05_api_views.sql, which its local compose uses.
-- Field names and meanings are the contract in pouw-docs/contracts/web-api.md.
--
-- Conventions
--   * ids are prefixed lowercase hex: blk:, tx:, proof:, circ:, addr: (as in node-api.md)
--   * hashes without a type (tx_root, witness_sha256) and field elements (eta, mask) are
--     lowercase hex without a prefix
--   * every row says whether its block is canonical; lists filter canonical = true

CREATE SCHEMA IF NOT EXISTS api;

CREATE FUNCTION api.hex(prefix text, b bytea) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    RETURN CASE WHEN b IS NULL THEN NULL ELSE prefix || encode(b, 'hex') END;

-- Keyset pagination: every list has a sort_key that is unique and ordered (newest is
-- largest). A page asks for sort_key below the last one it saw. Every view column that
-- holds it is declared COLLATE "C" (byte order), so the comparison does not depend on the
-- database locale. A COLLATE inside this function would not reach the column.
--   part 0 = an entry of block.txs (transfer or registration), part 1 = a proof tx.
--   A registration has no stored position; it sorts after the transfers of its block.
CREATE FUNCTION api.sort_key(height bigint, part int, pos int, id bytea) RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    RETURN lpad(height::text, 19, '0') || '.' || part || '.'
           || lpad(coalesce(pos, 999999)::text, 6, '0') || '.' || encode(id, 'hex');

-- ---------------------------------------------------------------- blocks
CREATE VIEW api.blocks AS
SELECT api.hex('blk:', b.hash)                 AS hash,
       b.height,
       api.hex('blk:', b.prev_hash)            AS prev_hash,
       b.block_time                            AS time,
       api.hex('addr:', b.miner)               AS miner,
       b.difficulty,
       b.bucket_count,
       b.bucket_id,
       encode(b.tx_root, 'hex')                AS tx_root,
       b.size_bytes,
       b.canonical,
       coalesce(t.n, 0)                        AS transfer_count,
       coalesce(c.n, 0)                        AS registration_count,
       coalesce(p.n, 0)                        AS proof_count,
       (coalesce(t.fee, 0) + coalesce(c.fee, 0) + coalesce(p.fee, 0))::bigint AS fee_total
FROM blocks b
LEFT JOIN LATERAL (SELECT count(*) AS n, sum(fee) AS fee FROM transfers WHERE block_hash = b.hash) t ON true
LEFT JOIN LATERAL (SELECT count(*) AS n, sum(fee) AS fee FROM circuits WHERE registered_block = b.hash) c ON true
LEFT JOIN LATERAL (SELECT count(*) AS n, sum(fee) AS fee FROM proof_txs WHERE block_hash = b.hash) p ON true;

-- ---------------------------------------------------------------- transfers
CREATE VIEW api.transfers AS
SELECT api.hex('tx:', t.txid)                  AS txid,
       api.hex('blk:', t.block_hash)           AS block_hash,
       b.height,
       b.block_time                            AS time,
       t.position,
       b.canonical,
       api.hex('addr:', t.sender)              AS sender,
       api.hex('addr:', t.recipient)           AS recipient,
       t.amount,
       t.fee,
       t.nonce,
       api.sort_key(b.height, 0, t.position, t.txid) COLLATE "C" AS sort_key
FROM transfers t
JOIN blocks b ON b.hash = t.block_hash;

-- ---------------------------------------------------------------- circuits
-- One row per registration. The circuit id is the txid of the register_circuit tx.
CREATE VIEW api.circuits AS
SELECT api.hex('circ:', c.circuit_id)          AS circuit_id,
       api.hex('tx:', c.circuit_id)            AS txid,
       c.proof_system,
       c.constraint_count,
       c.verifying_key_bytes,
       c.circuit_bytes,
       api.hex('addr:', c.registrant)          AS registrant,
       c.fee,
       api.hex('blk:', c.registered_block)     AS registered_block,
       c.registered_height,
       b.block_time                            AS registered_time,
       coalesce(b.canonical, false)            AS canonical,
       coalesce(p.n, 0)                        AS proof_count,
       p.last_height                           AS last_proof_height,
       coalesce(p.n, 0) * c.constraint_count   AS useful_work,
       api.sort_key(c.registered_height, 0, NULL, c.circuit_id) COLLATE "C" AS sort_key
FROM circuits c
LEFT JOIN blocks b ON b.hash = c.registered_block
LEFT JOIN LATERAL (
    SELECT count(*) AS n, max(pb.height) AS last_height
    FROM proof_txs pt JOIN blocks pb ON pb.hash = pt.block_hash AND pb.canonical
    WHERE pt.circuit_id = c.circuit_id
) p ON true;

-- ---------------------------------------------------------------- proof transactions
CREATE VIEW api.proofs AS
SELECT api.hex('proof:', p.proof_hash)         AS proof_hash,
       api.hex('tx:', p.request_txid)          AS request_txid,
       api.hex('blk:', p.block_hash)           AS block_hash,
       b.height,
       b.block_time                            AS time,
       p.position,
       b.canonical,
       api.hex('addr:', b.miner)               AS miner,
       api.hex('addr:', p.requester)           AS client,
       api.hex('circ:', p.circuit_id)          AS circuit_id,
       c.proof_system,
       c.constraint_count,
       p.fee,
       p.nonce,
       p.usage_counter,
       p.proof_bytes,
       p.witness_bytes,
       encode(p.witness_sha256, 'hex')         AS witness_sha256,
       p.public_input_count,
       encode(p.eta, 'hex')                    AS eta,
       encode(p.mask, 'hex')                   AS mask,
       api.sort_key(b.height, 1, p.position, p.proof_hash) COLLATE "C" AS sort_key
FROM proof_txs p
JOIN blocks b ON b.hash = p.block_hash
JOIN circuits c ON c.circuit_id = p.circuit_id;

-- ---------------------------------------------------------------- address history
-- One row per (address, transaction) as seen from that address, canonical blocks only.
-- A transfer to oneself is one row with direction 'self'.
CREATE VIEW api.address_txs AS
SELECT api.hex('addr:', t.sender) AS address,
       CASE WHEN t.sender = t.recipient THEN 'self' ELSE 'out' END AS direction,
       'transfer' AS kind, api.hex('tx:', t.txid) AS txid,
       b.height, b.block_time AS time, t.position,
       api.hex('addr:', t.sender) AS sender, api.hex('addr:', t.recipient) AS recipient,
       t.amount, t.fee, t.nonce, NULL::text AS circuit_id, NULL::text AS proof_hash,
       api.sort_key(b.height, 0, t.position, t.txid) COLLATE "C" AS sort_key
FROM transfers t JOIN blocks b ON b.hash = t.block_hash AND b.canonical
UNION ALL
SELECT api.hex('addr:', t.recipient), 'in', 'transfer', api.hex('tx:', t.txid),
       b.height, b.block_time, t.position,
       api.hex('addr:', t.sender), api.hex('addr:', t.recipient),
       t.amount, t.fee, t.nonce, NULL, NULL,
       api.sort_key(b.height, 0, t.position, t.txid) COLLATE "C"
FROM transfers t JOIN blocks b ON b.hash = t.block_hash AND b.canonical
WHERE t.recipient <> t.sender
UNION ALL
SELECT api.hex('addr:', c.registrant), 'out', 'register_circuit', api.hex('tx:', c.circuit_id),
       c.registered_height, b.block_time, NULL,
       api.hex('addr:', c.registrant), NULL,
       NULL, c.fee, NULL, api.hex('circ:', c.circuit_id), NULL,
       api.sort_key(c.registered_height, 0, NULL, c.circuit_id) COLLATE "C"
FROM circuits c JOIN blocks b ON b.hash = c.registered_block AND b.canonical
UNION ALL
SELECT api.hex('addr:', p.requester), 'out', 'proof_request', api.hex('tx:', p.request_txid),
       b.height, b.block_time, p.position,
       api.hex('addr:', p.requester), NULL,
       NULL, p.fee, p.nonce, api.hex('circ:', p.circuit_id), api.hex('proof:', p.proof_hash),
       api.sort_key(b.height, 1, p.position, p.proof_hash) COLLATE "C"
FROM proof_txs p JOIN blocks b ON b.hash = p.block_hash AND b.canonical;

-- ---------------------------------------------------------------- address summary
CREATE VIEW api.addresses AS
WITH a AS (
    SELECT sender AS address, 1 AS sent, 0 AS received, 0 AS requests, 0 AS circuits, 0 AS mined, b.height
    FROM transfers t JOIN blocks b ON b.hash = t.block_hash AND b.canonical
    UNION ALL
    SELECT recipient, 0, 1, 0, 0, 0, b.height
    FROM transfers t JOIN blocks b ON b.hash = t.block_hash AND b.canonical
    WHERE t.recipient <> t.sender
    UNION ALL
    SELECT registrant, 1, 0, 0, 1, 0, c.registered_height
    FROM circuits c JOIN blocks b ON b.hash = c.registered_block AND b.canonical
    UNION ALL
    SELECT requester, 0, 0, 1, 0, 0, b.height
    FROM proof_txs p JOIN blocks b ON b.hash = p.block_hash AND b.canonical
    UNION ALL
    SELECT miner, 0, 0, 0, 0, 1, height
    FROM blocks WHERE canonical
)
SELECT api.hex('addr:', address) AS address,
       sum(sent)::bigint         AS txs_sent,
       sum(received)::bigint     AS txs_received,
       sum(requests)::bigint     AS proof_requests,
       sum(circuits)::bigint     AS circuits_registered,
       sum(mined)::bigint        AS blocks_mined,
       min(height)               AS first_seen_height
FROM a
GROUP BY address;

-- ---------------------------------------------------------------- home: one row
CREATE VIEW api.stats AS
WITH c AS (SELECT * FROM blocks WHERE canonical),
     tip AS (SELECT * FROM c ORDER BY height DESC LIMIT 1)
SELECT (SELECT height FROM tip)                                   AS indexed_height,
       (SELECT block_time FROM tip)                               AS latest_block_time,
       (SELECT bucket_count FROM tip)                             AS bucket_count,
       (SELECT count(*) FROM c)                                   AS block_count,
       (SELECT count(*) FROM transfers t JOIN c ON c.hash = t.block_hash) AS transfer_count,
       (SELECT count(*) FROM proof_txs p JOIN c ON c.hash = p.block_hash) AS proof_count,
       (SELECT count(*) FROM circuits x JOIN c ON c.hash = x.registered_block) AS circuit_count,
       (SELECT extract(epoch FROM max(block_time) - min(block_time)) / nullif(count(*) - 1, 0)
          FROM c)::float8                                         AS avg_block_interval_s;

-- ---------------------------------------------------------------- mining screen
CREATE VIEW api.miner_shares AS
SELECT api.hex('addr:', miner)                                    AS miner,
       count(*)                                                   AS blocks,
       round(count(*)::numeric / sum(count(*)) OVER (), 4)        AS share,
       max(height)                                                AS last_block_height
FROM blocks
WHERE canonical
GROUP BY miner;

CREATE VIEW api.useful_work AS
SELECT s.proof_system,
       count(p.proof_hash)                                        AS proof_count,
       coalesce(sum(c.constraint_count), 0)::bigint               AS useful_work
FROM (VALUES ('groth16'), ('plonk_kzg'), ('marlin')) AS s (proof_system)
LEFT JOIN circuits c ON c.proof_system = s.proof_system
LEFT JOIN proof_txs p ON p.circuit_id = c.circuit_id
     AND EXISTS (SELECT 1 FROM blocks b WHERE b.hash = p.block_hash AND b.canonical)
GROUP BY s.proof_system;

-- ---------------------------------------------------------------- search
-- Hasura needs a tracked view as the return type of a function.
CREATE VIEW api.search_hits AS
SELECT NULL::text AS kind, NULL::text AS id, NULL::bigint AS height WHERE false;

-- The prefix of q selects what is searched. A tx: id can be a transfer, a circuit
-- registration or a proof request. Unknown input returns no rows, never an error.
CREATE FUNCTION api.search(q text) RETURNS SETOF api.search_hits
    LANGUAGE sql STABLE AS $$
    SELECT 'block', hash, height FROM api.blocks
     WHERE height = CASE WHEN q ~ '^[0-9]{1,18}$' THEN q::bigint END AND canonical
    UNION ALL
    SELECT 'block', hash, height FROM api.blocks WHERE q ~ '^blk:' AND hash = q
    UNION ALL
    SELECT 'transfer', txid, height FROM api.transfers WHERE q ~ '^tx:' AND txid = q
    UNION ALL
    SELECT 'circuit', circuit_id, registered_height FROM api.circuits WHERE q ~ '^tx:' AND txid = q
    UNION ALL
    SELECT 'proof', proof_hash, height FROM api.proofs WHERE q ~ '^tx:' AND request_txid = q
    UNION ALL
    SELECT 'proof', proof_hash, height FROM api.proofs WHERE q ~ '^proof:' AND proof_hash = q
    UNION ALL
    SELECT 'circuit', circuit_id, registered_height FROM api.circuits WHERE q ~ '^circ:' AND circuit_id = q
    UNION ALL
    SELECT 'address', address, first_seen_height FROM api.addresses WHERE q ~ '^addr:' AND address = q
$$;
