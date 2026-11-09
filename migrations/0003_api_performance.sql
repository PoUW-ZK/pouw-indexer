-- Performance of the explorer API (pouw-dashboard#43, #42).
--
-- 1. Statistics as materialized views. api.stats, api.miner_shares and api.useful_work
--    aggregate the whole chain. Hasura runs a live query once per second for all clients
--    of the same subscription, so the cost of one run is what matters. As materialized
--    views they are computed once per refresh and read as a few stored rows.
--    Columns, names and meaning stay the same, so Hasura metadata and the contract
--    (pouw-docs/contracts/web-api.md) do not change.
--
-- 2. The indexer calls api.refresh_stats() after it commits new blocks (or a reorg).
--    Until the first call the statistics are empty. REFRESH ... CONCURRENTLY lets Hasura
--    keep reading the old rows during the refresh; it needs a unique index on each view.
--
-- 3. Expression indexes for lookups by id. The api views expose ids as text
--    (api.hex('blk:', hash)), and a filter on that text can not use the indexes on the
--    raw bytea columns of 0001. An index on exactly the same expression can.

-- ---------------------------------------------------------------- 1. statistics

DROP VIEW api.stats;
DROP VIEW api.miner_shares;
DROP VIEW api.useful_work;

CREATE MATERIALIZED VIEW api.stats AS
WITH c AS (SELECT * FROM blocks WHERE canonical),
     tip AS (SELECT * FROM c ORDER BY height DESC LIMIT 1)
SELECT 1                                                          AS id,
       (SELECT height FROM tip)                                   AS indexed_height,
       (SELECT block_time FROM tip)                               AS latest_block_time,
       (SELECT bucket_count FROM tip)                             AS bucket_count,
       (SELECT count(*) FROM c)                                   AS block_count,
       (SELECT count(*) FROM transfers t JOIN c ON c.hash = t.block_hash) AS transfer_count,
       (SELECT count(*) FROM proof_txs p JOIN c ON c.hash = p.block_hash) AS proof_count,
       (SELECT count(*) FROM circuits x JOIN c ON c.hash = x.registered_block) AS circuit_count,
       (SELECT extract(epoch FROM max(block_time) - min(block_time)) / nullif(count(*) - 1, 0)
          FROM c)::float8                                         AS avg_block_interval_s;
-- one row always; id exists only for the unique index that CONCURRENTLY needs
CREATE UNIQUE INDEX stats_id ON api.stats (id);

CREATE MATERIALIZED VIEW api.miner_shares AS
SELECT api.hex('addr:', miner)                                    AS miner,
       count(*)                                                   AS blocks,
       round(count(*)::numeric / sum(count(*)) OVER (), 4)        AS share,
       max(height)                                                AS last_block_height
FROM blocks
WHERE canonical
GROUP BY miner;
CREATE UNIQUE INDEX miner_shares_miner ON api.miner_shares (miner);

CREATE MATERIALIZED VIEW api.useful_work AS
SELECT s.proof_system,
       count(p.proof_hash)                                        AS proof_count,
       coalesce(sum(c.constraint_count), 0)::bigint               AS useful_work
FROM (VALUES ('groth16'), ('plonk_kzg'), ('marlin')) AS s (proof_system)
LEFT JOIN circuits c ON c.proof_system = s.proof_system
LEFT JOIN proof_txs p ON p.circuit_id = c.circuit_id
     AND EXISTS (SELECT 1 FROM blocks b WHERE b.hash = p.block_hash AND b.canonical)
GROUP BY s.proof_system;
CREATE UNIQUE INDEX useful_work_system ON api.useful_work (proof_system);

-- The indexer runs this after each commit that adds blocks or changes canonical:
--   SELECT api.refresh_stats();
-- Milliseconds on a test network. Must run outside the transaction that wrote the blocks
-- or after its commit, so the views see the new rows.
CREATE FUNCTION api.refresh_stats() RETURNS void
    LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY api.stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY api.miner_shares;
    REFRESH MATERIALIZED VIEW CONCURRENTLY api.useful_work;
END $$;

-- ---------------------------------------------------------------- 2. lookups by id
-- Each index matches an expression of 0002, used by a filter of the contract.

-- blocks: BlockByHash, MinedBlocks, search
CREATE INDEX blocks_hash_hex   ON blocks (api.hex('blk:', hash));
CREATE INDEX blocks_miner_hex  ON blocks (api.hex('addr:', miner), height DESC);

-- api.blocks counts the registrations of each block (registration_count, fee_total);
-- 0001 has no index for that join
CREATE INDEX circuits_registered_block ON circuits (registered_block);

-- transfers: Transaction, search, address history
CREATE INDEX transfers_txid_hex      ON transfers (api.hex('tx:', txid));
CREATE INDEX transfers_sender_hex    ON transfers (api.hex('addr:', sender));
CREATE INDEX transfers_recipient_hex ON transfers (api.hex('addr:', recipient));

-- circuits: Circuit, Transaction (registration), search, address history
CREATE INDEX circuits_id_hex         ON circuits (api.hex('circ:', circuit_id));
CREATE INDEX circuits_txid_hex       ON circuits (api.hex('tx:', circuit_id));
CREATE INDEX circuits_registrant_hex ON circuits (api.hex('addr:', registrant));

-- proof_txs: Proof, Transaction (request), CircuitProofs, search, address history
CREATE INDEX proof_txs_hash_hex      ON proof_txs (api.hex('proof:', proof_hash));
CREATE INDEX proof_txs_request_hex   ON proof_txs (api.hex('tx:', request_txid));
CREATE INDEX proof_txs_circuit_hex   ON proof_txs (api.hex('circ:', circuit_id));
CREATE INDEX proof_txs_requester_hex ON proof_txs (api.hex('addr:', requester));
