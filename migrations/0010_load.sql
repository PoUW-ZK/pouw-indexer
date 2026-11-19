-- Load: blocks with hundreds of transfers, three miners (pouw-deploy#65).
--
-- 1. Counters on the block. api.blocks and network_stats counted the transfers, proofs and fees
--    of a block with a subquery per block. For a page of 20 blocks that is 20 scans of hundreds
--    of rows per run, and Hasura runs it every second for every open page. The indexer now
--    writes the numbers once, when it writes the block (src/store/blocks.rs), and the views
--    only read them. A block never changes after it is written, so they never go stale.
-- 2. network_stats: transfers per second and fees, for Grafana -> PoUW · Network. The first
--    seven columns are unchanged, the new ones are added at the end.
-- 3. Indexes: one that the explorer's "latest blocks" wants, and three that only slow
--    writes down (see below).
--
-- Verify the plans with scripts/explain.sql on a database of realistic size.

-- ---------------------------------------------------------------- 1. block counters
ALTER TABLE blocks
    ADD COLUMN transfer_count      INTEGER NOT NULL DEFAULT 0,   -- entries of txs that are transfers
    ADD COLUMN registration_count  INTEGER NOT NULL DEFAULT 0,   -- entries of txs that register a circuit
    ADD COLUMN proof_count         INTEGER NOT NULL DEFAULT 0,   -- entries of proof_txs
    ADD COLUMN transfer_fees       BIGINT  NOT NULL DEFAULT 0,   -- fees of the transfers
    ADD COLUMN fee_total           BIGINT  NOT NULL DEFAULT 0,   -- fees of every transaction in the block
    ADD COLUMN useful_work         BIGINT  NOT NULL DEFAULT 0;   -- constraint counts of the circuits of its proofs

-- Blocks written before this migration. Same meaning as the old views: a registration counts
-- for the block the circuits table points at.
UPDATE blocks b SET
    transfer_count     = (SELECT count(*) FROM transfers t WHERE t.block_hash = b.hash),
    transfer_fees      = (SELECT coalesce(sum(t.fee), 0) FROM transfers t WHERE t.block_hash = b.hash)::bigint,
    registration_count = (SELECT count(*) FROM circuits c WHERE c.registered_block = b.hash),
    proof_count        = (SELECT count(*) FROM proof_txs p WHERE p.block_hash = b.hash),
    useful_work        = (SELECT coalesce(sum(ci.constraint_count), 0)
                            FROM proof_txs p JOIN circuits ci ON ci.circuit_id = p.circuit_id
                           WHERE p.block_hash = b.hash)::bigint;

UPDATE blocks b SET
    fee_total = b.transfer_fees
        + (SELECT coalesce(sum(c.fee), 0) FROM circuits c WHERE c.registered_block = b.hash)::bigint
        + (SELECT coalesce(sum(p.fee), 0) FROM proof_txs p WHERE p.block_hash = b.hash)::bigint;

-- api.blocks: same columns, order and types as in 0002, read from the counters.
CREATE OR REPLACE VIEW api.blocks AS
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
       b.transfer_count::bigint                AS transfer_count,
       b.registration_count::bigint            AS registration_count,
       b.proof_count::bigint                   AS proof_count,
       b.fee_total                             AS fee_total
FROM blocks b;

-- ---------------------------------------------------------------- 2. network_stats
-- Per minute, canonical blocks only, so a reorganisation corrects it by itself.
--   transfers_per_s  coin transfers in that minute divided by 60. The newest minute is still
--                    filling up, so its value is low until the minute is over.
--   txs_per_s        the same for all transactions (transfers, registrations, proofs)
--   fees             all fees paid by transactions in blocks of that minute
--   transfer_fees    the part of them that comes from transfers
-- The gap to the previous block is taken from the parent block (prev_hash), which on the
-- canonical chain is the previous canonical block, so the view does not need a window over
-- the whole chain and reads only the blocks table.
CREATE OR REPLACE VIEW network_stats AS
SELECT date_trunc('minute', b.block_time)                                   AS minute,
       count(*)                                                             AS blocks,
       sum(b.transfer_count)::bigint                                        AS transfers,
       sum(b.proof_count)::bigint                                           AS proofs,
       sum(b.leaf_count)::bigint                                            AS txs,
       avg(extract(epoch FROM b.block_time - p.block_time))::float8         AS avg_block_time_s,
       sum(b.useful_work)::bigint                                           AS useful_work,
       (sum(b.transfer_count) / 60.0)::float8                               AS transfers_per_s,
       (sum(b.leaf_count) / 60.0)::float8                                   AS txs_per_s,
       sum(b.fee_total)::bigint                                             AS fees,
       sum(b.transfer_fees)::bigint                                         AS transfer_fees
FROM blocks b
LEFT JOIN blocks p ON p.hash = b.prev_hash
WHERE b.canonical
GROUP BY 1;

-- ---------------------------------------------------------------- 3. indexes
-- The explorer's home page and every "tip" query: newest canonical blocks. Smaller than
-- blocks_height (orphans are not in it) and the planner does not filter rows out of it.
CREATE INDEX blocks_canonical_height ON blocks (height DESC) WHERE canonical;

-- 0003 added an index on the hex expression of each id, because the API views filter on
-- exactly that expression. These three on the raw columns were not used by any query, and
-- transfers is the table that grows by hundreds of rows per block, so each of them cost a
-- write per row. (proof_txs and circuits keep theirs: few rows, and api.circuits joins
-- proof_txs on the raw circuit_id.)
DROP INDEX transfers_txid;
DROP INDEX transfers_sender;
DROP INDEX transfers_recipient;
