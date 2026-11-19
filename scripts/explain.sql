-- EXPLAIN of the explorer's queries on a database of realistic size.
--
-- Run it on a SCRATCH database that the indexer has already migrated (never on staging data):
--
--   createdb pouw_explain
--   DATABASE_URL=postgres://.../pouw_explain pouw-indexer seed     # runs the migrations
--   psql postgres://.../pouw_explain -v blocks=20000 -v per_block=300 -f scripts/explain.sql
--
-- 20000 blocks x 300 transfers = 6 million transfers. Needs a few GB and a few minutes.
-- Three "hot" addresses take part in about 1 transfer in 10, like the load generator's accounts.
-- What to look for in the plans: Index Scan / Index Only Scan instead of Seq Scan on blocks or
-- transfers, "Rows Removed by Filter" close to 0, and no Sort over thousands of rows.

\set ON_ERROR_STOP on
\timing on

CREATE FUNCTION pg_temp.h(n bigint, salt text) RETURNS bytea LANGUAGE sql IMMUTABLE AS
$$ SELECT decode(md5(salt || n::text) || md5(n::text || salt), 'hex') $$;

INSERT INTO miners (address)
SELECT pg_temp.h(m, 'miner') FROM generate_series(1, 3) m ON CONFLICT DO NOTHING;

INSERT INTO blocks (hash, height, prev_hash, block_time, miner, tx_root, size_bytes, leaf_count,
                    transfer_count, transfer_fees, fee_total, canonical)
SELECT pg_temp.h(g, 'blk'), 1000000 + g, pg_temp.h(g - 1, 'blk'),
       now() - (:blocks - g) * interval '10 seconds',
       pg_temp.h(1 + g % 3, 'miner'), pg_temp.h(g, 'root'), 40000, :per_block,
       :per_block, :per_block * 10, :per_block * 10,
       g % 500 <> 0                        -- a few orphans between the canonical blocks
FROM generate_series(1, :blocks) g;

-- orphans share the height of their canonical neighbour only in a real chain; here they take
-- a height of their own, which is enough for the unique index
INSERT INTO transfers (block_hash, position, txid, sender, recipient, amount, fee, nonce)
SELECT pg_temp.h(g, 'blk'), p, pg_temp.h(g * 100000 + p, 'tx'),
       pg_temp.h(CASE WHEN p % 10 = 0 THEN p % 3 ELSE 100 + (g * 7 + p) % 5000 END, 'addr'),
       pg_temp.h(CASE WHEN p % 10 = 5 THEN p % 3 ELSE 100 + (g * 13 + p) % 5000 END, 'addr'),
       1 + p, 10, g
FROM generate_series(1, :blocks) g, generate_series(0, :per_block - 1) p;

ANALYZE;

\echo
\echo '== 1. latest blocks (explorer home, every second per open page)'
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM api.blocks WHERE canonical ORDER BY height DESC LIMIT 20;

\echo '== 2. one block by hash'
SELECT api.hex('blk:', hash) AS some_hash FROM blocks WHERE height = 1000000 + :blocks / 2 \gset
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM api.blocks WHERE hash = :'some_hash';

\echo '== 3a. transactions of an ordinary address, newest 20'
SELECT api.hex('addr:', pg_temp.h(1234, 'addr')) AS cold \gset
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM api.address_txs WHERE address = :'cold' ORDER BY sort_key DESC LIMIT 20;

\echo '== 3b. transactions of a hot address (about 1 in 10 transfers). Expect this one to be the'
\echo '       slowest: all its rows are found through the index, joined to blocks and sorted'
SELECT api.hex('addr:', pg_temp.h(1, 'addr')) AS hot \gset
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM api.address_txs WHERE address = :'hot' ORDER BY sort_key DESC LIMIT 20;

\echo '== 4a. search by transaction id'
SELECT api.hex('tx:', pg_temp.h(5000 * 100000 + 7, 'tx')) AS some_tx \gset
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM api.search(:'some_tx');

\echo '== 4b. search by block height'
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM api.search((1000000 + :blocks / 2)::text);

\echo '== 4c. search by address (api.addresses aggregates five sources; the filter must reach them)'
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM api.search(:'cold');

\echo '== 5. Grafana: network_stats of the last hour'
EXPLAIN (ANALYZE, BUFFERS)
SELECT minute, transfers_per_s, fees FROM network_stats WHERE minute > now() - interval '1 hour' ORDER BY minute;

\echo '== 6. cost of the refresh the indexer runs after new blocks (at most every 2 s)'
SELECT api.refresh_stats();
