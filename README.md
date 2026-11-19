# pouw-indexer

Follows the PoUW-ZK nodes and writes the chain into PostgreSQL: blocks,
transfers, proofs, circuits, balances, statistics and what the nodes saw
(events). Hasura (pouw-dashboard) serves it to the explorer as GraphQL, Grafana
reads it directly.

```text
pouw-node ──GET /v1/blocks, /v1/accounts──▶ pouw-indexer ──▶ Postgres ──▶ Hasura ──▶ explorer
          ──GET /v1/events (long-poll)───▶                         └──▶ Grafana
```

- Blocks come from one node (`NODE_URL`, in the devnet node-1, the miner). The
  indexer asks for the JSON view (`Accept: application/json`), the node answers
  borsh otherwise (`contracts/node-api.md`).
- Events come from every node (`EVENT_SOURCES`), each with its own cursor, so a
  restart continues where it stopped.
- Formats are in pouw-docs: `contracts/block.md`, `node-api.md`,
  `node-events.md`, the API for the explorer in `web-api.graphql`.

## Configuration

Environment variables, all read in `src/config.rs` (a `.env` file works too):

| Variable         | Meaning                                                     | Default        |
| ---------------- | ----------------------------------------------------------- | -------------- |
| `DATABASE_URL`   | Postgres connection string                                  | required       |
| `BIND_ADDR`      | address of `/health` and `/metrics`                         | `0.0.0.0:8000` |
| `NODE_URL`       | REST API of the node whose blocks are indexed               | blocks off     |
| `NODE_FIXTURES`  | a file of blocks to follow instead of a node                | blocks off     |
| `NODE_ID`        | name of that node in `sync_state`                           | `node`         |
| `EVENT_SOURCES`  | event streams of nodes, `node-1=http://node-1:8000,...`     | events off     |
| `EVENT_FIXTURES` | recorded event streams, `node-1=file,...`                   | events off     |

`NODE_URL` and `NODE_FIXTURES` exclude each other, as do `EVENT_SOURCES` and
`EVENT_FIXTURES`. A node id is 1 to 64 characters (`pouw-api` `NodeId`).

Commands: `pouw-indexer` or `pouw-indexer serve` (migrations, then follow and
serve), `pouw-indexer seed [file]` (load fixture blocks, default
`tests/fixtures/blocks.borsh.jsonl`).

## Schema

Migrations in `migrations/` run on every start (sqlx). The tables are in
schema `public`, the API of the explorer is views in schema `api` that Hasura
tracks.

| Migration | What it adds |
| --- | --- |
| `0001_schema_v0` | `blocks`, `transfers`, `proof_txs`, `circuits`, `miners`, `proof_attempts`, `tx_observations`, `mempool_snapshots`, `ingest_cursors` |
| `0002_api_views` | views `api.blocks`, `api.transfers`, `api.proofs`, `api.circuits`, `api.addresses`, `api.address_txs`, `api.miner_shares`, `api.useful_work`, `api.stats`, `api.search_hits` |
| `0003_api_performance` | materialized statistics and indexes for the explorer |
| `0004_sync_state` | where the indexer stopped following a node, reorgs |
| `0005_events` | `node_events`, `block_observations`, `tx_rejections`, `event_gaps` |
| `0006_node_block_format` | the node's 168-byte header with proof-of-work fields |
| `0007_accounts` | `accounts` (balance, nonce from the node), `account_refresh`, `api.accounts` |
| `0008_network_stats` | `network_stats`: chain statistics per minute for Grafana |
| `0009_typed_node_id` | node ids checked like in the node |
| `0010_load` | counters on `blocks`, `network_stats` with `transfers_per_s`, `txs_per_s`, `fees`, `transfer_fees`; one index added, three unused ones dropped |

## Load

Blocks with hundreds of transfers (`make load SCENARIO=transfers` in pouw-deploy):

- the transfers of a block are written with one `INSERT ... UNNEST`, not one statement each;
- `blocks` carries the counters (`transfer_count`, `fee_total`, ...) written with the block, so
  `api.blocks` and `network_stats` do not count rows;
- balances are copied from the node 16 requests at a time, and while the indexer is behind it
  copies one batch per round, the rest waits in `account_refresh`;
- the materialized statistics are refreshed at most every 2 s (at once after a reorganisation);
- `pouw_indexer_lag_blocks` is what the `IndexerLagging` alert should watch.

`scripts/explain.sql` builds a large scratch database and prints the plans of the explorer's
queries. Run it after any change of a view or an index.

A new column or view the explorer needs: a migration here, then the Hasura
metadata and, for a new query, the contract and the allow-list in
pouw-dashboard.

## Run locally

```bash
docker compose up -d     # Postgres on 127.0.0.1:5432
export DATABASE_URL=postgres://pouw:pouw@localhost:5432/pouw_indexer
cargo run &              # migrations, then the API on 0.0.0.0:8000 (BIND_ADDR), /health
cargo run -- seed        # load tests/fixtures/blocks.borsh.jsonl
./pre_push_tests.sh      # fmt, clippy, tests
```

Against a running node: `NODE_URL=http://localhost:8000 cargo run`, with
events also `EVENT_SOURCES=node-1=http://localhost:8000`.

The tests need the database from `docker compose up -d` and `DATABASE_URL`
(`cargo test`; CI runs `cargo test --locked` against Postgres 16).

## In the network (pouw-deploy)

pouw-deploy runs the image `ghcr.io/pouw-zk/pouw-indexer` (published from
`main` by `docker.yml`) as the service `indexer`, with `NODE_URL=http://node-1:8000`
and the events of node-1..3. To run the whole devnet (3 nodes, indexer, Hasura,
explorer, Grafana) with your local changes, clone pouw-deploy next to this
repository and run `make dev` there. `make test` there also runs `e2e/run.sh`
of this repository against the running network.

The Dockerfile builds the image that pouw-deploy runs.
