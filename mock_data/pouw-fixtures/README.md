# pouw-node fixtures for developing pouw-indexer

Fake but contract-faithful output of three nodes (`node-a`, `miner-1`, `miner-2`)
over a 60-block chain, about 5 minutes of simulated time, 1600 events.
Built from `contracts/node-events.md` and `contracts/block.md` (branch `contracts-v1`).

## Quick start

```bash
python3 mock_node.py                 # node-a :8081, miner-1 :8082, miner-2 :8083
curl 'http://127.0.0.1:8081/v1/events?after=0&limit=5'
curl 'http://127.0.0.1:8081/v1/blocks?from=0&to=3'
```

Modes of the mock server:

| flag | what it is for |
| --- | --- |
| (none) | whole log available at once, good for replaying history into Postgres |
| `--live --speed 20` | events appear over time at 20x, tests long-poll and the caught-up state |
| `--retention 100` | node keeps only the last 100 events, tests `410 cursor_expired` and resync |

Regenerate with another chain: `python3 gen_fixtures.py --blocks 200 --seed 7`
(needs `pip install cryptography jsonschema`). The mock server needs only the stdlib.

## What is in here

| path | content | fidelity |
| --- | --- | --- |
| `events/<node>.jsonl` | one event per line, own `seq` 1,2,3,... per node, own clock | exact, every line validates against `node-events.schema.json` |
| `chain/blocks.borsh.jsonl` | every block as borsh hex | exact byte layout of block.md |
| `chain/blocks.jsonl` | every block as JSON, this is what `GET /v1/blocks` serves | **provisional**, node-api.md was not available |
| `chain/rejected_blocks.borsh.jsonl` | the 2 invalid blocks that nodes rejected, not part of the chain | exact layout |
| `meta/addresses.json` | `addr:` id to a name (alice, miner-1, rogue, ...) | helper |
| `meta/circuits.json` | the 3 registered circuits | helper |
| `meta/summary.json` | event counts and where each special scenario is | helper |
| `node-events.schema.json` | copied verbatim from node-events.md | contract |

Real, computed with the rules of block.md: txid, circuit id, block hash, proof hash,
Merkle root, bucket rule, eta chain, mask, Ed25519 signatures. `gen_fixtures.py` checks
itself against the test vectors printed in block.md before it writes anything, and they match.

Fake: proof bytes are random (they do not verify), the lottery outcome is random
(the real rule is an open point), fees and `difficulty` are made up.

## Scenarios you can find in the data

| scenario | where |
| --- | --- |
| Fork: block at height 22 is orphaned, node-a and miner-1 reorganise | `meta/summary.json` gives the hash. On node-a: `block_accepted` B, `block_received` and `block_verified` B', later `block_orphaned` B, `block_accepted` B', `block_accepted` C |
| Rejected block, `invalid_proof` (height 12) | `block_received`, `proof_verified` with `ok:false`, `block_verified` with `ok:false`, `block_rejected` |
| Rejected block, `bad_bucket` (height 33) | `block_received`, `block_rejected`, no verification events |
| Losing proofs | `lottery_result` with `won:false` on the miners, never in any block |
| Rejected transactions | `tx_rejected` with `bad_nonce`, `bad_signature`, `bad_chain_id`, `insufficient_balance`, `unknown_circuit`, plus `over_limit` and `malformed` (no `txid`) depending on the seed |
| Unanswered requests | 10 of 81 requests get `tx_received` but never reach a block (a block only takes one bucket) |
| Peer link flap | `peer_disconnected` and `peer_connected` on both miners at about 14:02:30 |
| Clock skew | node clocks differ by tens of ms, so the same moment has different `time` on each node |
| Mempool state | `mempool_snapshot` every 10 s on each node, pending proof requests grow to about 14 |

Only miners emit `proof_request_started`, `proof_produced` and `lottery_result`.
`node-a` is a non-mining full node.

## Things the spec does not say, and what I assumed

Check these with the node authors, because your indexer will see whatever the real node does.

1. **Field element prefix.** node-events.md says `0x` plus 62 hex digits. block.md says the
   JSON view has no prefix. Events use `0x` (the schema demands it). Blocks JSON also uses `0x`,
   change `FE_PREFIX_BLOCKS` in `gen_fixtures.py` if node-api.md says otherwise.
2. **Own block.** A miner emits `block_accepted` for its own block and no `block_received` or
   `block_verified`.
3. **Block verified but not accepted** (fork loser, same height): `block_received`,
   `proof_verified`, `block_verified ok:true`, and no `block_accepted`.
4. **Reorg order:** `block_orphaned` for the old block, then `block_accepted` for each block of
   the new branch from the lowest height up.
5. **`usage_counter` of a losing proof** is the counter it would have had (chain counter plus 1,
   plus winners already included for that circuit in the same block).
6. **`eta` of a losing proof** is the eta at the next free position in the block.
7. **A block with no winning proof is not produced.** Blocks with only transfers can exist.
8. **Retention:** the mock applies only the "last N events" part, not the 24 hour part.
9. **`limit=0`** is answered with 400, the spec does not say.
10. **`GET /v1/blocks` body** is `{"blocks": [...]}`, the whole shape is a guess.
11. Requester accounts only send proof requests, other accounts only transfers and circuit
    registrations. This keeps nonce order valid and is not a protocol rule.

## Not covered

Node restart (a second `node_started` and the seq continuing), `block_rejected` reasons other
than `invalid_proof` and `bad_bucket`, `cursor_expired` caused by the 24 hour rule, a node
that goes offline for a while, setup progress events (no kind exists yet), `/metrics`.
