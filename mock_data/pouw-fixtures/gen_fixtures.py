#!/usr/bin/env python3
"""
Generate fixtures that imitate what a pouw-node produces, for developing pouw-indexer.

Sources of truth (pouw-docs, branch contracts-v1):
  contracts/node-events.md  -> events/<node>.jsonl   (exact envelope, kinds and schema)
  contracts/block.md        -> chain/blocks.borsh.jsonl (exact byte layout, real hashes)
  contracts/node-api.md     -> NOT SEEN. chain/blocks.jsonl is a PROVISIONAL JSON view.

Everything that is a hash (txid, circuit id, block hash, proof hash, Merkle root, eta,
mask) is computed with the real rules of block.md. Signatures are real Ed25519.
What is fake: proof bytes (random, they will not verify) and the lottery outcome
(random, the real rule is an open point in block.md).

Usage:  python3 gen_fixtures.py [--blocks 60] [--seed 42] [--out .]
"""
import argparse
import base64
import hashlib
import json
import os
import random
import struct
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

# --------------------------------------------------------------------------- constants
CHAIN_ID = 1001
NODE_VERSION = "0.1.0"
NODES = ["node-a", "miner-1", "miner-2"]
MINERS = ["miner-1", "miner-2"]
# Each node has its own clock. Events are stamped with the node's clock (spec: times of
# different nodes are not comparable).
SKEW_MS = {"node-a": 0, "miner-1": 37, "miner-2": -21}
NODE_START_OFFSET_MS = {"node-a": 0, "miner-1": 120, "miner-2": 260}
START_MS = int(datetime(2026, 10, 19, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

K = 8                      # bucket_count. The rule for k is open (pouw-docs#11).
DIFFICULTY = 1000          # placeholder, meaning is an open point
MAX_WITNESS_BYTES = 1 << 20
FEE_TRANSFER = 10          # fee coefficients are open points, these are made up
FEE_REQUEST_BASE, FEE_PER_INPUT, FEE_PER_WITNESS_KB = 20, 2, 5

SYSTEMS = {"groth16": 0, "plonk_kzg": 1, "marlin": 2}
# (system, constraint_count) of the three circuits registered at the start
CIRCUITS = [("groth16", 85_000), ("plonk_kzg", 60_000), ("marlin", 25_000)]
PROOF_BYTES = {"groth16": 128, "plonk_kzg": 928, "marlin": 1008}  # ADR-0006: 128..1008 B
PROVE_MS_PER_CONSTRAINT = {"groth16": 0.012, "plonk_kzg": 0.03, "marlin": 0.1}
VERIFY_MS = {"groth16": (2, 4), "plonk_kzg": (6, 9), "marlin": (5, 8)}  # ADR-0006: 2.4..8.7 ms

FE_PREFIX_BLOCKS = "0x"    # node-events.md says "0x" + 62 hex, block.md says no prefix. See README.

DOMAIN_TX_SIG = b"pouw-zk/tx-sig/v1"
DOMAIN_ETA = b"pouw-zk/eta/v1"
DOMAIN_MASK = b"pouw-zk/mask/v1"

MIN_BLOCK_MS, MAX_BLOCK_MS = 3000, 7000   # block interval floor (the ADR sizing example uses ~10 s)
FORK_HEIGHT = 22
ROGUE_BLOCKS = [(12, "invalid_proof"), (33, "bad_bucket")]


# --------------------------------------------------------------------------- primitives
def sha(*parts):
    h = hashlib.sha256()
    for p in parts:
        h.update(p)
    return h.digest()


def u8(x): return struct.pack("<B", x)
def u32(x): return struct.pack("<I", x)
def u64(x): return struct.pack("<Q", x)
def vec_bytes(b): return u32(len(b)) + b
def vec_items(items): return u32(len(items)) + b"".join(items)


def merkle_root(ids):
    """RFC 6962 style tree of block.md. `ids` are the 32-byte leaf ids."""
    if not ids:
        return sha(b"\x00")
    level = [sha(b"\x00", i) for i in ids]
    while len(level) > 1:
        nxt = []
        for j in range(0, len(level), 2):
            if j + 1 < len(level):
                nxt.append(sha(b"\x01", level[j], level[j + 1]))
            else:
                nxt.append(level[j])       # odd node is promoted unchanged
        level = nxt
    return level[0]


def bucket_of(txid):
    return int.from_bytes(txid[:4], "little") % K


def hash_to_field(d):
    return d[:31]


def fe(d):
    return "0x" + hash_to_field(d).hex()


def tx_id(b): return "tx:" + b.hex()
def blk_id(b): return "blk:" + b.hex()
def circ_id(b): return "circ:" + b.hex()
def addr_id(b): return "addr:" + b.hex()
def proof_id(b): return "proof:" + b.hex()
def b64(b): return base64.b64encode(b).decode()


def iso(ms):
    dt = datetime.fromtimestamp(ms // 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ".%03dZ" % (ms % 1000)


# --------------------------------------------------------------------------- tx / block layout
def make_client(name, seed=None):
    seed = seed or sha(b"pouw-fixture-client/", name.encode())
    priv = Ed25519PrivateKey.from_private_bytes(seed)
    pub = priv.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    return {"name": name, "seed": seed, "priv": priv, "pub": pub, "nonce": 0}


def make_tx(client, kind, fields, *, fee, nonce=None, chain_id=CHAIN_ID, tamper_sig=False):
    """SignedTx per block.md. borsh body, Ed25519 over DOMAIN_TX_SIG || body."""
    nonce = client["nonce"] if nonce is None else nonce
    body = u32(chain_id) + client["pub"] + u64(nonce) + u64(fee)
    if kind == "transfer":
        body += u8(0) + fields["to"] + u64(fields["amount"])
    elif kind == "proof_request":
        body += (u8(1) + fields["circuit"] + vec_items(fields["public_inputs"])
                 + vec_bytes(fields["witness"]))
    elif kind == "register_circuit":
        body += (u8(2) + u8(SYSTEMS[fields["system"]]) + u64(fields["constraint_count"])
                 + vec_bytes(fields["verifying_key"]) + vec_bytes(fields["circuit"]))
    else:
        raise ValueError(kind)
    sig = client["priv"].sign(DOMAIN_TX_SIG + body)
    if tamper_sig:
        sig = bytes([sig[0] ^ 1]) + sig[1:]
    raw = body + sig
    return {"kind": kind, "chain_id": chain_id, "sender": client["pub"],
            "sender_name": client["name"], "nonce": nonce, "fee": fee, "fields": fields,
            "body": body, "sig": sig, "raw": raw, "txid": sha(raw)}


def make_prooftx(req, usage_counter, proof):
    raw = req["raw"] + u64(usage_counter) + vec_bytes(proof)
    return {"req": req, "usage_counter": usage_counter, "proof": proof, "raw": raw,
            "hash": sha(raw)}


def make_block(height, prev, ts, miner, bucket_id, txs, proofs):
    leaves = [t["txid"] for t in txs] + [p["hash"] for p in proofs]
    root = merkle_root(leaves)
    header = (u32(1) + u64(height) + prev + u64(ts) + miner + u64(DIFFICULTY)
              + u32(K) + u32(bucket_id) + u32(len(leaves)) + root)
    assert len(header) == 136
    raw = header + vec_items([t["raw"] for t in txs]) + vec_items([p["raw"] for p in proofs])
    return {"hash": sha(header), "height": height, "prev": prev, "ts": ts, "miner": miner,
            "bucket_id": bucket_id, "txs": txs, "proofs": proofs, "root": root,
            "header_raw": header, "raw": raw}


def next_state(state, blk):
    st = {"height": blk["height"], "tip": blk["hash"], "registry": dict(state["registry"]),
          "usage": dict(state["usage"]), "nonce": dict(state["nonce"])}
    for tx in blk["txs"]:
        st["nonce"][tx["sender"]] = tx["nonce"] + 1
        if tx["kind"] == "register_circuit":
            st["registry"][tx["txid"]] = {"system": tx["fields"]["system"],
                                          "constraint_count": tx["fields"]["constraint_count"],
                                          "height": blk["height"]}
    for p in blk["proofs"]:
        r = p["req"]
        st["nonce"][r["sender"]] = r["nonce"] + 1
        st["usage"][(blk["miner"], r["fields"]["circuit"])] = p["usage_counter"]
    return st


# --------------------------------------------------------------------------- self test
def self_test():
    """Check this file against the test vectors printed in block.md."""
    c = make_client("vector", b"\x01" * 32)
    assert c["pub"].hex() == "8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c"
    tx = make_tx(c, "transfer", {"to": b"\x22" * 32, "amount": 1000}, fee=10, nonce=0, chain_id=1)
    assert len(tx["body"]) == 93 and len(tx["raw"]) == 157
    assert tx["body"].hex() == "010000008a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c00000000000000000a00000000000000002222222222222222222222222222222222222222222222222222222222222222e803000000000000"
    assert tx["sig"].hex() == ("507d5e19dca04f6961f00fd6f6cb0e31dffa042c43357bada9160696778aa4f4"
                               "5c720c67845a2251dfcd4e0fd078790dec9cd32392b651e1ecb24128960f5304")
    assert tx["txid"].hex() == "41ef667fcdab0af9fbf57805b90f90a60825c43d4a796b1f26980207e622e6f7"
    leaves = [bytes([i]) * 32 for i in (1, 2, 3, 4)]
    roots = ["6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
             "dcffe786ded16d283c663846ad0c4ff26558fccde36ca9d30b2ea19eade9fc0e",
             "3a066e0f40c6a1981ebfa60d2411625d0517ae22c2fc8c7c1784ff8a75c78565",
             "df896896c799531f1fd1e556cea26a6989ab06853bcbfdd3e4f5097a611f658f",
             "3b3c0ce45d11517a54300a196b61497c4165150d72b7782a4548e3984da771b2"]
    for n, want in enumerate(roots):
        assert merkle_root(leaves[:n]).hex() == want, "merkle root with %d leaves" % n
    assert hash_to_field(sha(b"abc")).hex() == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015"


# --------------------------------------------------------------------------- simulation
class Sim:
    def __init__(self, seed, n_blocks):
        self.rng = random.Random(seed)
        self.n_blocks = n_blocks
        self.events = {n: [] for n in NODES}      # (clock_ms, order, kind, data)
        self.mp_ops = {n: [] for n in NODES}      # (clock_ms, order, op, tx)
        self.order = 0
        self.mempool = []                         # txs not in the canonical chain
        names = ["client-reg", "alice", "bob", "req-1", "req-2", "req-3", "req-4", "pauper"]
        self.clients = {n: make_client(n) for n in names}
        self.requesters = [self.clients["req-%d" % i] for i in range(1, 5)]
        self.miner_addr = {m: sha(b"pouw-fixture-miner/", m.encode()) for m in MINERS}
        self.rogue_addr = sha(b"pouw-fixture-miner/", b"rogue")
        genesis = make_block(0, b"\x00" * 32, START_MS // 1000 - 60, b"\x00" * 32, 0, [], [])
        self.chain = [genesis]
        self.state = {"height": 0, "tip": genesis["hash"], "registry": {}, "usage": {}, "nonce": {}}
        self.ready = {m: START_MS + 4000 for m in MINERS}   # when the miner may start the next block
        self.gen_t = START_MS + 3000
        self.rogue_todo = list(ROGUE_BLOCKS)
        self.rejected_blocks = []
        self.orphaned = []
        self.fork_done = False

    # ---- event plumbing -------------------------------------------------
    def ev(self, node, t, kind, data):
        self.order += 1
        self.events[node].append((t + SKEW_MS[node], self.order, kind, data))

    def mp(self, node, t, op, tx):
        self.order += 1
        self.mp_ops[node].append((t + SKEW_MS[node], self.order, op, tx))

    # ---- clients --------------------------------------------------------
    def submit_valid(self, t, tx):
        s = self.rng.choice(NODES)
        tx["arr"] = {n: (t if n == s else t + self.rng.randint(8, 60)) for n in NODES}
        self.mempool.append(tx)
        for n in NODES:
            self.ev(n, tx["arr"][n], "tx_received",
                    {"txid": tx_id(tx["txid"]), "tx_kind": tx["kind"],
                     "sender": addr_id(tx["sender"]), "fee": tx["fee"]})
            self.mp(n, tx["arr"][n], "add", tx)

    def startup(self):
        rng = self.rng
        for n in NODES:
            t = START_MS + NODE_START_OFFSET_MS[n]
            self.ev(n, t, "node_started", {"chain_id": CHAIN_ID, "version": NODE_VERSION, "height": 0})
        for a in NODES:
            for b in NODES:
                if a != b:
                    self.ev(a, START_MS + rng.randint(900, 2200), "peer_connected", {"peer_id": "peer-" + b})
        self.ev("node-a", START_MS + 1500, "peer_connected", {"peer_id": "peer-rogue"})
        # miner-1 <-> miner-2 link flaps once
        flap = START_MS + 150_000
        self.ev("miner-1", flap, "peer_disconnected", {"peer_id": "peer-miner-2"})
        self.ev("miner-2", flap + 41, "peer_disconnected", {"peer_id": "peer-miner-1"})
        self.ev("miner-1", flap + 3000, "peer_connected", {"peer_id": "peer-miner-2"})
        self.ev("miner-2", flap + 3052, "peer_connected", {"peer_id": "peer-miner-1"})
        # circuit registrations
        reg = self.clients["client-reg"]
        for i, (system, cons) in enumerate(CIRCUITS):
            fields = {"system": system, "constraint_count": cons,
                      "verifying_key": rng.randbytes(384), "circuit": rng.randbytes(4096)}
            tx = make_tx(reg, "register_circuit", fields, fee=100 + cons // 1000)
            reg["nonce"] += 1
            self.submit_valid(START_MS + 2500 + 100 * i, tx)

    def other_pub(self, client):
        others = [c["pub"] for c in self.clients.values() if c is not client and c["name"] != "pauper"]
        return self.rng.choice(others)

    def gen_arrivals(self, upto):
        rng = self.rng
        t = self.gen_t
        while True:
            t += max(1, int(rng.expovariate(0.7) * 1000))
            if t > upto:
                break
            r = rng.random()
            if r < 0.07:
                self.submit_rejected(t)
            elif r < 0.40 or not self.state["registry"]:
                self.make_transfer(t)
            else:
                self.make_request(t)
        self.gen_t = max(self.gen_t, upto)

    def make_transfer(self, t):
        c = self.clients[self.rng.choice(["alice", "bob", "client-reg"])]
        tx = make_tx(c, "transfer", {"to": self.other_pub(c), "amount": self.rng.randint(1, 500)},
                     fee=FEE_TRANSFER)
        c["nonce"] += 1
        self.submit_valid(t, tx)

    def make_request(self, t):
        rng = self.rng
        pending = Counter(tx["sender"] for tx in self.mempool)
        free = [c for c in self.requesters if pending[c["pub"]] < 3]
        if not free:
            return self.make_transfer(t)
        c = rng.choice(free)
        circuit = rng.choice(sorted(self.state["registry"]))
        n_inputs = rng.randint(1, 4)
        wsize = rng.choice([64, 128, 256, 512, 1024, 2048, 4096, 4096, 32768])
        fields = {"circuit": circuit,
                  "public_inputs": [rng.randbytes(31) for _ in range(n_inputs)],
                  "witness": rng.randbytes(wsize)}
        fee = FEE_REQUEST_BASE + FEE_PER_INPUT * n_inputs + FEE_PER_WITNESS_KB * ((wsize + 1023) // 1024)
        tx = make_tx(c, "proof_request", fields, fee=fee)
        c["nonce"] += 1
        self.submit_valid(t, tx)

    def submit_rejected(self, t):
        rng = self.rng
        kind = rng.choice(["bad_nonce", "bad_signature", "bad_chain_id", "insufficient_balance",
                           "unknown_circuit", "over_limit", "malformed"])
        node = rng.choice(NODES)
        data = {"reason_code": kind}
        alice, bob, pauper = self.clients["alice"], self.clients["bob"], self.clients["pauper"]
        req = rng.choice(self.requesters)
        if kind == "bad_nonce":
            tx = make_tx(alice, "transfer", {"to": bob["pub"], "amount": 5}, fee=FEE_TRANSFER,
                         nonce=alice["nonce"] + 3)
            data["message"] = "expected nonce %d, got %d" % (alice["nonce"], alice["nonce"] + 3)
        elif kind == "bad_signature":
            tx = make_tx(bob, "transfer", {"to": alice["pub"], "amount": 5}, fee=FEE_TRANSFER, tamper_sig=True)
            data["message"] = "signature verification failed"
        elif kind == "bad_chain_id":
            tx = make_tx(alice, "transfer", {"to": bob["pub"], "amount": 5}, fee=FEE_TRANSFER, chain_id=1002)
            data["message"] = "expected chain id %d, got 1002" % CHAIN_ID
        elif kind == "insufficient_balance":
            tx = make_tx(pauper, "transfer", {"to": alice["pub"], "amount": 500}, fee=FEE_TRANSFER)
            data["message"] = "balance 0 does not cover amount 500 plus fee %d" % FEE_TRANSFER
        elif kind == "unknown_circuit":
            fake = rng.randbytes(32)
            tx = make_tx(req, "proof_request",
                         {"circuit": fake, "public_inputs": [rng.randbytes(31)], "witness": rng.randbytes(64)}, fee=30)
            data["message"] = "circuit %s is not registered" % circ_id(fake)
        elif kind == "over_limit":
            circuit = rng.choice(sorted(self.state["registry"])) if self.state["registry"] else rng.randbytes(32)
            size = MAX_WITNESS_BYTES + 1
            tx = make_tx(req, "proof_request",
                         {"circuit": circuit, "public_inputs": [rng.randbytes(31)], "witness": rng.randbytes(size)}, fee=30)
            data["message"] = "witness of %d bytes exceeds MAX_WITNESS_BYTES %d" % (size, MAX_WITNESS_BYTES)
        else:  # malformed: bytes cannot be parsed, so no txid
            tx = None
            data["message"] = "unexpected end of input while decoding TxBody"
        if tx is not None:
            data = {"txid": tx_id(tx["txid"]), **data}
        self.ev(node, t + rng.randint(1, 4), "tx_rejected", data)

    # ---- mining ---------------------------------------------------------
    def build_block(self, miner, t0, state, *, view, max_attempts=4, target_finish=None,
                    silent=False, miner_addr=None):
        """Simulate one miner building a block on `state`. Emits the miner-side events
        (proof_request_started, proof_produced, lottery_result) unless silent."""
        rng = self.rng
        addr = miner_addr or self.miner_addr[miner]
        visible = [tx for tx in self.mempool if tx["arr"][view] <= t0]
        by_sender = defaultdict(list)
        for tx in visible:
            by_sender[tx["sender"]].append(tx)
        txs, chains = [], []
        for s, lst in by_sender.items():
            lst.sort(key=lambda x: x["nonce"])
            exp = state["nonce"].get(s, 0)
            if lst[0]["kind"] == "proof_request":          # requester accounts only send requests
                chain = []
                for tx in lst:
                    if tx["nonce"] != exp:
                        break
                    chain.append(tx)
                    exp += 1
                if chain:
                    chains.append(chain)
            else:                                          # other accounts: transfers, registrations
                for tx in lst:
                    if tx["nonce"] != exp or tx["kind"] == "proof_request":
                        break
                    txs.append(tx)
                    exp += 1
        txs.sort(key=lambda x: (x["arr"][view], x["nonce"]))

        if chains:
            heads = [c[0] for c in chains]
            head = min(heads, key=lambda x: x["arr"][view]) if rng.random() < 0.5 else rng.choice(heads)
            bucket = bucket_of(head["txid"])
        else:
            bucket = rng.randrange(K)
        cands = []
        for c in chains:
            for tx in c:
                if bucket_of(tx["txid"]) != bucket:
                    break
                cands.append(tx)
        cands.sort(key=lambda x: (x["arr"][view], x["nonce"]))
        cands = cands[:max_attempts]

        plan, lost = [], set()
        for tx in cands:
            if tx["sender"] in lost:
                continue
            win = rng.random() < 0.6          # the real lottery rule is an open point
            if not win:
                lost.add(tx["sender"])
            plan.append([tx, win])
        if plan and not any(w for _, w in plan):
            plan[-1][1] = True                 # a block needs at least one winning proof

        durs = []
        for tx, _ in plan:
            info = state["registry"][tx["fields"]["circuit"]]
            durs.append(max(20, int(info["constraint_count"] * PROVE_MS_PER_CONSTRAINT[info["system"]]
                                    * rng.uniform(0.85, 1.15))))
        t_start = t0 + rng.randint(5, 15)
        if target_finish is not None and durs:
            want = max(60 * len(durs), target_finish - t_start - 3 * len(durs) - 5)
            scale = want / sum(durs)
            durs = [max(20, int(d * scale)) for d in durs]

        txs_root = merkle_root([x["txid"] for x in txs])
        eta = sha(DOMAIN_ETA, state["tip"], txs_root)
        included, cnt, t = [], defaultdict(int), t_start
        for (tx, win), dur in zip(plan, durs):
            circ = tx["fields"]["circuit"]
            system = state["registry"][circ]["system"]
            if not silent:
                self.ev(miner, t, "proof_request_started",
                        {"txid": tx_id(tx["txid"]), "circuit": circ_id(circ), "system": system})
            t += dur
            counter = state["usage"].get((addr, circ), 0) + cnt[circ] + 1
            proof = rng.randbytes(PROOF_BYTES[system])
            mask = sha(DOMAIN_MASK, addr, circ, u64(counter))
            ptx = make_prooftx(tx, counter, proof)
            ptx.update(eta=eta, mask=mask, system=system)
            if not silent:
                self.ev(miner, t, "proof_produced",
                        {"txid": tx_id(tx["txid"]), "circuit": circ_id(circ), "system": system,
                         "prove_ms": dur, "proof_hash": proof_id(ptx["hash"])})
            t += 2
            if not silent:
                self.ev(miner, t, "lottery_result",
                        {"txid": tx_id(tx["txid"]), "proof_hash": proof_id(ptx["hash"]),
                         "circuit": circ_id(circ), "won": win, "eta": fe(eta), "mask": fe(mask),
                         "usage_counter": counter})
            t += 1
            if win:
                included.append(ptx)
                cnt[circ] += 1
                eta = sha(DOMAIN_ETA, eta, sha(proof))
        t_build = t + 5
        if target_finish is not None:
            if not durs:
                t_build = max(t_build, target_finish)
        else:
            # block interval floor: a miner does not publish faster than the network's pace
            t_build = max(t_build, t_start + rng.randint(MIN_BLOCK_MS, MAX_BLOCK_MS))
        blk = make_block(state["height"] + 1, state["tip"], t_build // 1000, addr, bucket, txs, included)
        return {"block": blk, "t_build": t_build, "bucket": bucket, "txs": txs, "included": included,
                "addr": addr}

    # ---- block propagation ----------------------------------------------
    def node_receive(self, node, blk, from_peer, t_recv, invalid_at=None):
        rng = self.rng
        self.ev(node, t_recv, "block_received",
                {"hash": blk_id(blk["hash"]), "height": blk["height"], "from_peer": from_peer})
        t, total, checked, ok = t_recv + rng.randint(1, 3), 0, 0, True
        for i, p in enumerate(blk["proofs"]):
            lo, hi = VERIFY_MS[p["system"]]
            v = rng.randint(lo, hi)
            t += v
            total += v
            checked += 1
            good = not (invalid_at is not None and i == invalid_at)
            self.ev(node, t, "proof_verified",
                    {"txid": tx_id(p["req"]["txid"]), "proof_hash": proof_id(p["hash"]),
                     "system": p["system"], "verify_ms": v, "ok": good})
            if not good:
                ok = False
                break
        over = rng.randint(3, 12)
        t += over
        self.ev(node, t, "block_verified",
                {"hash": blk_id(blk["hash"]), "height": blk["height"], "verify_ms": total + over,
                 "proof_count": checked, "ok": ok})
        return t, ok

    def node_accept(self, node, blk, t):
        self.ev(node, t, "block_accepted",
                {"hash": blk_id(blk["hash"]), "height": blk["height"], "miner": addr_id(blk["miner"]),
                 "tx_count": len(blk["txs"]), "proof_count": len(blk["proofs"]),
                 "bucket_count": K, "bucket_id": blk["bucket_id"]})
        for tx in blk["txs"] + [p["req"] for p in blk["proofs"]]:
            self.mp(node, t, "rm", tx)

    def node_orphan(self, node, blk, t):
        self.ev(node, t, "block_orphaned", {"hash": blk_id(blk["hash"]), "height": blk["height"]})
        for tx in blk["txs"] + [p["req"] for p in blk["proofs"]]:
            self.mp(node, t, "add", tx)

    def commit(self, blk):
        self.state = next_state(self.state, blk)
        self.chain.append(blk)
        gone = {tx["txid"] for tx in blk["txs"]} | {p["req"]["txid"] for p in blk["proofs"]}
        self.mempool = [tx for tx in self.mempool if tx["txid"] not in gone]

    def deliver_normal(self, blk, producer, t_build):
        rng = self.rng
        acc = {producer: t_build + rng.randint(1, 3)}
        self.node_accept(producer, blk, acc[producer])
        for n in NODES:
            if n == producer:
                continue
            t_done, _ = self.node_receive(n, blk, "peer-" + producer, t_build + rng.randint(15, 70))
            acc[n] = t_done + rng.randint(1, 3)
            self.node_accept(n, blk, acc[n])
        for m in MINERS:
            self.ready[m] = acc[m]
        return acc

    # ---- scenarios ------------------------------------------------------
    def run_normal(self):
        rng = self.rng
        miner = rng.choice(MINERS)
        t0 = self.ready[miner]
        self.gen_arrivals(t0)
        res = self.build_block(miner, t0, self.state, view=miner)
        self.maybe_rogue(res)
        self.deliver_normal(res["block"], miner, res["t_build"])
        self.commit(res["block"])

    def maybe_rogue(self, honest):
        """Inject a block from an outside peer that every node rejects. Never part of the chain."""
        if not self.rogue_todo or self.state["height"] + 1 < self.rogue_todo[0][0]:
            return
        kind = self.rogue_todo[0][1]
        rng = self.rng
        r = self.build_block("rogue", honest["t_build"] - 1, self.state, view="node-a", max_attempts=2,
                             silent=True, miner_addr=self.rogue_addr)
        if not r["included"]:
            return                                          # try again at the next height
        blk = r["block"]
        if kind == "bad_bucket":
            blk = make_block(blk["height"], blk["prev"], blk["ts"], blk["miner"], (r["bucket"] + 1) % K,
                             r["txs"], r["included"])
        self.rogue_todo.pop(0)
        for n in NODES:
            t_recv = honest["t_build"] + rng.randint(100, 400)
            peer = "peer-rogue" if n == "node-a" else "peer-node-a"
            if kind == "invalid_proof":
                t_done, _ = self.node_receive(n, blk, peer, t_recv, invalid_at=len(blk["proofs"]) - 1)
                self.ev(n, t_done + 1, "block_rejected",
                        {"hash": blk_id(blk["hash"]), "height": blk["height"], "reason_code": "invalid_proof"})
            else:
                self.ev(n, t_recv, "block_received",
                        {"hash": blk_id(blk["hash"]), "height": blk["height"], "from_peer": peer})
                self.ev(n, t_recv + rng.randint(1, 3), "block_rejected",
                        {"hash": blk_id(blk["hash"]), "height": blk["height"], "reason_code": "bad_bucket"})
        self.rejected_blocks.append({"kind": kind, "block": blk})

    def run_fork(self):
        """Two miners find a block at the same height. node-a and miner-1 take miner-1's block
        first, then reorganise when miner-2 extends its own block."""
        rng = self.rng
        m1, m2 = "miner-1", "miner-2"
        t0_1, t0_2 = self.ready[m1], self.ready[m2]
        self.gen_arrivals(max(t0_1, t0_2))
        r1 = self.build_block(m1, t0_1, self.state, view=m1, max_attempts=1)
        target = max(r1["t_build"] + rng.randint(10, 40), t0_2 + 30)
        r2 = self.build_block(m2, t0_2, self.state, view=m2, max_attempts=1, target_finish=target)
        B, B2 = r1["block"], r2["block"]
        T1, T2 = r1["t_build"], r2["t_build"]
        assert B["height"] == B2["height"] and B["hash"] != B2["hash"]

        # B: miner-1 accepts, node-a accepts (first seen), miner-2 already has B2 -> verifies only
        acc1 = T1 + rng.randint(1, 3)
        self.node_accept(m1, B, acc1)
        ta_done, _ = self.node_receive("node-a", B, "peer-" + m1, T1 + rng.randint(15, 40))
        acc_a_B = ta_done + 1
        self.node_accept("node-a", B, acc_a_B)
        self.node_receive(m2, B, "peer-" + m1, max(T1 + rng.randint(15, 70), T2 + 25))
        # B2: miner-2 accepts, the others verify but keep B (same height, first seen wins)
        acc2 = T2 + rng.randint(1, 3)
        self.node_accept(m2, B2, acc2)
        self.node_receive("node-a", B2, "peer-" + m2, max(T2 + rng.randint(15, 40), acc_a_B + 10))
        self.node_receive(m1, B2, "peer-" + m2, T2 + rng.randint(15, 70))

        # miner-2 extends B2, the chain through B2 wins
        self.commit(B2)
        t0_c = acc2
        self.gen_arrivals(t0_c)
        rc = self.build_block(m2, t0_c, self.state, view=m2)
        C = rc["block"]
        Tc = rc["t_build"]
        accC = {m2: Tc + rng.randint(1, 3)}
        self.node_accept(m2, C, accC[m2])
        for n in ("node-a", m1):
            t_done, _ = self.node_receive(n, C, "peer-" + m2, Tc + rng.randint(15, 70))
            self.node_orphan(n, B, t_done + 1)
            self.node_accept(n, B2, t_done + 2)
            self.node_accept(n, C, t_done + 3)
            accC[n] = t_done + 3
        self.commit(C)
        for m in MINERS:
            self.ready[m] = accC[m]
        self.orphaned.append(B)
        self.fork_done = True

    def run(self):
        self.startup()
        while self.state["height"] < self.n_blocks:
            h = self.state["height"] + 1
            if (not self.fork_done and h >= FORK_HEIGHT and self.n_blocks - h >= 2):
                self.run_fork()
            else:
                self.run_normal()
        self.build_snapshots()

    def build_snapshots(self):
        end = max(e[0] for n in NODES for e in self.events[n])
        for n in NODES:
            ops = sorted(self.mp_ops[n], key=lambda o: (o[0], o[1]))
            pend, i, k = {}, 0, 1
            start_clock = START_MS + NODE_START_OFFSET_MS[n] + SKEW_MS[n]
            while True:
                T = start_clock + 10_000 * k
                k += 1
                if T > end:
                    break
                while i < len(ops) and ops[i][0] <= T:
                    _, _, op, tx = ops[i]
                    i += 1
                    if op == "add":
                        pend[tx["txid"]] = tx
                    else:
                        pend.pop(tx["txid"], None)
                by = Counter(circ_id(tx["fields"]["circuit"]) for tx in pend.values()
                             if tx["kind"] == "proof_request")
                self.order += 1
                self.events[n].append((T, self.order, "mempool_snapshot",
                                       {"pending_txs": len(pend), "pending_proof_requests": sum(by.values()),
                                        "by_circuit": dict(sorted(by.items()))}))


# --------------------------------------------------------------------------- output
def tx_json(tx):
    f, body = tx["fields"], {"chain_id": tx["chain_id"], "sender": addr_id(tx["sender"]),
                             "nonce": tx["nonce"], "fee": tx["fee"], "kind": tx["kind"]}
    if tx["kind"] == "transfer":
        body.update(to=addr_id(f["to"]), amount=f["amount"])
    elif tx["kind"] == "proof_request":
        body.update(circuit=circ_id(f["circuit"]),
                    public_inputs=[FE_PREFIX_BLOCKS + x.hex() for x in f["public_inputs"]],
                    witness=b64(f["witness"]))
    else:
        body.update(system=f["system"], constraint_count=f["constraint_count"],
                    verifying_key=b64(f["verifying_key"]), circuit=b64(f["circuit"]))
    return {"txid": tx_id(tx["txid"]), "body": body, "signature": b64(tx["sig"])}


def block_json(blk):
    return {
        "hash": blk_id(blk["hash"]),
        "header": {"version": 1, "height": blk["height"], "prev_hash": blk_id(blk["prev"]),
                   "timestamp": blk["ts"], "miner": addr_id(blk["miner"]), "difficulty": DIFFICULTY,
                   "bucket_count": K, "bucket_id": blk["bucket_id"],
                   "tx_count": len(blk["txs"]) + len(blk["proofs"]), "tx_root": blk["root"].hex()},
        "txs": [tx_json(t) for t in blk["txs"]],
        "proof_txs": [{"proof_hash": proof_id(p["hash"]), "request": tx_json(p["req"]),
                       "usage_counter": p["usage_counter"], "proof": b64(p["proof"]),
                       "eta": FE_PREFIX_BLOCKS + hash_to_field(p["eta"]).hex(),
                       "mask": FE_PREFIX_BLOCKS + hash_to_field(p["mask"]).hex()}
                      for p in blk["proofs"]],
        "size_bytes": len(blk["raw"]),
    }


def write_outputs(sim, out):
    for d in ("events", "chain", "meta"):
        os.makedirs(os.path.join(out, d), exist_ok=True)
    counts = {}
    for n in NODES:
        evs = sorted(sim.events[n], key=lambda e: (e[0], e[1]))
        counts[n] = Counter(e[2] for e in evs)
        with open(os.path.join(out, "events", n + ".jsonl"), "w") as f:
            for seq, (t, _, kind, data) in enumerate(evs, 1):
                f.write(json.dumps({"seq": seq, "time": iso(t), "node_id": n, "kind": kind, "data": data}) + "\n")
    with open(os.path.join(out, "chain", "blocks.jsonl"), "w") as f:
        for b in sim.chain:
            f.write(json.dumps(block_json(b)) + "\n")
    with open(os.path.join(out, "chain", "blocks.borsh.jsonl"), "w") as f:
        for b in sim.chain:
            f.write(json.dumps({"height": b["height"], "hash": blk_id(b["hash"]),
                                "borsh_hex": b["raw"].hex()}) + "\n")
    with open(os.path.join(out, "chain", "rejected_blocks.borsh.jsonl"), "w") as f:
        for r in sim.rejected_blocks:
            b = r["block"]
            f.write(json.dumps({"height": b["height"], "hash": blk_id(b["hash"]), "reason": r["kind"],
                                "borsh_hex": b["raw"].hex()}) + "\n")
    names = {addr_id(c["pub"]): c["name"] for c in sim.clients.values()}
    names.update({addr_id(a): m for m, a in sim.miner_addr.items()})
    names[addr_id(sim.rogue_addr)] = "rogue"
    json.dump(names, open(os.path.join(out, "meta", "addresses.json"), "w"), indent=2)
    circuits = {circ_id(cid): {"system": i["system"], "constraint_count": i["constraint_count"],
                               "registered_at_height": i["height"]}
                for cid, i in sim.state["registry"].items()}
    json.dump(circuits, open(os.path.join(out, "meta", "circuits.json"), "w"), indent=2)
    summary = {
        "chain_id": CHAIN_ID, "blocks": len(sim.chain) - 1, "bucket_count": K,
        "nodes": NODES, "event_counts": {n: dict(c) for n, c in counts.items()},
        "scenarios": {
            "fork": {"orphaned_block": blk_id(sim.orphaned[0]["hash"]), "height": sim.orphaned[0]["height"]} if sim.orphaned else None,
            "rejected_blocks": [{"hash": blk_id(r["block"]["hash"]), "height": r["block"]["height"],
                                 "reason": r["kind"]} for r in sim.rejected_blocks],
            "peer_flap": "miner-1 <-> miner-2 at about 2026-10-19T14:02:30Z",
        },
        "tx_rejected_reasons": dict(Counter(e[3].get("reason_code") for n in NODES for e in sim.events[n]
                                            if e[2] == "tx_rejected")),
    }
    json.dump(summary, open(os.path.join(out, "meta", "summary.json"), "w"), indent=2)
    return summary


def validate(out, sim):
    """Independent re-read of the written files."""
    import jsonschema
    schema = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "node-events.schema.json")))
    v = jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker())
    problems, total = [], 0
    for n in NODES:
        last_t = None
        for i, line in enumerate(open(os.path.join(out, "events", n + ".jsonl")), 1):
            e = json.loads(line)
            total += 1
            errs = list(v.iter_errors(e))
            if errs:
                problems.append("%s seq %d: %s" % (n, i, errs[0].message[:120]))
            if e["seq"] != i:
                problems.append("%s: seq gap at line %d" % (n, i))
            if last_t and e["time"] < last_t:
                problems.append("%s: time goes backwards at seq %d" % (n, i))
            last_t = e["time"]
    hashes = set()
    for line in open(os.path.join(out, "chain", "blocks.borsh.jsonl")):
        r = json.loads(line)
        raw = bytes.fromhex(r["borsh_hex"])
        if "blk:" + sha(raw[:136]).hex() != r["hash"]:
            problems.append("block hash mismatch at height %d" % r["height"])
        hashes.add(r["hash"])
    accepted = {json.loads(l)["data"]["hash"] for l in open(os.path.join(out, "events", "node-a.jsonl"))
                if json.loads(l)["kind"] == "block_accepted"}
    orphaned = {blk_id(b["hash"]) for b in sim.orphaned}
    missing = accepted - hashes - orphaned
    if missing:
        problems.append("node-a accepted blocks that are not in chain/: %d" % len(missing))
    return total, problems


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--blocks", type=int, default=60)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=os.path.dirname(os.path.abspath(__file__)))
    a = ap.parse_args()
    self_test()
    print("self-test: matches the test vectors in block.md")
    sim = Sim(a.seed, a.blocks)
    sim.run()
    summary = write_outputs(sim, a.out)
    total, problems = validate(a.out, sim)
    print("wrote %d events for %s and %d blocks to %s" % (total, ", ".join(NODES), summary["blocks"], a.out))
    for n, c in summary["event_counts"].items():
        print("  %-8s %5d events  %s" % (n, sum(c.values()), dict(sorted(c.items()))))
    if problems:
        print("VALIDATION PROBLEMS:")
        for p in problems[:20]:
            print("  -", p)
        sys.exit(1)
    print("validation: every event matches node-events.schema.json, seq has no gaps, block hashes recompute")


if __name__ == "__main__":
    main()
