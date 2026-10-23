#!/usr/bin/env python3
"""
Mock pouw-node HTTP API for developing pouw-indexer. Python 3 stdlib only.

One server per node, each with its own event log and its own seq:
    node-a   http://127.0.0.1:8081
    miner-1  http://127.0.0.1:8082
    miner-2  http://127.0.0.1:8083

GET /v1/events?after=<seq>&limit=<n>&wait_ms=<ms>   (node-events.md, exact semantics)
    defaults 0 / 100 / 0, maxima none / 1000 / 30000, values above a maximum are lowered.
    Response {"events": [...], "next": <seq>}; when empty, next == after.
    Long-poll: holds the request until a new event exists or wait_ms ends.
    HTTP 410 cursor_expired when `after` is older than the oldest retained event.
GET /v1/blocks?from=<h>&to=<h>                      (PROVISIONAL shape, node-api.md not seen)
    Response {"blocks": [...]}, inclusive range.

Modes
    default  : the whole log is available at once.
    --live   : events appear over time, following their timestamps. --speed 10 = 10x faster.
               Lets you test long-poll and "caught up" behaviour.
    --retention N : keep only the last N visible events, to provoke 410 and the resync path.

Examples
    python3 mock_node.py
    python3 mock_node.py --live --speed 20
    python3 mock_node.py --retention 100
    curl 'http://127.0.0.1:8081/v1/events?after=0&limit=3'
"""
import argparse
import bisect
import json
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

NODES = ["node-a", "miner-1", "miner-2"]
DEFAULT_LIMIT, MAX_LIMIT, MAX_WAIT_MS = 100, 1000, 30000


def parse_ms(s):
    dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000) + int(s[20:23])


class NodeData:
    def __init__(self, name, events, blocks, live, speed, origin_ms, retention):
        self.name, self.events, self.blocks = name, events, blocks
        self.times = [parse_ms(e["time"]) for e in events]
        self.live, self.speed, self.origin_ms, self.retention = live, speed, origin_ms, retention
        self.t_start = time.time()
        # when this node accepted each block (blocks it never accepted are not served in live mode)
        self.accepted_ms = {e["data"]["hash"]: self.times[i]
                            for i, e in enumerate(events) if e["kind"] == "block_accepted"}

    def clock_ms(self):
        return self.origin_ms + (time.time() - self.t_start) * 1000 * self.speed

    def visible(self):
        if not self.live:
            return len(self.events)
        return bisect.bisect_right(self.times, self.clock_ms())

    def oldest_seq(self):
        if self.retention is None:
            return 1
        return max(1, self.visible() - self.retention + 1)

    def visible_blocks(self):
        if not self.live:
            return self.blocks
        now = self.clock_ms()
        return [b for b in self.blocks
                if b["header"]["height"] == 0 or self.accepted_ms.get(b["hash"], 1 << 62) <= now]


def make_handler(node):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):
            print("[%s] %s" % (node.name, fmt % args))

        def send_json(self, code, obj):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def err(self, code, ecode, msg, details=None):
            e = {"code": ecode, "message": msg}
            if details is not None:
                e["details"] = details
            self.send_json(code, {"error": e})

        def int_param(self, q, name, default, maximum=None, minimum=0):
            if name not in q:
                return default
            try:
                v = int(q[name][0])
            except ValueError:
                raise ValueError("%s must be an integer" % name)
            if v < minimum:
                raise ValueError("%s must be at least %d" % (name, minimum))
            return min(v, maximum) if maximum is not None else v

        def do_GET(self):
            u = urlparse(self.path)
            q = parse_qs(u.query)
            try:
                if u.path == "/v1/events":
                    return self.events(q)
                if u.path == "/v1/blocks":
                    return self.blocks(q)
                if u.path == "/healthz":
                    return self.send_json(200, {"node_id": node.name, "events": node.visible()})
            except ValueError as e:
                return self.err(400, "bad_request", str(e))
            self.err(404, "not_found", "no such endpoint: " + u.path)

        def events(self, q):
            after = self.int_param(q, "after", 0)
            limit = self.int_param(q, "limit", DEFAULT_LIMIT, MAX_LIMIT, minimum=1)
            wait_ms = self.int_param(q, "wait_ms", 0, MAX_WAIT_MS)
            oldest = node.oldest_seq()
            if after < oldest - 1:
                return self.err(410, "cursor_expired", "oldest available seq is %d" % oldest,
                                {"oldest_seq": oldest})
            deadline = time.time() + wait_ms / 1000
            while node.visible() <= after and time.time() < deadline:
                time.sleep(0.02)
            vis = node.visible()
            if after >= vis:
                return self.send_json(200, {"events": [], "next": after})
            chunk = node.events[after:min(vis, after + limit)]
            self.send_json(200, {"events": chunk, "next": chunk[-1]["seq"]})

        def blocks(self, q):
            lo = self.int_param(q, "from", 0)
            hi = self.int_param(q, "to", 1 << 62)
            out = [b for b in node.visible_blocks() if lo <= b["header"]["height"] <= hi]
            self.send_json(200, {"blocks": out})

    return Handler


def load_jsonl(path):
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", default=os.path.dirname(os.path.abspath(__file__)), help="fixtures directory")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--base-port", type=int, default=8081)
    ap.add_argument("--live", action="store_true", help="reveal events over time instead of all at once")
    ap.add_argument("--speed", type=float, default=1.0, help="live mode time factor")
    ap.add_argument("--retention", type=int, default=None, help="keep only the last N events (provokes 410)")
    a = ap.parse_args()

    blocks = load_jsonl(os.path.join(a.dir, "chain", "blocks.jsonl"))
    data = {n: load_jsonl(os.path.join(a.dir, "events", n + ".jsonl")) for n in NODES}
    origin = min(parse_ms(evs[0]["time"]) for evs in data.values())
    servers = []
    for i, n in enumerate(NODES):
        nd = NodeData(n, data[n], blocks, a.live, a.speed, origin, a.retention)
        srv = ThreadingHTTPServer((a.host, a.base_port + i), make_handler(nd))
        srv.daemon_threads = True
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        servers.append(srv)
        print("%-8s http://%s:%d  (%d events, %d blocks)" % (n, a.host, a.base_port + i, len(nd.events), len(blocks)))
    print("mode: %s%s" % ("live x%g" % a.speed if a.live else "static",
                          ", retention %d" % a.retention if a.retention else ""))
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        for s in servers:
            s.shutdown()


if __name__ == "__main__":
    main()
