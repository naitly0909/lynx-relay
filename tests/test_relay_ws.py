"""Lynx Relay streams: WebSocket codec, session failover with kept subscriptions, metering, and one real-socket round trip through the local server."""
import http.client, json, os, queue, socket, sys, tempfile, threading, time, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lynxrelay import core, server as SRV, usage as U, ws as WS, wsrelay as WSR

class Fake:
    def __init__(self): self.q = queue.Queue(); self.sent = []; self.closed = False
    def feed(self, obj): self.q.put(json.dumps(obj).encode())
    def drop(self): self.q.put(None)
    def recv_message(self):
        x = self.q.get(timeout=5)
        if x is None: raise WS.WSClosed("eof")
        return 1, x
    def send_text(self, t):
        if self.closed: raise WS.WSClosed("closed")
        self.sent.append(json.loads(t))
    def close(self): self.closed = True; self.q.put(None)
    def wait_sent(self, n, timeout=3):
        end = time.time() + timeout
        while len(self.sent) < n and time.time() < end: time.sleep(0.01)
        return self.sent

def ep(provider, prio, limit=0, period="none", cost_per_mb=0.0):
    e = core.Endpoint(provider, "solana_ws", "wss://" + provider + ".test/", 1000, limit, period, 0, ["KEY"], clock=lambda: 0, priority=prio); e.cost_per_mb = cost_per_mb; return e

class Codec(unittest.TestCase):
    def test_frames_roundtrip_ping_and_fragments(self):
        a, b = socket.socketpair(); c = WS.Conn(a, True); s = WS.Conn(b, False)
        def send(conn, text): t = threading.Thread(target=conn.send_text, args=(text,)); t.start(); return t
        for n in (0, 5, 125, 126, 70000):
            t = send(c, "x" * n); self.assertEqual(s.recv_message()[1], b"x" * n); t.join(); t = send(s, "y" * n); self.assertEqual(c.recv_message()[1], b"y" * n); t.join()
        c.send(WS.PING, b"hi"); c.send_text("after"); self.assertEqual(s.recv_message()[1], b"after"); self.assertEqual(c._frame()[1], WS.PONG)       # the ping was answered
        b.sendall(bytes([0x01, 3]) + b"abc" + bytes([0x80, 3]) + b"def"); self.assertEqual(c.recv_message(), (1, b"abcdef"))                               # fragmented message joined
        c.close(); self.assertRaises(WS.WSClosed, s.recv_message)

class SessionTests(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp(); self.usage = U.Usage(__import__("pathlib").Path(self.d) / "u.json"); self.now = 1_800_000_000.0
    def make(self, eps, ups):
        self.relay = core.Relay(eps, usage=self.usage, clock=lambda: self.now, sleep=lambda s: None); self.client = Fake(); self.connects = []
        def connect(url, timeout):
            self.connects.append(url); u = ups.pop(0) if len(ups) > 1 else ups[0]
            if u is None: raise WS.WSClosed("down")
            return u
        self.sess = WSR.Session(self.relay, self.client, "solana_ws", connect=connect, sleep=lambda s: time.sleep(0.01), retry_wait=0.01, give_up_s=0.3)
        self.th = threading.Thread(target=self.sess.run, daemon=True); self.th.start()
    def tearDown(self):
        self.client.drop(); self.th.join(3)

    def test_subscription_ids_are_rewritten_and_stable_across_a_switch(self):
        A, B = Fake(), Fake(); self.make([ep("a", 1), ep("b", 2)], [A, B])
        self.client.feed({"jsonrpc": "2.0", "id": 5, "method": "logsSubscribe", "params": [{"mentions": ["X"]}, {"commitment": "processed"}]})
        req = A.wait_sent(1)[0]; self.assertEqual(req["method"], "logsSubscribe"); A.feed({"jsonrpc": "2.0", "id": req["id"], "result": 777})
        self.assertEqual(self.client.wait_sent(1)[0], {"jsonrpc": "2.0", "id": 5, "result": 1})
        A.feed({"jsonrpc": "2.0", "method": "logsNotification", "params": {"subscription": 777, "result": {"n": 1}}}); self.assertEqual(self.client.wait_sent(2)[1]["params"]["subscription"], 1)
        A.drop()                                                                                       # upstream a dies: switch to b, subscribe again, same client id
        req2 = B.wait_sent(1)[0]; self.assertEqual((req2["method"], req2["params"][0]), ("logsSubscribe", {"mentions": ["X"]})); B.feed({"jsonrpc": "2.0", "id": req2["id"], "result": 888})
        B.feed({"jsonrpc": "2.0", "method": "logsNotification", "params": {"subscription": 888, "result": {"n": 2}}})
        got = self.client.wait_sent(3); self.assertEqual((got[2]["params"]["subscription"], got[2]["params"]["result"]), (1, {"n": 2})); self.assertEqual(len(got), 3)    # no second subscribe reply
        self.assertEqual(self.connects, ["wss://a.test/", "wss://b.test/"]); st = {s["source"]: s for s in self.relay.status()}; self.assertEqual(st["a"]["state"], "resting")
    def test_unsubscribe_and_passthrough(self):
        A = Fake(); self.make([ep("a", 1)], [A])
        self.client.feed({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe", "params": []}); req = A.wait_sent(1)[0]; A.feed({"jsonrpc": "2.0", "id": req["id"], "result": 41})
        self.client.wait_sent(1); self.client.feed({"jsonrpc": "2.0", "id": 2, "method": "logsUnsubscribe", "params": [1]}); un = A.wait_sent(2)[1]; self.assertEqual((un["method"], un["params"]), ("logsUnsubscribe", [41]))
        self.assertEqual(self.client.wait_sent(2)[1], {"jsonrpc": "2.0", "id": 2, "result": True})
        self.client.feed({"jsonrpc": "2.0", "id": 3, "method": "getSlot"}); self.assertEqual(A.wait_sent(3)[2]["method"], "getSlot")             # other requests pass through with the client's id
    def test_allowance_used_up_by_streamed_bytes_switches_source(self):
        A, B = Fake(), Fake(); self.make([ep("a", 1, 1000, "month", 1e6), ep("b", 2)], [A, B])           # 1 credit per byte, limit 1000 (98% = 980 bytes)
        self.client.feed({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe", "params": []}); req = A.wait_sent(1)[0]; A.feed({"jsonrpc": "2.0", "id": req["id"], "result": 7})
        for i in range(30): A.feed({"jsonrpc": "2.0", "method": "logsNotification", "params": {"subscription": 7, "result": {"pad": "x" * 40}}})
        req2 = B.wait_sent(1)[0]; self.assertEqual(req2["method"], "logsSubscribe"); self.assertEqual({s["source"]: s["state"] for s in self.relay.status()}["a"], "exhausted")
    def test_provider_rate_limit_on_subscribe_switches(self):
        A, B = Fake(), Fake(); self.make([ep("a", 1), ep("b", 2)], [A, B])
        self.client.feed({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe", "params": []}); req = A.wait_sent(1)[0]
        A.feed({"jsonrpc": "2.0", "id": req["id"], "error": {"code": -32005, "message": "rate limit exceeded"}}); self.assertEqual(B.wait_sent(1)[0]["method"], "logsSubscribe")
    def test_nothing_usable_closes_the_client_after_a_while(self):
        self.make([ep("a", 1)], [None]); self.th.join(5); self.assertFalse(self.th.is_alive()); self.assertTrue(self.client.closed)

class RealSockets(unittest.TestCase):
    def test_round_trip_through_the_local_server(self):
        up_srv = socket.socket(); up_srv.bind(("127.0.0.1", 0)); up_srv.listen(1); up_port = up_srv.getsockname()[1]; seen = {}
        def upstream():
            s, _ = up_srv.accept(); data = b""
            while b"\r\n\r\n" not in data: data += s.recv(4096)
            key = [l.split(":", 1)[1].strip() for l in data.decode().split("\r\n") if l.lower().startswith("sec-websocket-key")][0]
            s.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + WS.accept_key(key) + "\r\n\r\n").encode())
            c = WS.Conn(s, False); op, msg = c.recv_message(); m = json.loads(msg); seen["m"] = m
            c.send_text(json.dumps({"jsonrpc": "2.0", "id": m["id"], "result": 42})); c.send_text(json.dumps({"jsonrpc": "2.0", "method": "slotNotification", "params": {"subscription": 42, "result": {"slot": 9}}}))
            time.sleep(0.5); c.close()
        threading.Thread(target=upstream, daemon=True).start()
        e = core.Endpoint("local", "solana_ws", f"ws://127.0.0.1:{up_port}/", 1000, 0, "none", 0, [], clock=time.time, priority=1)
        d = tempfile.mkdtemp(); relay = core.Relay([e], usage=U.Usage(__import__("pathlib").Path(d) / "u.json"))
        srv = SRV.Server(("127.0.0.1", 0), SRV.make_handler(relay, 0)); port = srv.server_address[1]; srv.RequestHandlerClass = SRV.make_handler(relay, port); threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            c = WS.connect(f"ws://127.0.0.1:{port}/ws/solana"); c.send_text(json.dumps({"jsonrpc": "2.0", "id": 5, "method": "slotSubscribe", "params": []}))
            r1 = json.loads(c.recv_message()[1]); r2 = json.loads(c.recv_message()[1]); self.assertEqual(r1, {"jsonrpc": "2.0", "id": 5, "result": 1}); self.assertEqual(r2["params"]["subscription"], 1); self.assertEqual(seen["m"]["method"], "slotSubscribe")
            c.close()
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3); conn.request("GET", "/ws/solana", headers={"Origin": "https://evil.example"}); self.assertEqual(conn.getresponse().status, 403)     # a web page cannot open the stream
        finally: srv.shutdown(); srv.server_close(); up_srv.close()

class EvmSessionTests(unittest.TestCase):
    """eth_subscribe streams: hex subscription ids, stable across a switch, eth_unsubscribe, server route and catalog."""
    def setUp(self):
        self.d = tempfile.mkdtemp(); self.usage = U.Usage(__import__("pathlib").Path(self.d) / "u.json")
    def evm_ep(self, provider, prio):
        return core.Endpoint(provider, "evm_ws:eth", "wss://" + provider + ".test/", 1000, 0, "none", 0, ["KEY"], clock=lambda: 0, priority=prio)
    def test_hex_ids_stay_stable_across_a_switch_and_unsubscribe_works(self):
        A, B = Fake(), Fake(); relay = core.Relay([self.evm_ep("a", 1), self.evm_ep("b", 2)], usage=self.usage, clock=lambda: 1_800_000_000.0, sleep=lambda s: None); client = Fake(); ups = [A, B]
        sess = WSR.Session(relay, client, "evm_ws:eth", connect=lambda url, t: ups.pop(0), sleep=lambda s: time.sleep(0.01), retry_wait=0.01, give_up_s=0.3); th = threading.Thread(target=sess.run, daemon=True); th.start()
        try:
            client.feed({"jsonrpc": "2.0", "id": 3, "method": "eth_subscribe", "params": ["newHeads"]}); req = A.wait_sent(1)[0]; self.assertEqual(req["method"], "eth_subscribe")
            A.feed({"jsonrpc": "2.0", "id": req["id"], "result": "0xabc123"}); self.assertEqual(client.wait_sent(1)[0], {"jsonrpc": "2.0", "id": 3, "result": "0x1"})
            A.feed({"jsonrpc": "2.0", "method": "eth_subscription", "params": {"subscription": "0xabc123", "result": {"number": "0x10"}}}); self.assertEqual(client.wait_sent(2)[1]["params"]["subscription"], "0x1")
            A.drop(); req2 = B.wait_sent(1)[0]; self.assertEqual((req2["method"], req2["params"]), ("eth_subscribe", ["newHeads"]))
            B.feed({"jsonrpc": "2.0", "id": req2["id"], "result": "0xdef456"}); B.feed({"jsonrpc": "2.0", "method": "eth_subscription", "params": {"subscription": "0xdef456", "result": {"number": "0x11"}}})
            got = client.wait_sent(3); self.assertEqual(got[2]["params"]["subscription"], "0x1"); self.assertEqual(len(got), 3)
            client.feed({"jsonrpc": "2.0", "id": 4, "method": "eth_unsubscribe", "params": ["0x1"]}); self.assertEqual(client.wait_sent(4)[3], {"jsonrpc": "2.0", "id": 4, "result": True})
            un = B.wait_sent(2)[1]; self.assertEqual((un["method"], un["params"]), ("eth_unsubscribe", ["0xdef456"]))
        finally: client.drop(); th.join(3)
    def test_catalog_has_evm_streams_for_every_chain_without_a_key(self):
        from lynxrelay import catalog as CAT
        eps = [e for p in CAT.load()["providers"] if p["kind"] == "keyless" for e in p["endpoints"] if e["cap"].startswith("evm_ws:")]
        self.assertEqual(sorted(e["cap"] for e in eps), sorted("evm_ws:" + c for c in ("eth", "bsc", "base", "arb", "polygon", "avax", "op"))); self.assertTrue(all(e["url"].startswith("wss://") for e in eps))

if __name__ == "__main__": unittest.main()
