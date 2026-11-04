"""Lynx Relay (relay/lynxrelay): failover, allowances, safety rules, config, local proxy, MCP, catalog. Fake transport, temp home, no network."""
import http.client, io, json, os, stat, sys, tempfile, threading, unittest
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "relay"))
from lynxrelay import catalog as CAT, config as CFG, core, usage as U, server as SRV, mcp as MCP

OK = json.dumps({"jsonrpc": "2.0", "id": 1, "result": 123}).encode()
def rpc(method="getSlot", rid=1, params=None): return json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params or []}).encode()
def err(code, msg): return json.dumps({"jsonrpc": "2.0", "id": 1, "error": {"code": code, "message": msg}}).encode()

class Fake:
    """transport scripted per url fragment: list of (status, body, retry_after) answers consumed in order, the last repeats; an Exception instance is raised."""
    def __init__(self, script): self.script, self.log = script, []
    def __call__(self, url, body, timeout):
        self.log.append(url)
        for frag, answers in self.script.items():
            if frag in url:
                a = answers.pop(0) if len(answers) > 1 else answers[0]
                if isinstance(a, Exception): raise a
                return a
        raise AssertionError("no script for " + url)

class Clock:
    def __init__(self, t=1_800_000_000.0): self.t = t
    def __call__(self): return self.t

def ep(provider, url, limit=0, period="none", cost=1, keys=("SECRETKEY",), prio=0, cap="solana_rpc", rps=1000):
    return core.Endpoint(provider, cap, url, rps, limit, period, cost, list(keys), clock=lambda: 0, priority=prio)

class Base(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp(); os.environ["LYNX_RELAY_HOME"] = self.d; self.clock = Clock(); self.usage = U.Usage(CFG.path().parent / "usage.json")
    def tearDown(self): os.environ.pop("LYNX_RELAY_HOME", None)
    def relay(self, script, eps, **kw): self.t = Fake(script); return core.Relay(eps, usage=self.usage, transport=self.t, clock=self.clock, sleep=lambda s: None, **kw)

class Failover(Base):
    def test_first_source_used_and_counted(self):
        r = self.relay({"a.test": [(200, OK, None)], "b.test": [(200, OK, None)]}, [ep("a", "https://a.test/?k=SECRETKEY", 1000, "month", prio=1), ep("b", "https://b.test", prio=2)])
        res = r.call("solana_rpc", rpc()); self.assertEqual((res.status, res.source), (200, "a")); self.assertEqual(self.usage.used("a", "month", self.clock.t), 1.0)
    def test_429_moves_on_rests_and_comes_back(self):
        r = self.relay({"a.test": [(429, b"", 30.0), (200, OK, None)], "b.test": [(200, OK, None)]}, [ep("a", "https://a.test", prio=1), ep("b", "https://b.test", prio=2)])
        self.assertEqual(r.call("solana_rpc", rpc()).source, "b"); self.assertEqual(r.call("solana_rpc", rpc()).source, "b")     # a is resting
        self.assertEqual(r.status()[0]["state"], "resting"); self.clock.t += 31; self.assertEqual(r.call("solana_rpc", rpc()).source, "a")
    def test_allowance_used_up_locally_and_provider_quota_error(self):
        r = self.relay({"a.test": [(200, OK, None)], "b.test": [(200, OK, None)]}, [ep("a", "https://a.test", 3, "month", prio=1), ep("b", "https://b.test", prio=2)])
        got = [r.call("solana_rpc", rpc()).source for _ in range(4)]; self.assertEqual(got, ["a", "a", "b", "b"])       # 98% of 3 calls = 2 calls
        r2 = self.relay({"a.test": [(200, err(-32005, "monthly quota exceeded"), None)], "b.test": [(200, OK, None)]}, [ep("a", "https://a.test", 1000, "month", prio=1), ep("b", "https://b.test", prio=2)])
        self.assertEqual(r2.call("solana_rpc", rpc()).source, "b"); st = r2.status()[0]; self.assertEqual(st["state"], "exhausted")
    def test_month_rollover_resets_counter(self):
        u = self.usage; u.add("a", 5, "month", self.clock.t); self.assertEqual(u.used("a", "month", self.clock.t), 5)
        self.assertEqual(u.used("a", "month", self.clock.t + 40 * 86400), 0)
        self.assertNotEqual(U.period_key("month", self.clock.t, 1), U.period_key("month", self.clock.t, 28))
    def test_auth_error_disables_and_hides_the_key(self):
        r = self.relay({"a.test": [(401, b"bad key SECRETKEY", None)], "b.test": [(200, OK, None)]}, [ep("a", "https://a.test/SECRETKEY", prio=1), ep("b", "https://b.test", prio=2)])
        res = r.call("solana_rpc", rpc()); self.assertEqual(res.source, "b"); blob = json.dumps(r.status()) + json.dumps(res.tried) + res.body.decode()
        self.assertNotIn("SECRETKEY", blob); self.assertNotIn("a.test", blob); self.assertEqual(r.status()[0]["state"], "disabled")
    def test_definitive_node_error_is_returned_not_failed_over(self):
        bad = err(-32602, "invalid params"); r = self.relay({"a.test": [(200, bad, None)], "b.test": [(200, OK, None)]}, [ep("a", "https://a.test", prio=1), ep("b", "https://b.test", prio=2)])
        res = r.call("solana_rpc", rpc()); self.assertEqual((res.source, res.body), ("a", bad)); self.assertEqual(self.t.log, ["https://a.test"])
    def test_method_not_on_plan_fails_over_and_is_remembered(self):
        r = self.relay({"a.test": [(200, err(-32601, "Method not found"), None), (200, OK, None)], "b.test": [(200, OK, None)]}, [ep("a", "https://a.test", prio=1), ep("b", "https://b.test", prio=2)])
        self.assertEqual(r.call("solana_rpc", rpc("getBlock")).source, "b"); self.t.log.clear(); self.assertEqual(r.call("solana_rpc", rpc("getBlock")).source, "b"); self.assertEqual(self.t.log, ["https://b.test"])
        self.assertEqual(r.call("solana_rpc", rpc("getSlot")).source, "a")            # other methods still use a
    def test_every_node_says_method_unknown_returns_that_answer(self):
        e1 = err(-32601, "Method not found"); r = self.relay({"a.test": [(200, e1, None)], "b.test": [(200, e1, None)]}, [ep("a", "https://a.test", prio=1), ep("b", "https://b.test", prio=2)])
        res = r.call("solana_rpc", rpc("nope")); self.assertEqual((res.status, res.body), (200, e1))
    def test_all_down_gives_clear_error_without_urls(self):
        r = self.relay({"a.test": [(503, b"", None)], "b.test": [OSError("boom a.test SECRETKEY")]}, [ep("a", "https://a.test", prio=1), ep("b", "https://b.test", prio=2)])
        res = r.call("solana_rpc", rpc()); doc = res.json(); self.assertEqual((res.status, doc["error"]["code"]), (503, -32050)); self.assertEqual([t["reason"] for t in doc["error"]["data"]["tried"]], ["unavailable", "unavailable"])
        self.assertNotIn("a.test", res.body.decode()); self.assertGreaterEqual(doc["error"]["data"]["retry_in_s"], 1)
    def test_write_methods_not_repeated_after_ambiguous_failure(self):
        r = self.relay({"a.test": [OSError("timeout")], "b.test": [(200, OK, None)]}, [ep("a", "https://a.test", prio=1), ep("b", "https://b.test", prio=2)])
        res = r.call("solana_rpc", rpc("sendTransaction", params=["tx"])); self.assertEqual(res.status, 502); self.assertEqual(self.t.log, ["https://a.test"]); self.assertIn("may or may not", res.body.decode())
        r2 = self.relay({"a.test": [(429, b"", None)], "b.test": [(200, OK, None)]}, [ep("a", "https://a.test", prio=1), ep("b", "https://b.test", prio=2)])
        self.assertEqual(r2.call("solana_rpc", rpc("sendTransaction", params=["tx"])).source, "b")                   # a clear refusal is safe to retry
    def test_batch_cost_and_invalid_input(self):
        r = self.relay({"a.test": [(200, json.dumps([{"jsonrpc": "2.0", "id": 1, "result": 1}, {"jsonrpc": "2.0", "id": 2, "result": 2}]).encode(), None)]}, [ep("a", "https://a.test", 1000, "month")])
        body = json.dumps([{"jsonrpc": "2.0", "id": 1, "method": "getSlot"}, {"jsonrpc": "2.0", "id": 2, "method": "getSlot"}]).encode()
        self.assertEqual(r.call("solana_rpc", body).status, 200); self.assertEqual(self.usage.used("a", "month", self.clock.t), 2.0)
        self.assertEqual(r.call("solana_rpc", b"not json").status, 400); self.assertEqual(r.call("evm_rpc:eth", rpc()).status, 404)
    def test_rate_bucket_skips_a_busy_source(self):
        e = ep("a", "https://a.test", prio=1, rps=1); e.bucket = core.Bucket(0.1, self.clock)
        r = self.relay({"a.test": [(200, OK, None)], "b.test": [(200, OK, None)]}, [e, ep("b", "https://b.test", prio=2)])
        self.assertEqual(r.call("solana_rpc", rpc()).source, "a"); self.assertEqual(r.call("solana_rpc", rpc()).source, "b")

class ConfigAndCatalog(Base):
    def test_catalog_is_valid_and_keyless_https(self):
        self.assertEqual(CAT.problems(CAT.load(extra=False)), [])
        bad = {"providers": [{"id": "x", "kind": "keyless", "free": {"confidence": "nope"}, "endpoints": [{"cap": "solana_rpc", "url": "http://x", "rps": 1, "limit": 0, "cost": 1, "period": "none"}]}]}
        self.assertTrue(len(CAT.problems(bad)) >= 2)
    def test_config_mode_600_env_override_and_from_config(self):
        CFG.set_key("helius", "KEY1"); self.assertEqual(stat.S_IMODE(os.stat(CFG.path()).st_mode), 0o600); self.assertTrue(CFG.permissions_ok())
        data = CAT.load(extra=False); h = next(p for p in data["providers"] if p["id"] == "helius"); self.assertEqual(CFG.key_for(h), "KEY1")
        os.environ["HELIUS_API_KEY"] = "ENVKEY"
        try: self.assertEqual(CFG.key_for(h), "ENVKEY")
        finally: del os.environ["HELIUS_API_KEY"]
        r = core.Relay.from_config(clock=self.clock); srcs = [s["source"] for s in r.status() if s["cap"] == "solana_rpc"]
        self.assertEqual(srcs[0], "helius"); self.assertIn("solana-public", srcs); self.assertNotIn("alchemy", srcs)                    # keyed first, keyless last, unconfigured keyed skipped
        CFG.set_key("chainstack", "http://not-https"); self.assertNotIn("chainstack", [s["source"] for s in core.Relay.from_config().status()])
        CFG.save({"keys": {"helius": "K"}, "order": {"solana_rpc": ["solana-public", "helius"]}}); self.assertEqual(core.Relay.from_config().active()["solana_rpc"], "solana-public")
        CFG.save({"keys": {"helius": "K"}, "disabled": ["helius"]}); self.assertNotIn("helius", [s["source"] for s in core.Relay.from_config().status()])
    def test_probe_costs_one_call_each_and_reports(self):
        r = self.relay({"a.test": [(200, OK, None)], "b.test": [(401, b"", None)]}, [ep("a", "https://a.test", 100, "month", prio=1), ep("b", "https://b.test", prio=2)])
        res = r.probe(); self.assertEqual([(x["source"], x["ok"], x["kind"]) for x in res], [("a", True, "ok"), ("b", False, "auth")])

class Proxy(Base):
    def test_http_proxy_headers_guard_and_status(self):
        r = self.relay({"a.test": [(200, OK, None)]}, [ep("a", "https://a.test/SECRETKEY", 1000, "month")])
        srv = SRV.Server(("127.0.0.1", 0), SRV.make_handler(r, 0)); port = srv.server_address[1]; srv.RequestHandlerClass = SRV.make_handler(r, port)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        try:
            def req(method, path, body=None, headers=None):
                c = http.client.HTTPConnection("127.0.0.1", port, timeout=5); c.request(method, path, body, headers or {}); x = c.getresponse(); d = x.read(); h = dict(x.getheaders()); c.close(); return x.status, d, h
            st, d, h = req("POST", "/solana", rpc()); self.assertEqual((st, json.loads(d)["result"], h.get("X-Relay-Source")), (200, 123, "a"))
            self.assertEqual(req("POST", "/solana", rpc(), {"Origin": "https://evil.example"})[0], 403)                      # a web page cannot spend the allowance
            self.assertEqual(req("POST", "/solana", rpc(), {"Host": "evil.example"})[0], 403); self.assertEqual(req("POST", "/nope", rpc())[0], 404)
            st, d, h = req("GET", "/status"); self.assertEqual(st, 200); self.assertNotIn(b"SECRETKEY", d); self.assertIn(b'"a"', d); self.assertEqual(req("GET", "/health")[0], 200)
        finally: srv.shutdown(); srv.server_close()

class Mcp(Base):
    def run_mcp(self, relay, *msgs):
        out = io.StringIO(); MCP.main(relay, io.StringIO("\n".join(json.dumps(m) for m in msgs) + "\n"), out); return [json.loads(x) for x in out.getvalue().splitlines()]
    def test_tools_and_write_methods_refused(self):
        r = self.relay({"a.test": [(200, OK, None)]}, [ep("a", "https://a.test/SECRETKEY", 1000, "month")])
        res = self.run_mcp(r, {"jsonrpc": "2.0", "id": 1, "method": "initialize"}, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
                           {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "relay_rpc", "arguments": {"chain": "solana", "method": "getSlot"}}},
                           {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "relay_rpc", "arguments": {"chain": "solana", "method": "sendTransaction"}}},
                           {"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "relay_free_tiers", "arguments": {"chain": "eth"}}},
                           {"jsonrpc": "2.0", "id": 6, "method": "tools/call", "params": {"name": "relay_status", "arguments": {}}})
        names = {t["name"] for t in res[1]["result"]["tools"]}; self.assertEqual(names, {"relay_free_tiers", "relay_status", "relay_rpc", "relay_setup"}); self.assertNotIn("add_key", names)
        self.assertEqual(json.loads(res[2]["result"]["content"][0]["text"])["response"]["result"], 123); self.assertIn("not available", res[3]["result"]["content"][0]["text"])
        self.assertIn("disclaimer", res[4]["result"]["content"][0]["text"]); self.assertNotIn("SECRETKEY", json.dumps(res))
if __name__ == "__main__": unittest.main()
