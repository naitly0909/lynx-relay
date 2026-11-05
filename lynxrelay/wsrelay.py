"""Stream relay: ONE client WebSocket (your bot) in front of whichever free provider is active. Your subscriptions are remembered; when the upstream connection drops, errors out or its allowance is used up,
the session connects to the next source and subscribes again, so the client keeps the same subscription ids and just sees a short gap.
Messages are forwarded as they are, except that subscription ids are replaced by stable ids of this relay. Every upstream message is metered (bytes x cost per MB of the endpoint).
Not repeated or replayed: requests that were in flight at the moment of a switch (the client should retry them) and messages missed during the gap."""
import json, threading, time
from . import ws as WS

def _is_sub(method): return isinstance(method, str) and (method.endswith("Subscribe") or method == "eth_subscribe")
def _is_unsub(method): return isinstance(method, str) and (method.endswith("Unsubscribe") or method == "eth_unsubscribe")

class Session:
    def __init__(self, relay, client, cap="solana_ws", connect=WS.connect, sleep=time.sleep, retry_wait=1.0, give_up_s=120.0, clock=time.time, ping_s=30.0):
        self.relay, self.client, self.cap, self.connect, self.sleep, self.retry_wait, self.give_up_s, self.clock, self.ping_s = relay, client, cap, connect, sleep, retry_wait, give_up_s, clock, ping_s
        self.evm = cap.startswith("evm_ws")          # EVM subscription ids are hex strings (eth_subscribe); Solana ids are numbers
        self.lock = threading.RLock(); self.up = None; self.ep = None; self.done = threading.Event(); self.connected_at = 0.0
        self.subs = {}; self.upmap = {}; self.pending = {}; self.dropped = {}; self.next_cid = 1; self.next_req = 10 ** 9; self.switches = 0; self.failback_s = 300.0

    # ---- helpers ----
    def _to_client(self, obj):
        try: self.client.send_text(json.dumps(obj, separators=(",", ":")))
        except WS.WSClosed: self.done.set()
    def _to_up(self, obj):
        with self.lock:
            if self.up is None: return False
            try: self.up.send_text(json.dumps(obj, separators=(",", ":"))); return True
            except WS.WSClosed: return False
    def _out(self, cid): return "0x%x" % cid if self.evm else cid          # the stable id the client sees
    def _in(self, x):
        if isinstance(x, str) and x.startswith("0x"):
            try: return int(x, 16)
            except ValueError: return None
        return x if isinstance(x, int) and not isinstance(x, bool) else None
    def _req_id(self):
        with self.lock: self.next_req += 1; return self.next_req

    # ---- client side ----
    def _client_loop(self):
        while not self.done.is_set():
            try: op, data = self.client.recv_message()
            except WS.WSClosed: break
            try: m = json.loads(data)
            except ValueError: continue
            if isinstance(m, dict): self._client_message(m)
        self.done.set()
        with self.lock:
            if self.up is not None: self.up.close()

    def _client_message(self, m):
        method = m.get("method")
        if _is_sub(method):
            with self.lock:
                cid = self.next_cid; self.next_cid += 1
                self.subs[cid] = {"method": method, "params": m.get("params") or [], "up": None, "client_id": m.get("id")}
                self._send_subscription(cid)
        elif _is_unsub(method):
            params = m.get("params"); cid = self._in(params[0]) if isinstance(params, list) and params else None
            with self.lock:
                sub = self.subs.pop(cid, None)
                if sub and sub["up"] is None: self.dropped[cid] = method      # the upstream id is not known yet: end it when the answer comes
            if sub and sub["up"] is not None:
                with self.lock: self.upmap.pop(sub["up"], None)
                r = self._req_id(); self.pending[r] = ("unsub", cid); self._to_up({"jsonrpc": "2.0", "id": r, "method": method, "params": [sub["up"]]})
            self._to_client({"jsonrpc": "2.0", "id": m.get("id"), "result": bool(sub)})
        else: self._to_up(m)      # anything else is passed through untouched (replies come back with the client's own ids)

    def _send_subscription(self, cid):
        with self.lock: sub = self.subs.get(cid)
        if not sub: return
        r = self._req_id(); self.pending[r] = ("sub", cid)
        self._to_up({"jsonrpc": "2.0", "id": r, "method": sub["method"], "params": sub["params"]})

    # ---- upstream side ----
    def _open_upstream(self):
        """Connect to the first usable source that answers and subscribe again. -> True when connected."""
        while True:
            ep = self.relay.pick_ws(self.cap)          # resting, used-up, disabled sources are skipped there, so a source that just failed is not picked again
            if ep is None: return False
            try: up = self.connect(ep.url, 10.0); break
            except WS.WSClosed: self.relay.ws_failed(ep, "unavailable")
        with self.lock:
            self.up, self.ep, self.upmap, self.pending = up, ep, {}, {}
            for cid in list(self.subs): self._send_subscription(cid)
        self.switches += 1; self.relay.ws_ok(ep); self.connected_at = self.clock()
        return True

    def _upstream_message(self, data):
        try: m = json.loads(data)
        except ValueError: return self._to_client_raw(data)
        if not isinstance(m, dict): return self._to_client_raw(data)
        rid = m.get("id")
        if isinstance(rid, int) and rid in self.pending:
            kind, cid = self.pending.pop(rid)
            if kind == "unsub": return
            sub = self.subs.get(cid)
            if sub is None:
                with self.lock: unsub = self.dropped.pop(cid, None)
                if unsub and isinstance(m.get("result"), (int, str)) and not isinstance(m.get("result"), bool):
                    r = self._req_id(); self.pending[r] = ("unsub", cid); self._to_up({"jsonrpc": "2.0", "id": r, "method": unsub, "params": [m["result"]]})
                return
            if isinstance(m.get("result"), (int, str)) and not isinstance(m.get("result"), bool) and m.get("result") != "":
                sub["up"] = m["result"]; self.upmap[m["result"]] = cid
                if sub.get("client_id") is not None: self._to_client({"jsonrpc": "2.0", "id": sub["client_id"], "result": self._out(cid)}); sub["client_id"] = None
            elif m.get("error") is not None:
                kind2 = self.relay._classify(200, data, self.ep)[0]
                if kind2 in ("rate_limited", "quota", "auth", "unsupported"): raise WS.WSClosed("provider refused: " + kind2)       # switch source, subscriptions are kept
                self.subs.pop(cid, None)
                if sub.get("client_id") is not None: self._to_client({"jsonrpc": "2.0", "id": sub["client_id"], "error": m["error"]})
            return
        params = m.get("params")
        if isinstance(params, dict) and isinstance(params.get("subscription"), (int, str)) and params["subscription"] in self.upmap: params["subscription"] = self._out(self.upmap[params["subscription"]])
        self._to_client(m)

    def _to_client_raw(self, data):
        try: self.client.send_text(data.decode("utf-8", "replace"))
        except WS.WSClosed: self.done.set()

    def _heartbeat(self):
        """Pings keep a quiet subscription alive and make a dead connection time out (ws.IDLE_TIMEOUT), which starts the switch to the next source."""
        while not self.done.wait(self.ping_s):
            with self.lock: up = self.up
            if up is None: continue
            try: up.ping()
            except WS.WSClosed: pass

    def run(self):
        threading.Thread(target=self._client_loop, daemon=True).start(); threading.Thread(target=self._heartbeat, daemon=True).start(); started_wait = None
        while not self.done.is_set():
            if self.up is None:
                if not self._open_upstream():
                    started_wait = started_wait or self.clock()
                    if self.clock() - started_wait > self.give_up_s: break            # nothing usable for a long time: let the client decide
                    self.sleep(self.retry_wait); continue
                started_wait = None
            if self.ep is not None and self.clock() - self.connected_at > self.failback_s:           # come back to a preferred source once it has recovered (e.g. after its monthly reset)
                best = self.relay.pick_ws(self.cap)
                if best is not None and best.priority < self.ep.priority:
                    with self.lock:
                        try: self.up.close()
                        except Exception: pass
                        self.up = None
                    continue
                self.connected_at = self.clock()
            try:
                op, data = self.up.recv_message()
                if not self.relay.ws_meter(self.ep, len(data)): raise WS.WSClosed("allowance used up")
                self._upstream_message(data)
            except WS.WSClosed as e:
                if self.done.is_set(): break
                kind = "quota" if "allowance" in str(e) else "rate_limited" if "rate_limited" in str(e) else "auth" if "auth" in str(e) else "unsupported" if "unsupported" in str(e) else "unavailable"
                if kind != "quota": self.relay.ws_failed(self.ep, kind)            # a used-up allowance was already recorded by ws_meter
                with self.lock:
                    try: self.up.close()
                    except Exception: pass
                    self.up = None
        self.done.set(); self.close()

    def close(self):
        with self.lock:
            if self.up is not None:
                try: self.up.close()
                except Exception: pass
            self.up = None
        try: self.client.close()
        except Exception: pass
