"""The relay core: a list of free endpoints per capability, tried in order; a rate limit, an exhausted allowance or a failure moves to the next one.
Capabilities: "solana_rpc" and "evm_rpc:<chain>" (JSON-RPC over HTTP, single calls and batches). Standard library only, thread-safe.
Rules that keep it honest and safe:
  * keys never leave this process except to their own provider; error texts, status and logs carry provider NAMES and reasons, never URLs or keys;
  * a definitive answer of a node (invalid params, execution reverted, ...) is returned as it is, never retried elsewhere; only rate limits, quota, auth, plan limits and outages fail over;
  * write methods (sendTransaction, eth_sendRawTransaction) are never repeated after an AMBIGUOUS failure (timeout, 5xx): the first node may have processed it. They fail over only after a clear refusal (429, quota, auth);
  * the local counter stops an endpoint at 98% of its free allowance and the provider's own quota error switches it off for the rest of the period."""
import json, re, threading, time, urllib.error, urllib.request
from . import catalog as CAT, config as CFG
from .usage import Usage, period_key

WRITE_METHODS = frozenset({"sendTransaction", "sendRawTransaction", "eth_sendRawTransaction", "eth_sendTransaction", "simulateBundle", "sendBundle"})
RATE_CODES = frozenset({-32005, -32429, -32007, 429})
QUOTA_RE = re.compile(r"quota|monthly|credits? (?:limit|exhaust)|usage limit|out of credits|exceeded your|plan limit|compute units", re.I)
RATE_RE = re.compile(r"rate.?limit|too many requests|limit exceeded|throttl|slow down", re.I)
PLAN_RE = re.compile(r"not available on (?:your|the free)|upgrade|not supported|unsupported|archive|paid plan|not enabled|method not found", re.I)
SAFE_MARGIN = 0.98
MAX_BODY = 2_000_000
BACKOFF = (5, 15, 30, 60, 120, 300)

class Result:
    """status: HTTP status for the client; body: bytes (JSON); source: provider id or None; tried: [{"source","reason"}]."""
    def __init__(self, status, body, source=None, tried=None): self.status, self.body, self.source, self.tried = status, body, source, tried or []
    def json(self):
        try: return json.loads(self.body)
        except ValueError: return None

def default_transport(url, body, timeout):
    """-> (status, body bytes, retry_after seconds or None). Raises on connection problems (the message is never used)."""
    req = urllib.request.Request(url, body, {"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "lynx-relay/0.2"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r: return r.status, r.read(MAX_BODY), None
    except urllib.error.HTTPError as e:
        ra = e.headers.get("Retry-After") if e.headers else None
        try: ra = float(ra) if ra is not None else None
        except ValueError: ra = None
        try: data = e.read(MAX_BODY)
        except Exception: data = b""
        return e.code, data, ra

class Bucket:
    def __init__(self, rate, clock):
        self.rate, self.cap, self.clock = max(rate, 0.1), max(1.0, rate), clock; self.tokens, self.t = self.cap, clock()
    def wait_needed(self):
        """Seconds until a token is available (0 = take it now); takes it when available."""
        now = self.clock(); self.tokens = min(self.cap, self.tokens + (now - self.t) * self.rate); self.t = now
        if self.tokens >= 1: self.tokens -= 1; return 0.0
        return (1 - self.tokens) / self.rate

class Endpoint:
    def __init__(self, provider, cap, url, rps, limit, period, cost, keys, reset_day=1, clock=time.time, priority=0, name=None):
        self.provider, self.cap, self.url, self.limit, self.period, self.cost = provider, cap, url, float(limit), period, float(cost)
        self.id = f"{provider}:{cap}"; self.acct = provider; self.cost_per_mb = 0.0; self.keys = [k for k in keys if k]; self.reset_day = reset_day; self.priority = priority; self.name = name or provider
        self.bucket = Bucket(rps * 0.8, clock); self.rest_until = 0.0; self.exhausted_until = 0.0; self.disabled = ""; self.fails = 0
        self.unsupported = {}; self.calls = self.errors = self.failovers = 0; self.last_error = ""

class Relay:
    def __init__(self, endpoints, usage=None, transport=default_transport, clock=time.time, sleep=time.sleep, timeout=12.0):
        self.eps = sorted(endpoints, key=lambda e: e.priority); self.usage, self.transport, self.clock, self.sleep, self.timeout = usage or Usage(), transport, clock, sleep, timeout
        self.lock = threading.Lock()

    # ---- building ----
    @classmethod
    def from_config(cls, data=None, cfg=None, **kw):
        data = data or CAT.load(); cfg = cfg if cfg is not None else CFG.load(); eps = []; off = set(cfg.get("disabled") or []); rd = cfg.get("reset_day") or {}
        for idx, p in enumerate(data["providers"]):
            if p["id"] in off: continue
            key = "" if p["kind"] == "keyless" else CFG.key_for(p, cfg)
            if p["kind"] != "keyless" and not key: continue
            if p["kind"] == "user_url" and not key.startswith("https://"): continue
            for e in p["endpoints"]:
                url = e["url"].replace("{key}", key) if p["kind"] != "keyless" else e["url"]
                eps.append(Endpoint(p["id"], e["cap"], url, e["rps"], e["limit"], e["period"], e["cost"], [key], int(rd.get(p["id"], 1) or 1), kw.get("clock", time.time), priority=(idx * 10) if p["kind"] != "keyless" else 1000 + int(p.get("rank", 5)) * 10, name=p.get("name")))
        for e in eps:
            e.cost_per_mb = float(next((x.get("cost_per_mb", 0) for p in data["providers"] if p["id"] == e.provider for x in p["endpoints"] if x["cap"] == e.cap), 0) or 0)
        for cap, order in (cfg.get("order") or {}).items():       # explicit order per capability: listed providers first, in that order
            for rank, pid in enumerate(order):
                for e in eps:
                    if e.cap == cap and e.provider == pid: e.priority = -1000 + rank
        return cls(eps, **kw)

    def caps(self): return sorted({e.cap for e in self.eps})

    # ---- availability ----
    def _reason(self, ep, methods, cost, now):
        if ep.disabled: return "disabled:" + ep.disabled
        if now < ep.rest_until: return "resting"
        if now < ep.exhausted_until: return "exhausted"
        if any(now < ep.unsupported.get(m, 0) for m in methods): return "method_not_on_this_plan"
        if ep.limit and ep.period != "none" and self.usage.used(ep.acct, ep.period, now, ep.reset_day) + cost > ep.limit * SAFE_MARGIN: return "allowance_used_up"
        return None

    def _redact(self, text, ep):
        for k in ep.keys:
            if k: text = text.replace(k, "***")
        return text

    def _classify(self, status, body, ep):
        """-> (kind, retry_after or None, ambiguous). kind: ok | rate_limited | quota | auth | unsupported | unavailable."""
        text = body[:2000].decode("utf-8", "replace") if body else ""
        if status == 429: return "rate_limited", None, False
        if status == 402: return "quota", None, False
        if status in (401, 403): return ("quota" if QUOTA_RE.search(text) else "auth"), None, False
        if status >= 500 or status in (404, 408): return "unavailable", None, True
        try: doc = json.loads(body)
        except ValueError: return "unavailable", None, True
        items = doc if isinstance(doc, list) else [doc]
        for it in items:
            err = it.get("error") if isinstance(it, dict) else None
            if not err: continue
            code = err.get("code") if isinstance(err, dict) else None; msg = str(err.get("message", "") if isinstance(err, dict) else err)
            if QUOTA_RE.search(msg): return "quota", None, False          # before the rate codes: several providers answer a used-up allowance with a rate-limit code
            if code in RATE_CODES or RATE_RE.search(msg): return "rate_limited", None, False
            if code == -32601 or PLAN_RE.search(msg): return "unsupported", None, False
        return "ok", None, False

    def _fail(self, ep, kind, retry_after, now, methods):
        with self.lock:
            ep.errors += 1; ep.last_error = kind
            if kind in ("rate_limited", "unavailable"):
                ep.fails += 1; ep.rest_until = now + max(retry_after or 0, BACKOFF[min(ep.fails - 1, len(BACKOFF) - 1)])
            elif kind == "quota":
                nxt = {"day": 86400, "month": 31 * 86400}.get(ep.period, 3600)
                ep.exhausted_until = now + (min(nxt, 3600) if ep.period == "none" else self._until_next_period(ep, now))
                if ep.limit and ep.period != "none": self.usage.set_used(ep.acct, ep.limit, ep.period, now, ep.reset_day)
            elif kind == "auth": ep.disabled = "auth (check the key)"
            elif kind == "unsupported":
                for m in methods: ep.unsupported[m] = now + 3600

    def _until_next_period(self, ep, now):
        import datetime
        pk = period_key(ep.period, now, ep.reset_day); y, m, d = (int(x) for x in pk.split("-")) if ep.period == "month" else (0, 0, 0)
        if ep.period == "day": return 86400 - (now % 86400)
        nm = (y + (m == 12), 1 if m == 12 else m + 1); end = datetime.datetime(nm[0], nm[1], d, tzinfo=datetime.timezone.utc).timestamp()
        return max(60.0, end - now)

    # ---- the call ----
    def call(self, cap, body):
        """body: bytes of one JSON-RPC request or a batch. -> Result."""
        try: doc = json.loads(body)
        except ValueError: return Result(400, b'{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error"}}')
        items = doc if isinstance(doc, list) else [doc]
        if not items or not all(isinstance(i, dict) for i in items): return Result(400, b'{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"invalid request"}}')
        methods = [str(i.get("method", "")) for i in items]; rid = items[0].get("id"); write = any(m in WRITE_METHODS for m in methods); tried = []; attempted = []; last_def = None
        cands = [e for e in self.eps if e.cap == cap]
        if not cands: return Result(404, self._err(rid, -32050, "no free source is configured for " + cap, []), None, [])
        for ep in cands:
            now = self.clock(); cost = ep.cost * len(items); why = self._reason(ep, methods, cost, now)
            if why:
                tried.append({"source": ep.provider, "reason": why}); continue
            with self.lock: wait = ep.bucket.wait_needed()
            if wait > 0.5: tried.append({"source": ep.provider, "reason": "busy"}); continue
            if wait > 0: self.sleep(wait)
            try: status, data, ra = self.transport(ep.url, body, self.timeout)
            except Exception: status, data, ra = 0, b"", None
            now = self.clock()
            kind, _, ambiguous = ("unavailable", None, True) if status == 0 else self._classify(status, data, ep)
            if kind == "ok":
                with self.lock: ep.calls += 1; ep.fails = 0
                if ep.period != "none": self.usage.add(ep.acct, cost, ep.period, now, ep.reset_day)
                return Result(status, data, ep.provider, tried)
            attempted.append(kind)
            if kind == "unsupported": last_def = Result(status, data, ep.provider)
            self._fail(ep, kind, ra, now, methods); tried.append({"source": ep.provider, "reason": kind})
            if write and ambiguous:
                return Result(502, self._err(rid, -32051, "the transaction may or may not have been processed by " + ep.provider + "; check its signature before sending again", tried), None, tried)
            with self.lock: ep.failovers += 1
        if last_def and attempted and all(k == "unsupported" for k in attempted):      # every node says the method does not exist: that IS the answer, not an outage
            last_def.tried = tried; return last_def
        now = self.clock(); waits = [t - now for e in cands for t in (e.rest_until, e.exhausted_until) if t > now and not e.disabled]
        data = {"tried": tried}
        if waits: data["retry_in_s"] = int(max(1, min(waits)))
        return Result(503, self._err(rid, -32050, "all free sources are busy or used up", tried, data), None, tried)

    def _err(self, rid, code, msg, tried, data=None):
        return json.dumps({"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": msg, **({"data": data} if data else {})}}).encode()

    def rpc(self, cap, method, params=None, rid=1):
        """Convenience for code and the MCP server: one call -> (parsed answer or None, source or None)."""
        r = self.call(cap, json.dumps({"jsonrpc": "2.0", "id": rid, "method": method, "params": params if params is not None else []}).encode())
        return r.json(), r.source

    # ---- status (no URLs, no keys) ----
    def status(self):
        now = self.clock(); out = []
        for ep in self.eps:
            used = self.usage.used(ep.acct, ep.period, now, ep.reset_day) if ep.period != "none" else None
            state = "disabled" if ep.disabled else "resting" if now < ep.rest_until else "exhausted" if (now < ep.exhausted_until or (ep.limit and used is not None and used >= ep.limit * SAFE_MARGIN)) else "ok"
            out.append({"source": ep.provider, "name": ep.name, "cap": ep.cap, "state": state, "detail": ep.disabled or None, "used": None if used is None else round(used), "limit": ep.limit or None,
                        "percent": round(100 * used / ep.limit, 1) if used is not None and ep.limit else None, "resting_s": max(0, int(ep.rest_until - now)) if state == "resting" else 0,
                        "calls": ep.calls, "errors": ep.errors, "failovers": ep.failovers, "last_error": ep.last_error or None, "keyless": ep.limit == 0 and ep.period == "none"})
        return out

    def active(self):
        """{cap: provider id of the first usable endpoint or None}."""
        res = {}
        for cap in self.caps():
            res[cap] = next((s["source"] for s in self.status() if s["cap"] == cap and s["state"] == "ok"), None)
        return res

    # ---- streams (WebSocket) ----
    def pick_ws(self, cap, exclude=()):
        """First usable stream endpoint of a capability (e.g. solana_ws) or None. Streams are metered by the bytes they deliver (ws_meter)."""
        now = self.clock()
        for ep in self.eps:
            if ep.cap == cap and ep.id not in exclude and not self._reason(ep, [], 0, now): return ep
        return None

    def ws_meter(self, ep, nbytes):
        """Adds the cost of nbytes delivered by a stream; returns False when the endpoint just reached its allowance (the caller moves to the next source)."""
        if ep.period == "none" or not ep.cost_per_mb: return True
        now = self.clock(); self.usage.add(ep.acct, nbytes / 1e6 * ep.cost_per_mb, ep.period, now, ep.reset_day, calls=0)
        if ep.limit and self.usage.used(ep.acct, ep.period, now, ep.reset_day) >= ep.limit * SAFE_MARGIN:
            self._fail(ep, "quota", None, now, []); return False
        return True

    def ws_failed(self, ep, kind):
        self._fail(ep, kind, None, self.clock(), []); 
        with self.lock: ep.failovers += 1

    def ws_ok(self, ep):
        with self.lock: ep.calls += 1; ep.fails = 0

    def probe(self):
        """One cheap read call (getSlot / eth_blockNumber) to EVERY endpoint on its own, no failover, to check keys and reachability. Costs one call per endpoint. -> [{"source","cap","ok","kind","ms"}]."""
        out = []
        for ep in (e for e in self.eps if not e.cap.endswith("_ws")):
            method = "getSlot" if ep.cap == "solana_rpc" else "eth_blockNumber"; body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": []}).encode(); t0 = time.time()
            try: status, data, ra = self.transport(ep.url, body, self.timeout); kind = "unavailable" if status == 0 else self._classify(status, data, ep)[0]
            except Exception: kind = "unavailable"
            if kind == "ok" and ep.period != "none": self.usage.add(ep.acct, ep.cost, ep.period, self.clock(), ep.reset_day)
            out.append({"source": ep.provider, "cap": ep.cap, "ok": kind == "ok", "kind": kind, "ms": int((time.time() - t0) * 1000)})
        return out
