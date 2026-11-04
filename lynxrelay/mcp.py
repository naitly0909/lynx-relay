"""MCP server (stdio) for AI assistants: ask which free tiers exist, see your usage, and make READ-ONLY RPC calls through the relay. Keys are NOT set through the assistant (they would end up in the chat history):
run `lynx-relay init` or `lynx-relay add <provider>` in a terminal."""
import json, sys
from . import __version__, catalog as CAT
from .core import Relay, is_write

def chain_cap(chain):
    c = str(chain or "").lower(); return "solana_rpc" if c in ("solana", "sol") else "evm_rpc:" + c

def tools(relay):
    def free_tiers(a):
        data = CAT.load(); cap = chain_cap(a["chain"]) if a.get("chain") else None
        return {"verified": data["verified"], "disclaimer": data["disclaimer"], "providers": [{"id": p["id"], "name": p["name"], "kind": p["kind"], "free": p["free"], "signup": p["signup"], "notes": p.get("notes", ""), "networks": sorted({e["cap"] for e in p["endpoints"]})} for p in CAT.providers_for(data, cap)],
                "market_data": data.get("market_data", [])}
    def status(a): return {"active": relay.active(), "endpoints": relay.status()}
    def rpc(a):
        m = str(a.get("method", ""))
        if is_write(m): return {"error": "write methods are not available through the assistant"}
        doc, src = relay.rpc(chain_cap(a.get("chain")), m, a.get("params") or [])
        return {"source": src, "response": doc}
    def setup(a): return {"steps": ["pip install the package or run it from the repository", "lynx-relay init  (asks for one key per provider, press Enter to skip)", "lynx-relay test", "lynx-relay serve  then use http://127.0.0.1:8899/solana in your bot"], "rule": "one account per provider"}
    return {"relay_free_tiers": ("List free RPC providers with their documented free limits and where each number comes from (optionally for one chain: solana, eth, bsc, base, arb, polygon, avax, op).", {"chain": {"type": "string"}}, [], free_tiers),
            "relay_status": ("Show which free sources are configured, how much of each allowance is used, and which one is active now. No keys are shown.", {}, [], status),
            "relay_rpc": ("Read-only JSON-RPC call through the relay with automatic switching between your free sources. Write methods are refused.", {"chain": {"type": "string"}, "method": {"type": "string"}, "params": {"type": "array"}}, ["chain", "method"], rpc),
            "relay_setup": ("How to set the relay up (keys are entered in a terminal, never in the chat).", {}, [], setup)}

def main(relay=None, stdin=None, stdout=None):
    relay = relay or Relay.from_config(); T = tools(relay); stdin = stdin or sys.stdin; out = stdout or sys.stdout
    def reply(i, result=None, error=None):
        m = {"jsonrpc": "2.0", "id": i}; m.update({"error": error} if error else {"result": result}); out.write(json.dumps(m) + "\n"); out.flush()
    for line in stdin:
        try: r = json.loads(line)
        except ValueError: continue
        if not isinstance(r, dict): continue
        i, m = r.get("id"), r.get("method")
        if m == "initialize": reply(i, {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "lynx-relay", "version": __version__}})
        elif m == "tools/list": reply(i, {"tools": [{"name": n, "description": d, "inputSchema": {"type": "object", "properties": p, "required": req}} for n, (d, p, req, _) in T.items()]})
        elif m == "tools/call":
            p = r.get("params") or {}; t = T.get(p.get("name"))
            try: res = t[3](p.get("arguments") or {}) if t else {"error": "unknown tool"}
            except Exception as e: res = {"error": type(e).__name__}
            reply(i, {"content": [{"type": "text", "text": json.dumps(res)}]})
        elif i is not None: reply(i, error={"code": -32601, "message": "method not found"})
    relay.usage.flush()
