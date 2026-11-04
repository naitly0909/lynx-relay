"""lynx-relay command line:  providers | init | add <id> | remove <id> | status | test | serve | mcp."""
import argparse, getpass, json, sys
from . import __version__, catalog as CAT, config as CFG
from .core import Relay

def _table(rows, head):
    w = [max(len(str(r[i])) for r in rows + [head]) for i in range(len(head))]
    return "\n".join("  ".join(str(c).ljust(w[i]) for i, c in enumerate(r)).rstrip() for r in [head] + rows)

def cmd_providers(a):
    data = CAT.load(); cap = ("solana_rpc" if a.chain in ("solana", "sol") else "evm_rpc:" + a.chain) if a.chain else None
    if a.check:
        pr = CAT.problems(data); print("\n".join(pr) if pr else "catalog ok"); return 1 if pr else 0
    rows = [[p["id"], p["kind"], p["free"]["summary"][:70], p["free"]["confidence"], p["signup"] or "-"] for p in CAT.providers_for(data, cap)]
    print(_table(rows, ["id", "needs", "free allowance", "source quality", "sign up"])); print("\n" + data["disclaimer"]); print("Catalog verified:", data["verified"]); return 0

def _ask(prompt, secret=True):
    try: return (getpass.getpass(prompt) if secret and sys.stdin.isatty() else input(prompt)).strip()
    except EOFError: return ""

def cmd_init(a):
    data = CAT.load(); cfg = CFG.load(); print("Lynx Relay setup. One account per provider. Press Enter to skip a provider. Keys are saved only in", CFG.path(), "(mode 600).\n")
    for p in data["providers"]:
        if p["kind"] == "keyless": continue
        have = bool(CFG.key_for(p, cfg))
        print(f"{p['name']}: {p['free']['summary']}\n  sign up: {p['signup']}  (quality of this number: {p['free']['confidence']})")
        if p.get("notes"): print("  note:", p["notes"])
        v = _ask(("  already set, Enter to keep. " if have else "  ") + ("Paste the full endpoint URL: " if p["kind"] == "user_url" else "Paste your API key: "))
        if v: CFG.set_key(p["id"], v)
    keyless = [p["name"] for p in data["providers"] if p["kind"] == "keyless"]
    print("\nWorks without any key (used as the last fallback):", ", ".join(sorted(set(keyless)))); print("\nNext: lynx-relay test   then   lynx-relay serve"); return 0

def cmd_add(a):
    p = next((x for x in CAT.load()["providers"] if x["id"] == a.provider), None)
    if not p or p["kind"] == "keyless": print("unknown or keyless provider; see: lynx-relay providers"); return 1
    v = a.key or _ask("Paste the " + ("full endpoint URL: " if p["kind"] == "user_url" else "API key: "))
    if not v: print("nothing saved"); return 1
    CFG.set_key(p["id"], v); print("saved for", p["id"]); return 0

def cmd_remove(a): CFG.remove_key(a.provider); print("removed", a.provider); return 0

def cmd_status(a):
    r = Relay.from_config()
    if not CFG.permissions_ok(): print("WARNING: your config file is readable by other users: chmod 600", CFG.path())
    if a.json: print(json.dumps({"active": r.active(), "endpoints": r.status()}, indent=1)); return 0
    rows = [[s["cap"], s["source"], s["state"] + (f" ({s['resting_s']}s)" if s["resting_s"] else ""), "-" if s["used"] is None else f"{s['used']}/{int(s['limit']) if s['limit'] else 'no cap'}", "-" if s["percent"] is None else f"{s['percent']}%", s["calls"], s["failovers"]] for s in r.status()]
    print(_table(rows, ["network", "source", "state", "used", "%", "calls", "switched away"])); print("\nactive now:", ", ".join(f"{c}: {p or 'NONE'}" for c, p in r.active().items())); return 0

def cmd_test(a):
    r = Relay.from_config(); rows = [[x["cap"], x["source"], "ok" if x["ok"] else "FAILED: " + x["kind"], f"{x['ms']} ms"] for x in r.probe()]; r.usage.flush()
    print(_table(rows, ["network", "source", "result", "time"])); return 0 if all(x[2] == "ok" for x in rows) else 2

def cmd_serve(a):
    from .server import serve
    r = Relay.from_config(); port = a.port or int(CFG.load().get("port") or 8899)
    print(f"Lynx Relay on http://127.0.0.1:{port}  (POST /solana, /evm/<chain>; WebSocket /ws/solana; GET /status). Ctrl+C to stop."); serve(r, port); return 0

def cmd_mcp(a):
    from .mcp import main as mcp_main
    mcp_main(); return 0

def main(argv=None):
    ap = argparse.ArgumentParser(prog="lynx-relay", description="Spend your free RPC allowances in turn: one account per provider, automatic switching, keys stay on your machine.")
    ap.add_argument("--version", action="version", version=__version__); sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("providers", help="free providers and their allowances"); p.add_argument("--chain"); p.add_argument("--check", action="store_true"); p.set_defaults(f=cmd_providers)
    sub.add_parser("init", help="interactive setup").set_defaults(f=cmd_init)
    p = sub.add_parser("add", help="save one key"); p.add_argument("provider"); p.add_argument("--key"); p.set_defaults(f=cmd_add)
    p = sub.add_parser("remove", help="delete one key"); p.add_argument("provider"); p.set_defaults(f=cmd_remove)
    p = sub.add_parser("status", help="what is configured and how much is used"); p.add_argument("--json", action="store_true"); p.set_defaults(f=cmd_status)
    sub.add_parser("test", help="one cheap call to every configured source").set_defaults(f=cmd_test)
    p = sub.add_parser("serve", help="local proxy"); p.add_argument("--port", type=int); p.set_defaults(f=cmd_serve)
    sub.add_parser("mcp", help="MCP server on stdio").set_defaults(f=cmd_mcp)
    a = ap.parse_args(argv); return a.f(a)

if __name__ == "__main__": sys.exit(main())
