"""The built-in catalog of free providers (catalog.json) plus an optional local override (~/.lynx-relay/catalog.local.json: same shape, providers with the same id replace the built-in ones)."""
import json, os, pathlib

HERE = pathlib.Path(__file__).resolve().parent
CAPS = ("solana_rpc", "solana_ws", "evm_rpc:eth", "evm_rpc:bsc", "evm_rpc:base", "evm_rpc:arb", "evm_rpc:polygon", "evm_rpc:avax", "evm_rpc:op",
        "evm_ws:eth", "evm_ws:bsc", "evm_ws:base", "evm_ws:arb", "evm_ws:polygon", "evm_ws:avax", "evm_ws:op")
KINDS = ("keyed", "user_url", "keyless")
CONFIDENCE = ("measured_2026-10-25", "vendor_doc", "third_party_unverified")

def home():
    return pathlib.Path(os.environ.get("LYNX_RELAY_HOME") or pathlib.Path.home() / ".lynx-relay")

def load(extra=True):
    data = json.loads((HERE / "catalog.json").read_text(encoding="utf-8"))
    local = home() / "catalog.local.json"
    if extra and local.exists():
        try:
            over = json.loads(local.read_text(encoding="utf-8")); ids = {p["id"] for p in over.get("providers", [])}
            data["providers"] = [p for p in data["providers"] if p["id"] not in ids] + over.get("providers", [])
        except (OSError, ValueError, KeyError, TypeError): pass
    return data

def problems(data):
    """Problems of a catalog as a list of strings (empty = valid). Used by the tests and by `lynx-relay providers --check`."""
    out, seen = [], set()
    for p in data.get("providers", []):
        pid = p.get("id")
        if not pid or pid in seen: out.append(f"duplicate or missing id: {pid}")
        seen.add(pid)
        if p.get("kind") not in KINDS: out.append(f"{pid}: bad kind")
        if (p.get("free") or {}).get("confidence") not in CONFIDENCE: out.append(f"{pid}: bad confidence")
        if not p.get("endpoints"): out.append(f"{pid}: no endpoints")
        for e in p.get("endpoints", []):
            if e.get("cap") not in CAPS: out.append(f"{pid}: unknown cap {e.get('cap')}")
            ok_scheme = ("wss://",) if (e.get("cap", "").endswith("_ws") or e.get("cap", "").startswith("evm_ws:")) else ("https://",)
            if p.get("kind") == "keyless" and not str(e.get("url", "")).startswith(ok_scheme): out.append(f"{pid}: keyless endpoint must be " + ok_scheme[0])
            if p.get("kind") == "keyed" and "{key}" not in e.get("url", ""): out.append(f"{pid}: keyed url needs {{key}}")
            for k in ("rps", "limit", "cost"):
                if not isinstance(e.get(k), (int, float)) or e[k] < 0: out.append(f"{pid}: bad {k}")
            if e.get("period") not in ("month", "day", "none"): out.append(f"{pid}: bad period")
    return out

def providers_for(data, cap=None):
    return [p for p in data["providers"] if not cap or any(e["cap"] == cap for e in p["endpoints"])]
