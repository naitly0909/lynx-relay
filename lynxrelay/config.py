"""Local configuration: ~/.lynx-relay/config.json (mode 600). Keys live ONLY here or in environment variables, never in logs, status output or the catalog.
{"keys": {"helius": "...", "chainstack": "https://full-endpoint-url"}, "order": {"solana_rpc": ["helius", "alchemy"]}, "reset_day": {"helius": 12}, "disabled": ["ankr"], "port": 8899}"""
import json, os, pathlib, stat, tempfile
from .catalog import home

def path(): return home() / "config.json"

def load():
    try: cfg = json.loads(path().read_text(encoding="utf-8"))
    except (OSError, ValueError): cfg = {}
    return cfg if isinstance(cfg, dict) else {}

def save(cfg):
    p = path(); p.parent.mkdir(parents=True, exist_ok=True)
    try: os.chmod(p.parent, 0o700)
    except OSError: pass
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), prefix=".cfg-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f: json.dump(cfg, f, indent=1)
        os.chmod(tmp, 0o600); os.replace(tmp, p)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)

def permissions_ok():
    """False when the config file is readable by group or others (keys inside)."""
    try: return not (path().stat().st_mode & (stat.S_IRWXG | stat.S_IRWXO))
    except OSError: return True

def key_for(provider, cfg=None):
    """The key (or full URL for user_url providers) of one provider: environment variable of the catalog first, then the config file. '' = none."""
    cfg = cfg if cfg is not None else load()
    env = provider.get("key_env") or ""
    v = (os.environ.get(env) if env else None) or (cfg.get("keys") or {}).get(provider["id"]) or ""
    return str(v).strip()

def set_key(pid, key):
    cfg = load(); cfg.setdefault("keys", {})[pid] = key.strip(); save(cfg)

def remove_key(pid):
    cfg = load(); (cfg.get("keys") or {}).pop(pid, None); save(cfg)
