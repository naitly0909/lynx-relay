"""Usage counters per endpoint, kept in ~/.lynx-relay/usage.json (atomic writes). The monthly counter restarts at the billing day of the provider (config reset_day, default 1), the daily one at 00:00 UTC.
Counters are this tool's own count (calls x cost). Your provider's dashboard is the truth; the tool stops a little before the limit (98%) and also reacts to the provider's own quota errors."""
import datetime, json, os, tempfile, threading, time
try: import fcntl
except ImportError: fcntl = None      # Windows: no file lock, the last save wins
from .catalog import home

def path(): return home() / "usage.json"

def period_key(period, now, reset_day=1):
    d = datetime.datetime.fromtimestamp(now, datetime.timezone.utc)
    if period == "day": return d.strftime("%Y-%m-%d")
    if period == "month":
        rd = min(max(int(reset_day or 1), 1), 28)
        y, m = (d.year, d.month) if d.day >= rd else ((d.year, d.month - 1) if d.month > 1 else (d.year - 1, 12))
        return f"{y:04d}-{m:02d}-{rd:02d}"
    return "all"

class Usage:
    """serve, mcp and test are separate processes on one file: each keeps what it added since its last save (self.added) and a save adds that to the file under a lock,
    so nobody overwrites another's counts and the 98% stop sees the total."""
    def __init__(self, file=None):
        self.file = file or path(); self.lock = threading.Lock(); self.data = self._read(); self.added = {}; self.dirty = 0; self.last_flush = time.time()
    def _read(self):
        try: d = json.loads(self.file.read_text(encoding="utf-8"))
        except (OSError, ValueError): return {}
        return d if isinstance(d, dict) else {}
    def used(self, eid, period, now, reset_day=1):
        with self.lock:
            r = self.data.get(eid) or {}
            return float(r.get("used", 0)) if r.get("period") == period_key(period, now, reset_day) else 0.0
    def add(self, eid, n, period, now, reset_day=1, calls=1):
        with self.lock:
            pk = period_key(period, now, reset_day); r = self.data.get(eid)
            if not r or r.get("period") != pk: r = self.data[eid] = {"period": pk, "used": 0.0, "calls": 0}
            r["used"] = float(r.get("used", 0)) + n; r["calls"] = int(r.get("calls", 0)) + calls; self.dirty += 1
            a = self.added.get(eid)
            if not a or a["period"] != pk: a = self.added[eid] = {"period": pk, "used": 0.0, "calls": 0}
            a["used"] += n; a["calls"] += calls
            age = time.time() - self.last_flush
            if (self.dirty >= 20 and age >= 5) or age >= 60: self._flush()      # at most one write every 5 s however busy a stream is; a quiet proxy saves once a minute
    def set_used(self, eid, n, period, now, reset_day=1):
        with self.lock:
            self.added.pop(eid, None)
            self.data[eid] = {"period": period_key(period, now, reset_day), "used": float(n), "calls": int((self.data.get(eid) or {}).get("calls", 0))}; self._flush(exact=eid)
    def flush(self):
        with self.lock: self._flush()
    def _flush(self, exact=None):
        self.dirty = 0; self.last_flush = time.time()
        try:
            self.file.parent.mkdir(parents=True, exist_ok=True)
            with open(str(self.file) + ".lock", "a") as lk:
                if fcntl: fcntl.flock(lk, fcntl.LOCK_EX)
                disk = self._read()
                for eid, a in self.added.items():
                    r = disk.get(eid)
                    if not isinstance(r, dict) or r.get("period") != a["period"]: r = disk[eid] = {"period": a["period"], "used": 0.0, "calls": 0}
                    r["used"] = float(r.get("used", 0)) + a["used"]; r["calls"] = int(r.get("calls", 0)) + a["calls"]
                if exact: disk[exact] = self.data[exact]
                fd, tmp = tempfile.mkstemp(dir=str(self.file.parent), prefix=".usage-")
                with os.fdopen(fd, "w", encoding="utf-8") as f: json.dump(disk, f)
                os.replace(tmp, self.file)
            self.added = {}; self.data = disk
        except OSError: pass
