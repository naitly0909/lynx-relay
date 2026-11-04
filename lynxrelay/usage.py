"""Usage counters per endpoint, kept in ~/.lynx-relay/usage.json (atomic writes). The monthly counter restarts at the billing day of the provider (config reset_day, default 1), the daily one at 00:00 UTC.
Counters are this tool's own count (calls x cost). Your provider's dashboard is the truth; the tool stops a little before the limit (98%) and also reacts to the provider's own quota errors."""
import datetime, json, os, tempfile, threading, time
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
    def __init__(self, file=None):
        self.file = file or path(); self.lock = threading.Lock(); self.data = {}; self.dirty = 0; self.last_flush = time.time()
        try: self.data = json.loads(self.file.read_text(encoding="utf-8"))
        except (OSError, ValueError): self.data = {}
        if not isinstance(self.data, dict): self.data = {}
    def used(self, eid, period, now, reset_day=1):
        with self.lock:
            r = self.data.get(eid) or {}
            return float(r.get("used", 0)) if r.get("period") == period_key(period, now, reset_day) else 0.0
    def add(self, eid, n, period, now, reset_day=1, calls=1):
        with self.lock:
            pk = period_key(period, now, reset_day); r = self.data.get(eid)
            if not r or r.get("period") != pk: r = self.data[eid] = {"period": pk, "used": 0.0, "calls": 0}
            r["used"] = float(r.get("used", 0)) + n; r["calls"] = int(r.get("calls", 0)) + calls; self.dirty += 1
            age = time.time() - self.last_flush
            if (self.dirty >= 20 and age >= 5) or age >= 60: self._flush()      # at most one write every 5 s however busy a stream is; a quiet proxy saves once a minute
    def set_used(self, eid, n, period, now, reset_day=1):
        with self.lock:
            self.data[eid] = {"period": period_key(period, now, reset_day), "used": float(n), "calls": int((self.data.get(eid) or {}).get("calls", 0))}; self._flush()
    def flush(self):
        with self.lock: self._flush()
    def _flush(self):
        self.dirty = 0; self.last_flush = time.time()
        try:
            self.file.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=str(self.file.parent), prefix=".usage-")
            with os.fdopen(fd, "w", encoding="utf-8") as f: json.dump(self.data, f)
            os.replace(tmp, self.file)
        except OSError: pass
