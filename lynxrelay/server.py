"""Local proxy: point any bot at http://127.0.0.1:8899/solana or /evm/<chain> (eth, bsc, base, arb, polygon, avax, op) instead of a provider URL.
Binds to 127.0.0.1 only. Requests that carry an Origin header (a web page in your browser) or a foreign Host header are refused, so a website cannot spend your allowance.
GET /status (states and usage, no keys), GET /health. WebSocket: ws://127.0.0.1:<port>/ws/solana or /ws/evm/<chain> (subscriptions are kept when the source changes). The answering source is in the X-Relay-Source response header; the JSON body is the node's own answer, untouched."""
import http.server, json, socketserver, threading
from . import __version__, ws as WS, wsrelay as WSR

MAX_REQUEST = 1_000_000

def make_handler(relay, port):
    allowed_hosts = {f"127.0.0.1:{port}", f"localhost:{port}", f"[::1]:{port}"}
    class H(http.server.BaseHTTPRequestHandler):
        server_version = "lynx-relay/" + __version__
        def log_message(self, fmt, *a): pass          # never log paths or bodies (they can carry addresses)
        def _send(self, status, body, ctype="application/json", extra=None):
            self.send_response(status); self.send_header("Content-Type", ctype); self.send_header("Content-Length", str(len(body)))
            for k, v in (extra or {}).items(): self.send_header(k, v)
            self.end_headers(); self.wfile.write(body)
        def _guard(self):
            if self.headers.get("Origin") is not None or (self.headers.get("Host") or "") not in allowed_hosts:
                self._send(403, b'{"error":"local clients only"}'); return False
            return True
        def do_GET(self):
            if not self._guard(): return
            if self.path == "/health": return self._send(200, b'{"ok":true}')
            cap = "solana_ws" if self.path == "/ws/solana" else ("evm_ws:" + self.path[8:]) if self.path.startswith("/ws/evm/") else None
            if cap is not None:
                if not any(e.cap == cap for e in relay.eps): return self._send(404, b'{"error":"no stream source for this chain, use /ws/solana or /ws/evm/<chain>"}')
                try: conn = WS.server_accept(self)
                except WS.WSClosed: return self._send(400, b'{"error":"websocket upgrade expected"}')
                WSR.Session(relay, conn, cap).run(); return
            if self.path == "/status": return self._send(200, json.dumps({"active": relay.active(), "endpoints": relay.status()}).encode())
            self._send(200, b"lynx-relay: POST JSON-RPC to /solana or /evm/<chain>; GET /status\n", "text/plain")
        def do_POST(self):
            if not self._guard(): return
            cap = "solana_rpc" if self.path == "/solana" else ("evm_rpc:" + self.path[5:]) if self.path.startswith("/evm/") else None
            if cap is None: return self._send(404, b'{"error":"use /solana or /evm/<chain>"}')
            try: n = int(self.headers.get("Content-Length") or 0)
            except ValueError: n = -1
            if n <= 0 or n > MAX_REQUEST: return self._send(400, b'{"error":"bad body size"}')
            r = relay.call(cap, self.rfile.read(n))
            self._send(r.status, r.body, extra={"X-Relay-Source": r.source} if r.source else None)
    return H

class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

def serve(relay, port=8899, background=False):
    srv = Server(("127.0.0.1", port), make_handler(relay, port))
    if background:
        threading.Thread(target=srv.serve_forever, daemon=True).start(); return srv
    try: srv.serve_forever()
    except KeyboardInterrupt: pass
    finally: relay.usage.flush(); srv.server_close()
    return srv
