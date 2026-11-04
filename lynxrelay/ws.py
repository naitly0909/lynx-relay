"""Minimal RFC 6455 WebSocket client and server side, standard library only: handshake, text/binary frames (masking, 16/64-bit lengths, fragments), ping/pong, close."""
import base64, hashlib, os, socket, ssl, struct, threading, urllib.parse

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_MSG = 16 * 1024 * 1024
TEXT, BINARY, CLOSE, PING, PONG = 1, 2, 8, 9, 10

class WSClosed(Exception):
    pass

def _mask(data, mask):
    if not data: return data
    m = (mask * (len(data) // 4 + 1))[:len(data)]
    return (int.from_bytes(data, "big") ^ int.from_bytes(m, "big")).to_bytes(len(data), "big")

def accept_key(key): return base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()

class Conn:
    """One WebSocket connection. client_side=True masks outgoing frames. recv_message() blocks; close the socket from another thread (shutdown) to interrupt it."""
    def __init__(self, sock, client_side):
        self.sock, self.client_side, self.wlock, self.closed, self.pre = sock, client_side, threading.Lock(), False, b""
    def _read(self, n):
        data = self.pre[:n]; self.pre = self.pre[n:]
        while len(data) < n:
            try: chunk = self.sock.recv(n - len(data))
            except OSError as e: raise WSClosed(type(e).__name__)
            if not chunk: raise WSClosed("eof")
            data += chunk
        return data
    def _frame(self):
        b1, b2 = self._read(2); fin, op, masked, ln = bool(b1 & 0x80), b1 & 0x0F, bool(b2 & 0x80), b2 & 0x7F
        if ln == 126: ln = struct.unpack(">H", self._read(2))[0]
        elif ln == 127: ln = struct.unpack(">Q", self._read(8))[0]
        if ln > MAX_MSG: raise WSClosed("frame too large")
        mask = self._read(4) if masked else b""; data = self._read(ln) if ln else b""
        if masked: data = _mask(data, mask)
        return fin, op, data
    def recv_message(self):
        """-> (opcode, payload) of the next text or binary message (fragments joined). Answers pings, raises WSClosed on close."""
        buf, first = b"", None
        while True:
            fin, op, data = self._frame()
            if op == PING: self.send(PONG, data); continue
            if op == PONG: continue
            if op == CLOSE:
                try: self.send(CLOSE, data[:2])
                except WSClosed: pass
                self.closed = True; raise WSClosed("closed by peer")
            if op in (TEXT, BINARY): first = op; buf = data
            else: buf += data          # continuation
            if len(buf) > MAX_MSG: raise WSClosed("message too large")
            if fin: return first, buf
    def send(self, op, payload=b""):
        head = bytes([0x80 | op]); ln = len(payload); m = 0x80 if self.client_side else 0
        head += bytes([m | ln]) if ln < 126 else bytes([m | 126]) + struct.pack(">H", ln) if ln < 65536 else bytes([m | 127]) + struct.pack(">Q", ln)
        if self.client_side:
            mask = os.urandom(4); payload = _mask(payload, mask); head += mask
        with self.wlock:
            try: self.sock.sendall(head + payload)
            except OSError as e: raise WSClosed(type(e).__name__)
    def send_text(self, text): self.send(TEXT, text.encode("utf-8"))
    def close(self):
        if self.closed: return
        self.closed = True
        try: self.send(CLOSE, struct.pack(">H", 1000))
        except Exception: pass
        for f in (lambda: self.sock.shutdown(socket.SHUT_RDWR), self.sock.close):
            try: f()
            except OSError: pass

def connect(url, timeout=10.0, headers=None):
    """Open a client connection to ws:// or wss://. The URL (it may carry a key) is never put in an exception message."""
    u = urllib.parse.urlsplit(url)
    if u.scheme not in ("ws", "wss") or not u.hostname: raise WSClosed("bad url")
    port = u.port or (443 if u.scheme == "wss" else 80)
    try:
        sock = socket.create_connection((u.hostname, port), timeout=timeout)
        if u.scheme == "wss": sock = ssl.create_default_context().wrap_socket(sock, server_hostname=u.hostname)
        key = base64.b64encode(os.urandom(16)).decode(); path = (u.path or "/") + ("?" + u.query if u.query else "")
        req = f"GET {path} HTTP/1.1\r\nHost: {u.netloc.split('@')[-1]}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nUser-Agent: lynx-relay/0.2\r\n"
        req += "".join(f"{k}: {v}\r\n" for k, v in (headers or {}).items()) + "\r\n"; sock.sendall(req.encode())
        resp = b""
        while b"\r\n\r\n" not in resp:
            chunk = sock.recv(4096)
            if not chunk or len(resp) > 65536: raise WSClosed("no handshake")
            resp += chunk
        head, _, rest = resp.partition(b"\r\n\r\n"); lines = head.decode("latin-1").split("\r\n"); hd = {l.split(":", 1)[0].strip().lower(): l.split(":", 1)[1].strip() for l in lines[1:] if ":" in l}
        status = int(lines[0].split()[1]) if len(lines[0].split()) > 1 else 0
        if status != 101 or hd.get("sec-websocket-accept") != accept_key(key): raise WSClosed(f"handshake refused {status}")
        sock.settimeout(None); c = Conn(sock, True)
        c.pre = rest
        return c
    except WSClosed: raise
    except Exception as e: raise WSClosed(type(e).__name__)

def server_accept(handler):
    """Complete the upgrade of an http.server handler (headers already read) and return a server-side Conn on its socket."""
    key = handler.headers.get("Sec-WebSocket-Key")
    if not key or "websocket" not in (handler.headers.get("Upgrade") or "").lower(): raise WSClosed("not a websocket request")
    handler.wfile.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept_key(key) + "\r\n\r\n").encode()); handler.wfile.flush()
    handler.close_connection = True
    return Conn(handler.connection, False)
