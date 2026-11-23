"""Client-side connection with heartbeat, acknowledgements and automatic reconnect."""
import queue
import socket
import threading
import time

from common import (AUTH_TOKEN, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, HOST,
                    LOCATIONS, PORT, LineSocket, enable_keepalive)


class Rejected(ConnectionError):
    """The broker answered but refused us (bad name, bad token...). Don't retry."""


class BrokerClient:
    def __init__(self, role, name, host=HOST, port=PORT, on_push=None, on_status=None):
        self.role, self.name, self.host, self.port = role, name, host, port
        self.on_push = on_push or (lambda m: None)
        self.on_status = on_status or print
        self.ls = None
        self.connected = False
        self.closed = True          # True when the *user* is disconnected
        self.locations = list(LOCATIONS)   # replaced by the broker's list on connect
        self._responses = queue.Queue()
        self._req_lock = threading.Lock()
        self._rid = 0
        self._seen = set()          # push ids already shown on this connection

    # --------------------------------------------------------- connection --
    def connect(self):
        """User-initiated connect. Raises OSError / ConnectionError on failure."""
        self.closed = False
        try:
            return self._open()
        except Exception:
            self.closed = True
            raise

    def _open(self):
        sock = socket.create_connection((self.host, self.port), timeout=5)
        sock.settimeout(HEARTBEAT_TIMEOUT)
        enable_keepalive(sock)
        ls = LineSocket(sock)
        try:
            hello = {"type": "hello", "role": self.role, "name": self.name}
            if AUTH_TOKEN:
                hello["token"] = AUTH_TOKEN
            if not ls.send(hello):
                raise ConnectionError("Could not send hello")
            ack = ls.recv()
            if ack is None:
                raise ConnectionError("Broker closed the connection")
            if not ack.get("ok"):
                raise Rejected(ack.get("error", "Rejected by broker"))
        except Exception:
            ls.close()
            raise
        if ack.get("locations"):
            self.locations = list(ack["locations"])
        self._seen.clear()
        self.ls, self.connected = ls, True
        threading.Thread(target=self._reader, args=(ls,), daemon=True).start()
        threading.Thread(target=self._heartbeat, args=(ls,), daemon=True).start()
        return ack

    def disconnect(self):
        self.closed = True
        self.connected = False
        if self.ls:
            self.ls.send({"cmd": "bye"})
            self.ls.close()

    # ------------------------------------------------------ background IO --
    def _reader(self, ls):
        try:
            while True:
                msg = ls.recv()
                if msg is None:
                    break
                kind = msg.get("type")
                if kind == "push":
                    # Always ack so the broker can mark it delivered...
                    ls.send({"cmd": "ack", "id": msg.get("id")})
                    # ...but only show it once.
                    if msg.get("id") in self._seen:
                        continue
                    self._seen.add(msg.get("id"))
                    self.on_push(msg)
                elif kind == "response":
                    self._responses.put(msg)
                elif kind == "kicked":
                    self.closed = True
                    self.on_status(f"Disconnected by broker: {msg.get('reason')}")
        except OSError:
            pass
        finally:
            ls.close()
            if self.ls is ls:
                self.connected = False
                if not self.closed:
                    self._reconnect_loop()

    def _heartbeat(self, ls):
        while self.ls is ls and self.connected:
            time.sleep(HEARTBEAT_INTERVAL)
            if not ls.send({"cmd": "ping"}):
                ls.close()
                return

    def _reconnect_loop(self):
        delay = 1
        while not self.closed:
            self.on_status(f"Connection lost. Retrying in {delay}s...")
            time.sleep(delay)
            if self.closed:
                return
            try:
                ack = self._open()
                self.on_status("Reconnected to broker."
                               + (f" Retrieving {ack['missed']} missed alert(s)..."
                                  if ack.get("missed") else ""))
                return
            except Rejected as exc:
                # The broker is reachable but refuses us: retrying won't help.
                self.closed = True
                self.on_status(f"Broker rejected reconnect: {exc}")
                return
            except (OSError, ConnectionError):
                delay = min(delay * 2, 5)

    # ----------------------------------------------------------- requests --
    def request(self, payload: dict, timeout=5):
        with self._req_lock:
            if not self.connected or not self.ls:
                return {"ok": False, "error": "Not connected to broker (reconnecting...)"}
            self._rid += 1
            rid = self._rid
            if not self.ls.send({**payload, "rid": rid}):
                return {"ok": False, "error": "Send failed (connection lost)"}
            deadline = time.time() + timeout
            while True:
                left = deadline - time.time()
                if left <= 0:
                    return {"ok": False, "error": "Broker did not respond in time"}
                try:
                    r = self._responses.get(timeout=left)
                except queue.Empty:
                    return {"ok": False, "error": "Broker did not respond in time"}
                if r.get("rid") == rid:
                    return r      # stale replies from earlier requests are skipped