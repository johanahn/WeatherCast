"""Shared constants and helpers for WeatherCast (low-level sockets only).

Wire protocol: newline-delimited JSON over TCP. One JSON object per line.
"""
import json
import os
import socket
import sys
import threading
import os
import sys

# Where CLIENTS connect (override with --host or WEATHERCAST_HOST).
HOST = os.environ.get("WEATHERCAST_HOST", "127.0.0.1")
# Where the BROKER listens. 0.0.0.0 = reachable from other devices.
BIND_HOST = "0.0.0.0"
PORT = 5050

# Optional shared secret. Set the same WEATHERCAST_TOKEN on broker and clients.
# NOTE: it is sent in plain text; use a VPN/TLS if the network is untrusted.
AUTH_TOKEN = os.environ.get("WEATHERCAST_TOKEN", "")

# Default list; the broker's copy is sent to clients on connect.
LOCATIONS = ["Iloilo City", "Pavia", "Roxas City", "Kalibo"]
ALERT_LEVELS = ["Low", "Moderate", "High", "Severe"]

HEARTBEAT_INTERVAL = 10   # seconds between client pings
HEARTBEAT_TIMEOUT = 30    # seconds of silence before a peer is considered dead
MAX_LINE = 65536
RETENTION_DAYS = 7        # broker deletes alerts older than this


def setup_console():
    """Make sure symbols such as the degree sign print on any terminal."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass


def enable_keepalive(sock: socket.socket):
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    except OSError:
        pass


class LineSocket:
    """Thread-safe send / line-buffered receive wrapper around a TCP socket."""

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self._rfile = sock.makefile("r", encoding="utf-8", newline="\n")
        self._wlock = threading.Lock()

    def send(self, obj: dict) -> bool:
        data = (json.dumps(obj) + "\n").encode("utf-8")
        try:
            with self._wlock:
                self.sock.sendall(data)
            return True
        except OSError:
            return False

    def recv(self):
        """Return the next message dict, or None if the peer closed.

        Raises OSError (including socket.timeout) on network failure,
        oversized lines, truncated lines or invalid UTF-8.
        """
        try:
            line = self._rfile.readline(MAX_LINE)
        except ValueError as exc:  # UnicodeDecodeError or read on closed file
            raise OSError(f"bad data or closed stream: {exc}")
        if not line:
            return None
        if not line.endswith("\n"):
            raise OSError("line too long or connection cut mid-message")
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            return {"type": "invalid"}
        return obj if isinstance(obj, dict) else {"type": "invalid"}

    def close(self):
        for closer in (lambda: self.sock.shutdown(socket.SHUT_RDWR),
                       self.sock.close, self._rfile.close):
            try:
                closer()
            except (OSError, ValueError):
                pass


def format_temperature(value) -> str:
    text = str(value).strip()
    try:
        float(text)
        return f"{text}\u00b0C"
    except ValueError:
        return text


def format_alert(m: dict, missed: bool = False) -> str:
    width = 48
    level = m.get("alert_level", "Low")
    badge = LEVEL_STYLES.get(level, level)
    border_color = RED if level in ("High", "Severe") else YELLOW if level == "Moderate" else CYAN
    tag = " MISSED WEATHER ALERT " if missed else " LIVE WEATHER ALERT "

    lines = [
        f"{border_color}┌─{BOLD}{tag}{RESET}{border_color}{'─' * (width - len(tag) - 1)}┐{RESET}",
        f"  {BOLD}Location:{RESET}    {m['location']}",
        f"  {BOLD}Condition:{RESET}   {m['condition']} ({format_temperature(m['temperature'])})",
        f"  {BOLD}Alert Level:{RESET} {badge}",
        f"  {BOLD}Issued:{RESET}      {DIM}{m['created_at']}{RESET}",
        f"{border_color}├{'─' * width}┤{RESET}",
        f"  {m['message']}",
        f"{border_color}└{'─' * width}┘{RESET}",
    ]
    return "\n".join(lines)

    # --- ANSI Colors & Styling ---
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
ORANGE = "\033[38;5;208m"
RED = "\033[91m"

LEVEL_STYLES = {
    "Low": f"{GREEN}● LOW{RESET}",
    "Moderate": f"{YELLOW}▲ MODERATE{RESET}",
    "High": f"{ORANGE}◆ HIGH{RESET}",
    "Severe": f"{RED}{BOLD}🚨 SEVERE{RESET}",
}


def clear_screen():
    """Clear the terminal viewport and scrollback buffer."""
    if os.name == "nt":
        os.system("cls")
    else:
        # \033[2J clears visible screen, \033[3J clears scrollback buffer, \033[H moves cursor to top-left
        sys.stdout.write("\033[2J\033[3J\033[H")
        sys.stdout.flush()
        
def print_header(title: str, subtitle: str = ""):
    """Render a clean box header."""
    width = 48
    print(f"{CYAN}╔{'═' * width}╗{RESET}")
    print(f"{CYAN}║{RESET}{BOLD}{title.center(width)}{RESET}{CYAN}║{RESET}")
    if subtitle:
        print(f"{CYAN}║{RESET}{DIM}{subtitle.center(width)}{RESET}{CYAN}║{RESET}")
    print(f"{CYAN}╚{'═' * width}╝{RESET}")