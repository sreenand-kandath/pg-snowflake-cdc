"""
health.py
Lightweight HTTP health-check server running in a background thread.

Endpoints:
  GET /healthz   — liveness:  always 200 while the process is alive
  GET /readyz    — readiness: 200 only once the replication stream is running,
                              503 if the stream has not started or has failed

Point your container orchestrator's liveness + readiness probes at these.
Port is configurable via HEALTH_PORT (default: 8080).
"""

import http.server
import logging
import os
import threading
import time
from typing import Optional

log = logging.getLogger(__name__)

HEALTH_PORT: int = int(os.environ.get("HEALTH_PORT", "8080"))

# Shared state updated by main.py
_state: dict = {
    "ready": False,       # True once replication stream is running
    "last_event_ts": 0.0, # Unix timestamp of last successfully processed event
    "error": None,        # Last error message, if any
}


def set_ready(ready: bool, error: Optional[str] = None) -> None:
    _state["ready"] = ready
    _state["error"] = error


def record_event() -> None:
    _state["last_event_ts"] = time.monotonic()


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/healthz":
            self._respond(200, {"status": "ok"})

        elif self.path == "/readyz":
            if _state["ready"]:
                self._respond(200, {
                    "status": "ready",
                    "last_event_age_s": round(time.monotonic() - _state["last_event_ts"], 1),
                })
            else:
                self._respond(503, {
                    "status": "not_ready",
                    "error": _state.get("error", "stream not started"),
                })
        else:
            self._respond(404, {"status": "not_found"})

    def _respond(self, code: int, body: dict) -> None:
        import json
        payload = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt: str, *args) -> None:
        # Suppress per-request access logs — too noisy for probe traffic
        pass


def start_health_server() -> threading.Thread:
    """Start the HTTP server in a daemon thread. Returns the thread."""
    server = http.server.HTTPServer(("0.0.0.0", HEALTH_PORT), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="health-server")
    thread.start()
    log.info("Health server listening on port %d (/healthz /readyz)", HEALTH_PORT)
    return thread
