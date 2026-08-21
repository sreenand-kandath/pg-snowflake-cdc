"""
monitoring.py
Centralised observability: structured JSON logging + Datadog metrics/traces.

This integration is optional — every function here no-ops safely if DD_API_KEY
is never set, so the service runs fine without Datadog. Swap this module out
entirely if you use a different observability stack (Prometheus, CloudWatch, ...);
main.py only calls the small set of functions re-exported at the bottom.

Logging
───────
All loggers output structured JSON to stdout. Most log platforms (Datadog,
CloudWatch, Google Cloud Logging, ...) parse structured JSON automatically
when reading container stdout. Each log entry includes: timestamp, level,
logger, message, service, env, plus any extra fields passed as keyword args.

Datadog metrics
───────────────
Metrics are sent via DogStatsD UDP, with an HTTP fallback. Two modes:

  1. With a Datadog Agent sidecar (recommended for production):
       Set DD_AGENT_HOST to the agent's hostname/IP.
       The Agent forwards metrics to Datadog with full tagging.

  2. Without an agent (fallback):
       Metrics are POSTed directly to the Datadog HTTP API every 10s.

Key metrics tracked (replace the "cdc." prefix via DD_METRIC_PREFIX if you want
these namespaced under your own service name):
  cdc.events.processed   — counter  (tags: table, op)
  cdc.events.published   — counter  (tags: table, op)
  cdc.reconnects         — counter
  cdc.replication.lag_ms — gauge

Environment variables
──────────────────────
  DD_API_KEY       — Datadog API key (optional; leave unset to disable Datadog)
  DD_APP_KEY       — Datadog app key (optional)
  DD_SITE          — Datadog site, e.g. datadoghq.com / datadoghq.eu (default: datadoghq.com)
  DD_AGENT_HOST    — DogStatsD agent host (default: unset → HTTP fallback)
  DD_STATSD_PORT   — DogStatsD port (default: 8125)
  DD_ENV           — deployment env tag (default: dev)
  DD_SERVICE       — service name tag (default: pg-snowflake-cdc)
  DD_OTLP_ENDPOINT — OTLP traces endpoint (optional; enables APM traces)
  DD_METRIC_PREFIX — metric name prefix (default: cdc)
  DD_TEAM_TAG      — optional fixed "team:<value>" tag applied to every metric
"""

import datetime
import json
import logging
import os
import queue
import socket
import sys
import threading
import time
import urllib.request
from typing import Any, Optional

from opentelemetry import trace as _otel_trace
from opentelemetry.sdk.resources import Resource, SERVICE_NAME as _RES_SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider as _TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor as _BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter as _OTLPSpanExporter

# ── Constants ─────────────────────────────────────────────────────────────── #

DD_AGENT_HOST: Optional[str] = os.environ.get("DD_AGENT_HOST")
DD_STATSD_PORT: int = int(os.environ.get("DD_STATSD_PORT", "8125"))
DD_ENV: str = os.environ.get("DD_ENV", "dev")
DD_SERVICE: str = os.environ.get("DD_SERVICE", "pg-snowflake-cdc")
DD_VERSION: str = os.environ.get("DD_VERSION", "1.0.0")
DD_API_KEY: str = os.environ.get("DD_API_KEY", "")
DD_APP_KEY: str = os.environ.get("DD_APP_KEY", "")
DD_SITE: str = os.environ.get("DD_SITE", "datadoghq.com")
_METRIC_PREFIX: str = os.environ.get("DD_METRIC_PREFIX", "cdc")

_DD_LOG_INTAKE = f"https://http-intake.logs.{DD_SITE}/api/v2/logs"
_DD_METRICS_URL = f"https://api.{DD_SITE}/api/v1/series"
_DD_EVENTS_URL = f"https://api.{DD_SITE}/api/v1/events"

_GLOBAL_TAGS = [f"env:{DD_ENV}", f"service:{DD_SERVICE}", f"version:{DD_VERSION}"]
if os.environ.get("DD_TEAM_TAG"):
    _GLOBAL_TAGS.append(f"team:{os.environ['DD_TEAM_TAG']}")

# ── Structured JSON log formatter ─────────────────────────────────────────── #


class _JsonFormatter(logging.Formatter):
    """Formats every log record as a single-line JSON object."""

    _SKIP_KEYS = frozenset({
        "name", "msg", "args", "levelname", "levelno", "pathname",
        "filename", "module", "exc_info", "exc_text", "stack_info",
        "lineno", "funcName", "created", "msecs", "relativeCreated",
        "thread", "threadName", "processName", "process", "message",
    })

    def format(self, record: logging.LogRecord) -> str:
        dd_ctx: dict[str, Any] = {
            "service": DD_SERVICE,
            "env": DD_ENV,
            "version": DD_VERSION,
        }
        # Inject OpenTelemetry trace context for log/trace correlation.
        # Datadog uses 64-bit trace IDs (lower 64 bits of the 128-bit OTel ID).
        span = _otel_trace.get_current_span()
        span_ctx = span.get_span_context() if span else None
        if span_ctx and span_ctx.is_valid:
            dd_ctx["trace_id"] = str(span_ctx.trace_id & 0xFFFFFFFFFFFFFFFF)
            dd_ctx["span_id"] = str(span_ctx.span_id)

        doc: dict[str, Any] = {
            "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "status": record.levelname.lower(),
            "message": record.getMessage(),
            "logger": {"name": record.name},
            "dd": dd_ctx,
            "service": DD_SERVICE,
            "ddsource": "python",
            "ddtags": ",".join(_GLOBAL_TAGS),
            "hostname": socket.gethostname(),
        }
        if record.exc_info:
            doc["error"] = {
                "kind": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]),
                "stack": self.formatException(record.exc_info),
            }
        for key, val in record.__dict__.items():
            if key not in self._SKIP_KEYS:
                doc[key] = val

        return json.dumps(doc, default=str)


def configure_logging(level: str = "INFO") -> None:
    """Call once at process start to switch all loggers to JSON output."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("opentelemetry").setLevel(logging.WARNING)

    _init_otel()
    # Always add the HTTP log handler — it no-ops when DD_API_KEY is empty,
    # and starts shipping as soon as the key is set.
    dd_handler = _DDLogHandler()
    dd_handler.setFormatter(_JsonFormatter())
    root.addHandler(dd_handler)
    if DD_API_KEY:
        logging.getLogger("monitoring").info(
            "Datadog HTTP log shipping enabled", extra={"site": DD_SITE, "env": DD_ENV},
        )


# ── OpenTelemetry initialisation ──────────────────────────────────────────── #


def _init_otel() -> None:
    """
    Set up the global OTel TracerProvider. If DD_OTLP_ENDPOINT is set (e.g.
    pointing at a Datadog agent sidecar or any OTLP collector), spans are
    exported via OTLP HTTP. Otherwise traces stay in-process (trace/span IDs
    still appear in JSON logs for log/trace correlation).
    """
    resource = Resource.create({
        _RES_SERVICE_NAME: DD_SERVICE,
        "deployment.environment": DD_ENV,
        "service.version": DD_VERSION,
    })
    provider = _TracerProvider(resource=resource)

    otlp_endpoint = os.environ.get("DD_OTLP_ENDPOINT", "")
    if otlp_endpoint:
        try:
            headers = {"DD-API-KEY": DD_API_KEY} if DD_API_KEY else {}
            exporter = _OTLPSpanExporter(endpoint=otlp_endpoint, headers=headers)
            provider.add_span_processor(_BatchSpanProcessor(exporter))
            logging.getLogger("monitoring").info(
                "OTel OTLP exporter configured", extra={"endpoint": otlp_endpoint},
            )
        except Exception as exc:
            logging.getLogger("monitoring").warning(
                "OTel OTLP exporter setup failed", extra={"error": str(exc)}
            )

    _otel_trace.set_tracer_provider(provider)


def get_tracer() -> _otel_trace.Tracer:
    """Return the application-wide OpenTelemetry tracer."""
    return _otel_trace.get_tracer(DD_SERVICE, DD_VERSION)


# ── Datadog HTTP log intake (agentless) ───────────────────────────────────── #


class _DDLogHandler(logging.Handler):
    """
    Async batch handler that POSTs JSON log records to the Datadog HTTP Log
    Intake. Ships up to 100 records at a time, flushing every 5 seconds.
    Silently drops records if the queue is full or the intake is unreachable
    — this handler must never block or crash the main thread.
    """
    _MAX_BATCH = 100
    _FLUSH_INTERVAL = 5.0

    def __init__(self) -> None:
        super().__init__()
        self._q: "queue.Queue[str]" = queue.Queue(maxsize=10_000)
        self._t = threading.Thread(target=self._worker, daemon=True, name="dd-log-shipper")
        self._t.start()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._q.put_nowait(self.format(record))
        except queue.Full:
            pass  # drop — never block the main thread

    def _worker(self) -> None:
        while True:
            batch: list[str] = []
            deadline = time.monotonic() + self._FLUSH_INTERVAL
            while len(batch) < self._MAX_BATCH:
                remaining = max(0.05, deadline - time.monotonic())
                try:
                    batch.append(self._q.get(timeout=remaining))
                except queue.Empty:
                    break
            if batch:
                self._post(batch)

    def _post(self, records: list[str]) -> None:
        if not DD_API_KEY:
            return
        try:
            body = ("[" + ",".join(records) + "]").encode()
            req = urllib.request.Request(
                _DD_LOG_INTAKE, data=body,
                headers={"Content-Type": "application/json", "DD-API-KEY": DD_API_KEY},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=10).close()
        except Exception:
            pass  # never raise from a background thread


# ── Datadog Metrics + Events API (agentless) ──────────────────────────────── #


def send_metric_to_datadog(metric: str, value: float, tags: Optional[list[str]] = None,
                            metric_type: str = "gauge") -> bool:
    """POST a single metric to the Datadog Metrics API. Returns True on success."""
    if not DD_API_KEY:
        return False
    try:
        payload = json.dumps({"series": [{
            "metric": metric, "points": [[int(time.time()), value]],
            "type": metric_type, "tags": _GLOBAL_TAGS + (tags or []),
        }]}).encode()
        req = urllib.request.Request(
            _DD_METRICS_URL, data=payload,
            headers={"Content-Type": "application/json", "DD-API-KEY": DD_API_KEY},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10).close()
        return True
    except Exception as exc:
        logging.getLogger("monitoring").warning(
            "DD metric send failed", extra={"metric": metric, "error": str(exc)}
        )
        return False


def send_event_to_datadog(title: str, text: str, alert_type: str = "info",
                           tags: Optional[list[str]] = None) -> bool:
    """POST an event to the Datadog Events API. alert_type: info | warning | error | success."""
    if not DD_API_KEY:
        return False
    try:
        payload = json.dumps({
            "title": title, "text": text, "alert_type": alert_type,
            "tags": _GLOBAL_TAGS + (tags or []), "source_type_name": "python",
        }).encode()
        req = urllib.request.Request(
            _DD_EVENTS_URL, data=payload,
            headers={"Content-Type": "application/json", "DD-API-KEY": DD_API_KEY},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10).close()
        return True
    except Exception as exc:
        logging.getLogger("monitoring").warning(
            "DD event send failed", extra={"title": title, "error": str(exc)}
        )
        return False


# ── DogStatsD client (UDP, no agent dependency at import time) ────────────── #


class _StatsdClient:
    """Minimal DogStatsD UDP client. Datagrams are silently dropped if no
    agent is listening, so the app never fails due to a missing Datadog agent."""

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._sock: Optional[socket.socket] = None
        self._lock = threading.Lock()

    def _get_sock(self) -> socket.socket:
        if self._sock is None:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock.setblocking(False)
        return self._sock

    def _send(self, metric: str) -> None:
        try:
            with self._lock:
                self._get_sock().sendto(metric.encode(), (self._host, self._port))
        except Exception:
            pass

    def _tag_str(self, extra_tags: list[str]) -> str:
        tags = _GLOBAL_TAGS + extra_tags
        return "|#" + ",".join(tags) if tags else ""

    def increment(self, metric: str, value: int = 1, tags: Optional[list[str]] = None) -> None:
        self._send(f"{metric}:{value}|c{self._tag_str(tags or [])}")

    def gauge(self, metric: str, value: float, tags: Optional[list[str]] = None) -> None:
        self._send(f"{metric}:{value}|g{self._tag_str(tags or [])}")

    def histogram(self, metric: str, value: float, tags: Optional[list[str]] = None) -> None:
        self._send(f"{metric}:{value}|h{self._tag_str(tags or [])}")


class _HttpStatsd:
    """HTTP-based metric sender used when DD_AGENT_HOST is not configured.
    Buffers increment/gauge calls and POSTs to the Datadog Metrics API every 10s."""
    _FLUSH_INTERVAL = 10.0

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._gauges: dict[tuple, float] = {}
        self._counters: dict[tuple, float] = {}
        self._t = threading.Thread(target=self._worker, daemon=True, name="dd-metric-http")
        self._t.start()

    def increment(self, metric: str, value: int = 1, tags: Optional[list[str]] = None) -> None:
        key = (metric, tuple(sorted(_GLOBAL_TAGS + (tags or []))))
        with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + value

    def gauge(self, metric: str, value: float, tags: Optional[list[str]] = None) -> None:
        key = (metric, tuple(sorted(_GLOBAL_TAGS + (tags or []))))
        with self._lock:
            self._gauges[key] = value

    def histogram(self, metric: str, value: float, tags: Optional[list[str]] = None) -> None:
        self.gauge(metric, value, tags)

    def _worker(self) -> None:
        while True:
            time.sleep(self._FLUSH_INTERVAL)
            self._flush()

    def _flush(self) -> None:
        if not DD_API_KEY:
            return
        with self._lock:
            gauges = dict(self._gauges)
            counters = dict(self._counters)
            self._counters.clear()
        now = int(time.time())
        series = []
        for (metric, tags), value in gauges.items():
            series.append({"metric": metric, "points": [[now, value]], "type": "gauge", "tags": list(tags)})
        for (metric, tags), value in counters.items():
            series.append({"metric": metric, "points": [[now, value]], "type": "count", "tags": list(tags)})
        if not series:
            return
        try:
            payload = json.dumps({"series": series}).encode()
            req = urllib.request.Request(
                _DD_METRICS_URL, data=payload,
                headers={"Content-Type": "application/json", "DD-API-KEY": DD_API_KEY},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=10).close()
        except Exception:
            pass


_statsd_client: Optional[Any] = None


def get_statsd():
    global _statsd_client
    if _statsd_client is None:
        if DD_AGENT_HOST:
            _statsd_client = _StatsdClient(DD_AGENT_HOST, DD_STATSD_PORT)
            logging.getLogger("monitoring").info(
                "DogStatsD client configured", extra={"host": DD_AGENT_HOST, "port": DD_STATSD_PORT}
            )
        else:
            _statsd_client = _HttpStatsd()
            logging.getLogger("monitoring").info(
                "DD_AGENT_HOST not set — metrics via HTTP API (agentless)"
            )
    return _statsd_client


# ── Convenience wrappers used by other modules ─────────────────────────────── #

_OP_NAMES = {"c": "insert", "u": "update", "d": "delete"}


def count_event(table: str, op: str, stage: str = "processed") -> None:
    """<prefix>.events.<stage>  tags: table, op"""
    get_statsd().increment(
        f"{_METRIC_PREFIX}.events.{stage}",
        tags=[f"table:{table}", f"op:{_OP_NAMES.get(op, op)}"],
    )


def count_reconnect() -> None:
    get_statsd().increment(f"{_METRIC_PREFIX}.reconnects")


def gauge_lag(lag_ms: float) -> None:
    get_statsd().gauge(f"{_METRIC_PREFIX}.replication.lag_ms", lag_ms)


def count_snowflake_flush(table: str, batch_size: int) -> None:
    get_statsd().increment(f"{_METRIC_PREFIX}.snowflake.flush_events", value=batch_size, tags=[f"table:{table}"])


def gauge_snowflake_flush_ms(ms: float) -> None:
    get_statsd().gauge(f"{_METRIC_PREFIX}.snowflake.flush_duration_ms", ms)


def count_snowflake_error(table: str, reason: str) -> None:
    get_statsd().increment(f"{_METRIC_PREFIX}.snowflake.errors", tags=[f"table:{table}", f"reason:{reason[:32]}"])


def gauge_poll_time(ms: float) -> None:
    """ms taken for one pg_logical_slot_get_binary_changes() call."""
    get_statsd().gauge(f"{_METRIC_PREFIX}.poll.duration_ms", ms)


def send_heartbeat() -> None:
    """Emit 0-value counts for event-driven metrics so dashboards show 0
    instead of a gap during quiet periods. Call on every empty-poll cycle."""
    sd = get_statsd()
    sd.increment(f"{_METRIC_PREFIX}.reconnects", value=0)
    sd.increment(f"{_METRIC_PREFIX}.snowflake.errors", value=0)
