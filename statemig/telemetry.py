"""Foxtrot OTEL contract v1: bounded migration signals, exported by the official SDK.

Mirrored in the independently deployed Python modules. No control decision depends
on an exporter. Only explicitly projected migration data is emitted.
"""

from __future__ import annotations

import asyncio
import atexit
import inspect
import json
import logging
import math
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from opentelemetry import trace
from opentelemetry._logs import LogRecord, SeverityNumber
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.metrics import Observation
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from .telemetry_contract import snapshot_projection

log = logging.getLogger(__name__)
propagator = TraceContextTextMapPropagator()


class Telemetry:
    def __init__(
        self, module: str, *, providers: tuple[Any, Any, Any] | None = None, disabled: bool = False
    ) -> None:
        self.module = module
        self.closed = False
        self.enabled = (
            not disabled
            and os.getenv("OTEL_SDK_DISABLED", "false").lower() != "true"
            and bool(providers or os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
        )
        self._lock = threading.Lock()
        self._values: dict[tuple[str, str], tuple[float, float, dict[str, str]]] = {}
        self._gauges: dict[str, Any] = {}
        self.logs: Any = None
        self.metrics: Any = None
        self.traces: Any = None
        self.logger: Any = None
        self.tracer: Any = trace.NoOpTracer()
        if not self.enabled:
            return
        if providers:
            self.logs, self.metrics, self.traces = providers
        else:
            resource = Resource.create(
                {
                    "service.name": os.getenv("OTEL_SERVICE_NAME", f"foxtrot-{module}"),
                    "service.instance.id": uuid4().hex,
                    "foxtrot.module": module,
                    "foxtrot.contract.version": "1",
                }
            )
            self.logs = LoggerProvider(resource=resource)
            self.logs.add_log_record_processor(
                BatchLogRecordProcessor(OTLPLogExporter(timeout=2), max_queue_size=2048)
            )
            self.metrics = MeterProvider(
                resource=resource,
                metric_readers=[
                    PeriodicExportingMetricReader(
                        OTLPMetricExporter(timeout=2),
                        export_interval_millis=5000,
                        export_timeout_millis=3000,
                    )
                ],
            )
            self.traces = TracerProvider(resource=resource)
            self.traces.add_span_processor(
                BatchSpanProcessor(OTLPSpanExporter(timeout=2), max_queue_size=2048)
            )
        self.logger = self.logs.get_logger("foxtrot.migration", "1")
        self.meter = self.metrics.get_meter("foxtrot.migration", "1")
        self.tracer = self.traces.get_tracer("foxtrot.migration", "1")
        self.duration = self.meter.create_histogram("foxtrot.operation.duration", unit="s")
        self.operations = self.meter.create_counter("foxtrot.operation.count", unit="{operation}")

    @staticmethod
    def headers() -> dict[str, str]:
        headers: dict[str, str] = {}
        propagator.inject(headers)
        return headers

    def measure(self, name: str, value: Any, attributes: dict[str, str]) -> None:
        if not self.enabled or isinstance(value, bool) or not isinstance(value, (float, int)):
            return
        if not math.isfinite(value):
            return
        # Metric dimensions never include operation/transition IDs, keys or URLs.
        attrs = {
            k: str(v)[:128]
            for k, v in attributes.items()
            if k
            in {"workflow.id", "resource.kind", "direction", "actor.id", "environment", "signal"}
        }
        key = (name, json.dumps(attrs, sort_keys=True))
        with self._lock:
            now = time.monotonic()
            self._values = {k: v for k, v in self._values.items() if now - v[0] < 20}
            if key not in self._values and len(self._values) >= 2048:
                return
            self._values[key] = (now, float(value), attrs)
            create = name not in self._gauges
            if create:
                self._gauges[name] = None
        # Register outside our lock: SDK collection invokes callbacks under its own lock.
        if create:
            gauge = self.meter.create_observable_gauge(
                name, callbacks=[lambda _options, metric=name: self._observe(metric)]
            )
            with self._lock:
                self._gauges[name] = gauge

    def _observe(self, name: str) -> list[Observation]:
        with self._lock:
            now = time.monotonic()
            return [
                Observation(v[1], v[2])
                for k, v in self._values.items()
                if k[0] == name and now - v[0] < 20
            ]

    def snapshot(self, kind: str, data: dict[str, Any], *, error: str | None = None) -> bool:
        if not self.enabled:
            return False
        try:
            payload = snapshot_projection(kind, data)
            workflow = payload.get("workflowId")
            if not isinstance(workflow, str) or not 0 < len(workflow) <= 128:
                return False
            envelope = {
                "schemaVersion": 1,
                "resource": kind,
                "data": payload,
                "collectionError": error,
            }
            encoded = json.dumps(envelope, allow_nan=False, separators=(",", ":"))
            if len(encoded.encode()) > 32768:
                return False
            self.logger.emit(
                LogRecord(
                    timestamp=time.time_ns(),
                    severity_number=SeverityNumber.INFO,
                    severity_text="INFO",
                    body=encoded,
                    attributes={
                        "foxtrot.event": "snapshot",
                        "workflow.id": workflow,
                        "resource.kind": kind,
                    },
                )
            )
            attrs = {"workflow.id": workflow, "resource.kind": kind}
            with self._lock:
                self._values = {
                    k: v
                    for k, v in self._values.items()
                    if any(v[2].get(key) != value for key, value in attrs.items())
                }
            unavailable = (
                error
                or ((payload.get("details") or {}).get("database") or {}).get(
                    "observationAvailable"
                )
                is False
            )
            self.measure("foxtrot.observation.available", 0 if unavailable else 1, attrs)
            if not unavailable:
                at = (payload.get("observed") or {}).get("observedAt")
                try:
                    observed = datetime.fromisoformat(str(at).replace("Z", "+00:00"))
                    age = (datetime.now(UTC) - observed).total_seconds()
                    self.measure("foxtrot.observation.age", age, attrs)
                    if 0 <= age <= 15:
                        self._measure_snapshot(payload, attrs)
                except (TypeError, ValueError):
                    pass
            return True
        except Exception:
            log.warning("telemetry snapshot omitted: invalid observation")
            return False

    def _measure_snapshot(self, data: dict[str, Any], attrs: dict[str, str]) -> None:
        for key in ("completed", "total", "bytes", "totalBytes"):
            self.measure(
                f"foxtrot.migration.{key.lower()}", (data.get("progress") or {}).get(key), attrs
            )
        details = data.get("details") or {}
        redis = details.get("redis") or {}
        sync = redis.get("sync") or {}
        if not self._fresh(sync):
            sync = {}
        proxy = redis.get("proxy") or {}
        if not self._fresh(proxy):
            proxy = {}
        database = details.get("database") or {}
        if not self._fresh(database) or database.get("observationAvailable") is False:
            database = {}
        for key in ("backlog", "copied", "lag_ms", "oldestDirtyAgeMs"):
            self.measure(
                f"foxtrot.redis.{key.lower()}",
                sync.get(key),
                attrs | {"direction": str(sync.get("direction", "UNKNOWN"))},
            )
        for key in ("inflight_writes", "open_tx", "last_pause_ms", "pubsub_sessions"):
            self.measure(f"foxtrot.redis.proxy.{key}", proxy.get(key), attrs)
        for key in (
            "sourceWriterSessions",
            "targetWriterSessions",
            "sourcePreparedTransactions",
            "targetPreparedTransactions",
        ):
            self.measure(f"foxtrot.database.{key.lower()}", database.get(key), attrs)
        for direction in ("forward", "reverse"):
            flow = (data.get("replication") or {}).get(direction) or {}
            for key in ("backlog", "lagSeconds"):
                self.measure(
                    f"foxtrot.replication.{key.lower()}",
                    flow.get(key),
                    attrs | {"direction": direction},
                )
        for actor in data.get("actors") or []:
            if not self._fresh(actor):
                continue
            for field in ("inFlight", "activeWebSockets"):
                self.measure(
                    f"foxtrot.app.{field.lower()}",
                    actor.get(field),
                    attrs | {"actor.id": str(actor.get("actorId", "UNKNOWN"))},
                )
        for key in ("total", "failed", "timeouts", "p95Ms", "missingSamples"):
            self.measure(f"foxtrot.probe.{key.lower()}", data.get(key), attrs)

    @staticmethod
    def _fresh(data: dict[str, Any]) -> bool:
        try:
            at = datetime.fromisoformat(str(data.get("observedAt")).replace("Z", "+00:00"))
            return 0 <= (datetime.now(UTC) - at).total_seconds() <= 15
        except (ValueError, TypeError):
            return False

    @contextmanager
    def operation(
        self,
        action: str,
        *,
        workflow: str = "",
        migration: str = "",
        operation: str = "",
        parent: dict[str, str] | None = None,
    ) -> Iterator[Any]:
        attrs = {
            "workflow.id": workflow,
            "migration.id": migration,
            "operation.id": operation,
            "migration.action": action,
        }
        start = time.monotonic()
        with self.tracer.start_as_current_span(
            f"migration.{action.lower()}",
            context=propagator.extract(parent) if parent else None,
            attributes={k: v[:128] for k, v in attrs.items() if v},
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            result = "ok"
            try:
                yield span
            except BaseException as exc:
                result = "error"
                span.set_status(trace.Status(trace.StatusCode.ERROR, type(exc).__name__))
                raise
            finally:
                if span.is_recording() and span.status.status_code == trace.StatusCode.ERROR:
                    result = "error"
                if self.enabled:
                    labels = {"migration.action": action, "result": result}
                    self.duration.record(time.monotonic() - start, labels)
                    self.operations.add(1, labels)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        for provider in (self.logs, self.metrics, self.traces):
            if provider:
                try:
                    provider.force_flush(timeout_millis=3000)
                    provider.shutdown()
                except Exception:
                    log.warning("telemetry shutdown did not complete")


class TraceMiddleware:
    """Trace context across service HTTP calls; never capture headers, query or bodies."""

    def __init__(self, app: Any, telemetry: Telemetry) -> None:
        self.app, self.telemetry = app, telemetry

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http" or not self.telemetry.enabled:
            await self.app(scope, receive, send)
            return
        headers = {
            k.decode(): v.decode()
            for k, v in scope.get("headers", [])
            if k in (b"traceparent", b"tracestate")
        }
        with self.telemetry.tracer.start_as_current_span(
            scope["method"],
            context=propagator.extract(headers),
            kind=trace.SpanKind.SERVER,
            record_exception=False,
            set_status_on_exception=False,
        ) as span:

            async def traced_send(message: Any) -> None:
                if message["type"] == "http.response.start":
                    span.set_attribute("http.response.status_code", message["status"])
                    if message["status"] >= 500:
                        span.set_status(trace.Status(trace.StatusCode.ERROR, "HTTP_SERVER_ERROR"))
                await send(message)

            try:
                await self.app(scope, receive, traced_send)
            except BaseException as exc:
                span.set_status(trace.Status(trace.StatusCode.ERROR, type(exc).__name__))
                raise
            finally:
                route = getattr(scope.get("route"), "path", "unmatched")
                span.update_name(f"{scope['method']} {route}")
                span.set_attribute("http.route", route)


_instances: dict[str, Telemetry] = {}


def get_telemetry(module: str) -> Telemetry:
    if module not in _instances:
        try:
            _instances[module] = Telemetry(module)
        except Exception:
            log.warning("OTEL initialization failed; exporter disabled")
            _instances[module] = Telemetry(module, disabled=True)
        atexit.register(_instances[module].close)
    return _instances[module]


async def publish_loop(telemetry: Telemetry, collect: Any) -> None:
    """Module-owned collection. Export failures never control migration execution."""
    if not telemetry.enabled:
        return
    while True:
        try:
            rows = (
                await collect()
                if inspect.iscoroutinefunction(collect)
                else await asyncio.to_thread(collect)
            )
            for kind, data in rows[:256]:
                telemetry.snapshot(kind, data)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.warning("telemetry observation unavailable")
        await asyncio.sleep(5)
