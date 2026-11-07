import json
import unittest
from datetime import datetime, timezone

from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter, SimpleLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from statemig.telemetry import Telemetry


class OtelSDKTest(unittest.TestCase):
    def test_three_signals_context_and_redaction(self):
        logs, metrics, traces = InMemoryLogRecordExporter(), InMemoryMetricReader(), InMemorySpanExporter()
        lp = LoggerProvider(shutdown_on_exit=False)
        lp.add_log_record_processor(SimpleLogRecordProcessor(logs))
        tp = TracerProvider(shutdown_on_exit=False)
        tp.add_span_processor(SimpleSpanProcessor(traces))
        telemetry = Telemetry("redis", providers=(lp, MeterProvider(metric_readers=[metrics], shutdown_on_exit=False), tp))
        now = datetime.now(timezone.utc).isoformat()
        with telemetry.operation("CUTOVER", workflow="w", operation="op"):
            parent = telemetry.headers()
            with telemetry.operation("VERIFY", workflow="w", parent=parent):
                self.assertTrue(telemetry.snapshot("REDIS", {
                    "workflowId": "w", "migrationId": "r", "observed": {"observedAt": now},
                    "details": {"redis": {"proxy": {"inflight_writes": 2, "observedAt": now,
                                                   "password": "secret"}}},
                    "dsn": "secret", "plan": {"token": "secret"},
                }))
        records = logs.get_finished_logs()
        self.assertEqual(len(records), 1)
        body = records[0].log_record.body
        self.assertNotIn("secret", body)
        self.assertEqual(json.loads(body)["data"]["details"]["redis"]["proxy"]["inflight_writes"], 2)
        spans = traces.get_finished_spans()
        self.assertEqual(spans[0].parent.span_id, spans[1].context.span_id)
        self.assertEqual(records[0].log_record.trace_id, spans[0].context.trace_id)
        data = metrics.get_metrics_data()
        names = {m.name for r in data.resource_metrics for scope in r.scope_metrics for m in scope.metrics}
        self.assertIn("foxtrot.redis.proxy.inflight_writes", names)
        self.assertIn("foxtrot.operation.duration", names)
        telemetry.close()
        telemetry.close()

    def test_sensitive_exception_message_never_becomes_span_event(self):
        logs, spans = LoggerProvider(shutdown_on_exit=False), InMemorySpanExporter()
        traces = TracerProvider(shutdown_on_exit=False)
        traces.add_span_processor(SimpleSpanProcessor(spans))
        telemetry = Telemetry("database", providers=(logs, MeterProvider(shutdown_on_exit=False), traces))
        with self.assertRaises(ValueError):
            with telemetry.operation("COPY", workflow="w"):
                raise ValueError("password=secret")
        span = spans.get_finished_spans()[0]
        self.assertEqual(span.status.status_code.name, "ERROR")
        self.assertNotIn("secret", str(span.events) + str(span.status.description))
        telemetry.close()
