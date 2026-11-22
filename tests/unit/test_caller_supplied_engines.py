"""Caller-supplied read engine and audit sink on the quality and audit path (ADR-0002)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.engine import Engine
from tests.settings_isolation import use_settings_without_dotenv

from airflow_dq_agent.contracts.models import AuditEvent, QualitySuiteReport
from airflow_dq_agent.demo import green_report
from airflow_dq_agent.quality.suite import run_quality_suite
from airflow_dq_agent.traces.writer import PostgresAuditSink, record_quality_report

SUITE = "airflow_dq_agent.quality.suite"
WRITER = "airflow_dq_agent.traces.writer"


@pytest.fixture(autouse=True)
def _isolated(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    use_settings_without_dotenv(monkeypatch)
    monkeypatch.setenv("TRACE_POSTGRES", "false")
    monkeypatch.setenv("APPLY_MODE", "off")
    monkeypatch.setenv("TRACES_DIR", str(tmp_path / "traces"))

    def no_default_engine(dsn: str | None = None) -> Engine:
        raise AssertionError(f"make_engine called with {dsn!r}")

    monkeypatch.setattr(f"{SUITE}.make_engine", no_default_engine)
    monkeypatch.setattr(f"{WRITER}.make_engine", no_default_engine)


class RecordingSink:
    def __init__(self, *, fail: bool = False) -> None:
        self.events: list[AuditEvent] = []
        self.check_runs: list[QualitySuiteReport] = []
        self._fail = fail

    def append(self, event: AuditEvent) -> None:
        if self._fail:
            raise RuntimeError("scoped audit write refused")
        self.events.append(event)

    def record_check_runs(self, report: QualitySuiteReport) -> None:
        if self._fail:
            raise RuntimeError("scoped audit write refused")
        self.check_runs.append(report)


class RecordingConnection:
    def __init__(self, statements: list[tuple[str, dict[str, Any]]]) -> None:
        self._statements = statements

    def __enter__(self) -> RecordingConnection:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, statement: Any, params: dict[str, Any]) -> None:
        self._statements.append((str(statement), params))


class RecordingEngine:
    def __init__(self) -> None:
        self.statements: list[tuple[str, dict[str, Any]]] = []

    def begin(self) -> RecordingConnection:
        return RecordingConnection(self.statements)


def _capture_load_frames(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    seen: list[object] = []

    def fake_load_frames(engine: object) -> dict[str, Any]:
        seen.append(engine)
        return {}

    monkeypatch.setattr(f"{SUITE}.load_frames", fake_load_frames)
    return seen


def _jsonl_lines(tmp_path: Path) -> list[str]:
    path = tmp_path / "traces" / "agent-traces.jsonl"
    return path.read_text(encoding="utf-8").splitlines() if path.exists() else []


def test_suite_reads_through_the_supplied_engine_and_writes_the_supplied_sink(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    seen = _capture_load_frames(monkeypatch)
    read_engine = object()
    sink = RecordingSink()

    report = run_quality_suite(read_engine=read_engine, audit_sink=sink)  # type: ignore[arg-type]

    assert seen == [read_engine]
    assert [r.run_id for r in sink.check_runs] == [report.run_id]
    assert [e.event_id for e in sink.events] == [report.audit_event_id]
    assert len(_jsonl_lines(tmp_path)) == 1


def test_suite_supplied_sink_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    _capture_load_frames(monkeypatch)

    with pytest.raises(RuntimeError, match="scoped audit write refused"):
        run_quality_suite(read_engine=object(), audit_sink=RecordingSink(fail=True))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"read_engine": object()},
        {"audit_sink": RecordingSink()},
        {"read_engine": object(), "audit_sink": RecordingSink(), "dsn": "postgresql://x/y"},
    ],
    ids=["engine-without-sink", "sink-without-engine", "engine-with-dsn"],
)
def test_suite_refuses_partial_or_conflicting_supply(
    monkeypatch: pytest.MonkeyPatch, kwargs: dict[str, Any]
) -> None:
    seen = _capture_load_frames(monkeypatch)

    with pytest.raises(ValueError, match="run_quality_suite"):
        run_quality_suite(**kwargs)

    assert seen == []


def test_record_quality_report_writes_supplied_sink_even_with_trace_postgres_off(
    tmp_path: Path,
) -> None:
    sink = RecordingSink()
    report = green_report()

    event = record_quality_report(report, audit_sink=sink)

    assert [e.event_id for e in sink.events] == [event.event_id]
    assert [r.run_id for r in sink.check_runs] == [report.run_id]
    assert len(_jsonl_lines(tmp_path)) == 1


def test_record_quality_report_supplied_sink_failure_skips_jsonl(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="scoped audit write refused"):
        record_quality_report(green_report(), audit_sink=RecordingSink(fail=True))

    assert _jsonl_lines(tmp_path) == []


@pytest.mark.parametrize(
    "kwargs",
    [{"dsn": "postgresql://x/y"}, {"mirror_postgres": False}, {"mirror_postgres": True}],
    ids=["dsn", "mirror-off", "mirror-on"],
)
def test_record_quality_report_refuses_sink_with_ignored_options(kwargs: dict[str, Any]) -> None:
    sink = RecordingSink()

    with pytest.raises(ValueError, match="record_quality_report"):
        record_quality_report(green_report(), audit_sink=sink, **kwargs)

    assert sink.events == []


def test_postgres_sink_writes_through_the_supplied_engine() -> None:
    engine = RecordingEngine()
    sink = PostgresAuditSink(engine=engine)  # type: ignore[arg-type]

    event = record_quality_report(green_report(), audit_sink=sink)

    tables = [statement.split()[2] for statement, _ in engine.statements]
    assert set(tables) == {"dq.check_runs", "dq.traces"}
    assert engine.statements[-1][1]["trace_id"] == event.event_id


def test_postgres_sink_refuses_both_dsn_and_engine() -> None:
    with pytest.raises(ValueError, match="PostgresAuditSink"):
        PostgresAuditSink("postgresql://x/y", engine=RecordingEngine())  # type: ignore[arg-type]
