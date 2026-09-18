"""Read credential selection at quality and proposer read seams."""

from __future__ import annotations

import importlib.util
import sys
import types
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from airflow_dq_agent.agent.runner import (
    _get_observed_schema_tool,
    _sample_failing_rows_tool,
    get_observed_schema,
    sample_failing_rows,
)
from airflow_dq_agent.demo import seeded_failure_report
from airflow_dq_agent.quality.suite import run_quality_suite
from airflow_dq_agent.warehouse.db import make_engine

DAG_PATH = Path(__file__).resolve().parents[2] / "dags" / "dq_daily.py"

READ_DSN = "postgresql+psycopg://reader:read-secret@read-host:65432/warehouse"
WAREHOUSE_DSN = "postgresql+psycopg://warehouse:wh-secret@wh-host:5433/warehouse"
APPLY_DSN = "postgresql+psycopg://applier:ap-secret@ap-host:5433/warehouse"
OVERRIDE_DSN = "postgresql+psycopg://override:ov-secret@ov-host:65432/warehouse"
SAMPLE_CHECK_ID = "fact_orders.total_amount.completeness"
OBSERVED_TABLE = "dim_customer"
SUITE_ENGINE = "airflow_dq_agent.quality.suite.make_engine"
RUNNER_ENGINE = "airflow_dq_agent.agent.runner.make_engine"


def _configure_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("READ_DSN", READ_DSN)
    monkeypatch.setenv("WAREHOUSE_DSN", WAREHOUSE_DSN)
    monkeypatch.setenv("APPLY_DSN", APPLY_DSN)


def _capture_engine(monkeypatch: pytest.MonkeyPatch, target: str) -> list[str | None]:
    seen: list[str | None] = []

    def fake_make_engine(dsn: str | None = None):
        seen.append(dsn)
        raise RuntimeError("engine-probe")

    monkeypatch.setattr(target, fake_make_engine)
    return seen


def test_quality_suite_uses_configured_read_dsn_when_no_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_credentials(monkeypatch)
    seen = _capture_engine(monkeypatch, SUITE_ENGINE)

    with pytest.raises(RuntimeError, match="engine-probe"):
        run_quality_suite()

    assert seen == [READ_DSN]


def test_quality_suite_explicit_override_wins_over_configured_read_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_credentials(monkeypatch)
    seen = _capture_engine(monkeypatch, SUITE_ENGINE)

    with pytest.raises(RuntimeError, match="engine-probe"):
        run_quality_suite(OVERRIDE_DSN)

    assert seen == [OVERRIDE_DSN]


def test_quality_suite_uses_warehouse_only_when_no_read_override_or_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("READ_DSN", raising=False)
    monkeypatch.setenv("WAREHOUSE_DSN", WAREHOUSE_DSN)
    monkeypatch.setenv("APPLY_DSN", APPLY_DSN)
    seen = _capture_engine(monkeypatch, SUITE_ENGINE)

    with pytest.raises(RuntimeError, match="engine-probe"):
        run_quality_suite()

    assert seen == [WAREHOUSE_DSN]


def test_sample_failing_rows_uses_configured_read_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_credentials(monkeypatch)
    seen = _capture_engine(monkeypatch, RUNNER_ENGINE)

    with pytest.raises(RuntimeError, match="engine-probe"):
        sample_failing_rows(SAMPLE_CHECK_ID)

    assert seen == [READ_DSN]


def test_sample_failing_rows_explicit_override_wins_over_configured_read_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_credentials(monkeypatch)
    seen = _capture_engine(monkeypatch, RUNNER_ENGINE)

    with pytest.raises(RuntimeError, match="engine-probe"):
        sample_failing_rows(SAMPLE_CHECK_ID, dsn=OVERRIDE_DSN)

    assert seen == [OVERRIDE_DSN]


def test_get_observed_schema_uses_configured_read_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_credentials(monkeypatch)
    seen = _capture_engine(monkeypatch, RUNNER_ENGINE)

    with pytest.raises(RuntimeError, match="engine-probe"):
        get_observed_schema(OBSERVED_TABLE)

    assert seen == [READ_DSN]


def test_get_observed_schema_explicit_override_wins_over_configured_read_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_credentials(monkeypatch)
    seen = _capture_engine(monkeypatch, RUNNER_ENGINE)

    with pytest.raises(RuntimeError, match="engine-probe"):
        get_observed_schema(OBSERVED_TABLE, dsn=OVERRIDE_DSN)

    assert seen == [OVERRIDE_DSN]


def test_live_proposer_read_tools_use_configured_read_dsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_credentials(monkeypatch)
    seen = _capture_engine(monkeypatch, RUNNER_ENGINE)

    with pytest.raises(RuntimeError, match="engine-probe"):
        _sample_failing_rows_tool(SAMPLE_CHECK_ID)
    with pytest.raises(RuntimeError, match="engine-probe"):
        _get_observed_schema_tool(OBSERVED_TABLE)

    assert seen == [READ_DSN, READ_DSN]


def test_quality_suite_read_failure_does_not_fall_back_to_warehouse_or_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_credentials(monkeypatch)
    seen = _capture_engine(monkeypatch, SUITE_ENGINE)

    with pytest.raises(RuntimeError):
        run_quality_suite()

    assert seen == [READ_DSN]
    assert WAREHOUSE_DSN not in seen
    assert APPLY_DSN not in seen


def test_sample_and_schema_read_failure_does_not_fall_back_to_warehouse_or_apply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_credentials(monkeypatch)
    seen = _capture_engine(monkeypatch, RUNNER_ENGINE)

    with pytest.raises(RuntimeError):
        sample_failing_rows(SAMPLE_CHECK_ID)
    with pytest.raises(RuntimeError):
        get_observed_schema(OBSERVED_TABLE)
    with pytest.raises(RuntimeError):
        _sample_failing_rows_tool(SAMPLE_CHECK_ID)

    assert seen == [READ_DSN, READ_DSN, READ_DSN]
    assert WAREHOUSE_DSN not in seen
    assert APPLY_DSN not in seen


def _assert_no_credential_leak(exc: BaseException, *, secret: str, user: str, dsn: str) -> None:
    parts = [str(exc), repr(exc)]
    current: BaseException | None = exc
    seen_ids: set[int] = set()
    while current is not None and id(current) not in seen_ids:
        seen_ids.add(id(current))
        parts.append(str(current))
        parts.append(repr(current))
        current = current.__cause__ or current.__context__
    text = "\n".join(parts)
    assert secret not in text
    assert user not in text
    assert dsn not in text
    assert "postgresql+" not in text.lower()


def test_quality_suite_read_failure_does_not_expose_dsn_or_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "s3cret-password"
    user = "ci-reader"
    bad_dsn = f"postgresql+psycopg://{user}:{secret}@127.0.0.1:1/warehouse"
    monkeypatch.setenv("READ_DSN", bad_dsn)
    monkeypatch.setenv("WAREHOUSE_DSN", WAREHOUSE_DSN)
    monkeypatch.setenv("APPLY_DSN", APPLY_DSN)

    with pytest.raises(RuntimeError, match="Read connection failed") as excinfo:
        run_quality_suite()

    _assert_no_credential_leak(excinfo.value, secret=secret, user=user, dsn=bad_dsn)


def test_sample_failing_rows_read_failure_does_not_expose_dsn_or_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "s3cret-password"
    user = "ci-reader"
    bad_dsn = f"postgresql+psycopg://{user}:{secret}@127.0.0.1:1/warehouse"
    monkeypatch.setenv("READ_DSN", bad_dsn)
    monkeypatch.setenv("WAREHOUSE_DSN", WAREHOUSE_DSN)

    with pytest.raises(RuntimeError, match="Read connection failed") as excinfo:
        sample_failing_rows(SAMPLE_CHECK_ID)

    _assert_no_credential_leak(excinfo.value, secret=secret, user=user, dsn=bad_dsn)


def test_get_observed_schema_read_failure_does_not_expose_dsn_or_credential(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "s3cret-password"
    user = "ci-reader"
    bad_dsn = f"postgresql+psycopg://{user}:{secret}@127.0.0.1:1/warehouse"
    monkeypatch.setenv("READ_DSN", bad_dsn)
    monkeypatch.setenv("WAREHOUSE_DSN", WAREHOUSE_DSN)

    with pytest.raises(RuntimeError, match="Read connection failed") as excinfo:
        get_observed_schema(OBSERVED_TABLE)

    _assert_no_credential_leak(excinfo.value, secret=secret, user=user, dsn=bad_dsn)


def test_make_engine_without_override_keeps_warehouse_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_credentials(monkeypatch)
    rendered = make_engine().url.render_as_string(hide_password=False)
    assert rendered == WAREHOUSE_DSN
    assert READ_DSN not in rendered
    assert APPLY_DSN not in rendered


@pytest.mark.parametrize("operation", ["suite", "samples", "schema"])
def test_malformed_read_dsn_has_controlled_error(
    operation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(
        "READ_DSN", "postgresql+psycopg://reader:password@localhost:private-port/warehouse"
    )
    with pytest.raises(RuntimeError, match=r"^Read connection failed$"):
        if operation == "suite":
            run_quality_suite()
        elif operation == "samples":
            sample_failing_rows(SAMPLE_CHECK_ID)
        else:
            get_observed_schema(OBSERVED_TABLE)


def test_sample_failing_rows_uses_warehouse_only_when_no_read_override_or_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("READ_DSN", raising=False)
    monkeypatch.setenv("WAREHOUSE_DSN", WAREHOUSE_DSN)
    monkeypatch.setenv("APPLY_DSN", APPLY_DSN)
    seen = _capture_engine(monkeypatch, RUNNER_ENGINE)

    with pytest.raises(RuntimeError, match="engine-probe"):
        sample_failing_rows(SAMPLE_CHECK_ID)

    assert seen == [WAREHOUSE_DSN]


def test_get_observed_schema_uses_warehouse_only_when_no_read_override_or_setting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("READ_DSN", raising=False)
    monkeypatch.setenv("WAREHOUSE_DSN", WAREHOUSE_DSN)
    monkeypatch.setenv("APPLY_DSN", APPLY_DSN)
    seen = _capture_engine(monkeypatch, RUNNER_ENGINE)

    with pytest.raises(RuntimeError, match="engine-probe"):
        get_observed_schema(OBSERVED_TABLE)

    assert seen == [WAREHOUSE_DSN]


def _load_dag_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[types.ModuleType, dict[str, Callable[..., Any]]]:
    unused = "postgresql+psycopg://dq:dq@localhost:1/unused-warehouse"
    monkeypatch.setenv("WAREHOUSE_DSN", unused)
    monkeypatch.delenv("READ_DSN", raising=False)
    monkeypatch.delenv("AUDIT_DSN", raising=False)
    monkeypatch.delenv("APPLY_DSN", raising=False)
    monkeypatch.setenv("LLM_MODE", "stub")
    monkeypatch.setenv("APPLY_MODE", "off")

    exceptions_module = types.ModuleType("airflow.exceptions")

    class AirflowSkipException(Exception): ...

    exceptions_module.AirflowSkipException = AirflowSkipException
    sdk_module = types.ModuleType("airflow.sdk")
    tasks: dict[str, Callable[..., Any]] = {}

    def _stub_dag(
        *_args: Any, **_kwargs: Any
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def wrap(fn: Callable[..., Any]) -> Callable[..., Any]:
            return fn

        return wrap

    def _stub_task(
        fn: Callable[..., Any] | None = None, **_kwargs: Any
    ) -> Callable[..., Any] | Callable[[Callable[..., Any]], Callable[..., Any]]:
        def register(candidate: Callable[..., Any]) -> Callable[..., Any]:
            tasks[candidate.__name__] = candidate

            def xcom_reference(*_call_args: Any, **_call_kwargs: Any) -> None:
                return None

            xcom_reference.__name__ = candidate.__name__
            return xcom_reference

        return register if fn is None else register(fn)

    sdk_module.dag = _stub_dag
    sdk_module.task = _stub_task
    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.exceptions", exceptions_module)
    monkeypatch.setitem(sys.modules, "airflow.sdk", sdk_module)

    spec = importlib.util.spec_from_file_location("dq_daily_read_credentials", DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, tasks


def test_dag_suite_uses_bound_settings_not_stale_process_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, tasks = _load_dag_tasks(monkeypatch)
    seen: list[str | None] = []

    def fake_suite(dsn: str | None = None):
        seen.append(dsn)
        return seeded_failure_report()

    module.settings = module.settings.model_copy(
        update={"warehouse_dsn": OVERRIDE_DSN, "read_dsn": None}
    )
    monkeypatch.setattr(module, "run_quality_suite", fake_suite)
    tasks["run_suite_task"]()

    assert seen == [OVERRIDE_DSN]
