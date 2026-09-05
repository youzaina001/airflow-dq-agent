"""Lookup adapters for durable Audit Lineage events."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from sqlalchemy import text

from airflow_dq_agent.config import get_settings
from airflow_dq_agent.contracts.models import AuditEvent
from airflow_dq_agent.warehouse.db import make_engine


class AuditLineageLookup(Protocol):
    """Resolve one persisted Audit Lineage event by ID."""

    def get(self, event_id: str) -> AuditEvent | None: ...


class InMemoryAuditRepository:
    """Tiny in-memory lookup for tests; production injects a durable adapter."""

    def __init__(self, events: Iterable[AuditEvent] = ()) -> None:
        self._events = {event.event_id: event for event in events}

    def add(self, event: AuditEvent) -> None:
        self._events[event.event_id] = event

    def get(self, event_id: str) -> AuditEvent | None:
        return self._events.get(event_id)


def _event_from_body(body: object) -> AuditEvent:
    if isinstance(body, (bytes, str)):
        return AuditEvent.model_validate_json(body)
    return AuditEvent.model_validate(body)


class PostgresAuditRepository:
    """Read one Audit Lineage event from the Postgres traces table."""

    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn

    def get(self, event_id: str) -> AuditEvent | None:
        settings = get_settings()
        engine = make_engine(self._dsn or settings.audit_dsn or settings.warehouse_dsn)
        with engine.connect() as connection:
            row = connection.execute(
                text("SELECT body FROM dq.traces WHERE trace_id = :trace_id"),
                {"trace_id": event_id},
            ).first()
        if row is None:
            return None
        return _event_from_body(row[0])
