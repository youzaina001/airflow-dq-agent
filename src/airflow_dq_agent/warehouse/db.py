from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from airflow_dq_agent.config import get_settings

DDL_PATH = Path(__file__).with_name("ddl.sql")


def make_engine(dsn: str | None = None) -> Engine:
    url = dsn or get_settings().warehouse_dsn
    return create_engine(url, future=True, pool_pre_ping=True)


@contextmanager
def connect(engine: Engine | None = None) -> Iterator[Connection]:
    eng = engine or make_engine()
    with eng.begin() as conn:
        yield conn


def apply_ddl(engine: Engine) -> None:
    sql = DDL_PATH.read_text(encoding="utf-8")
    with engine.begin() as conn:
        conn.execute(text(sql))
