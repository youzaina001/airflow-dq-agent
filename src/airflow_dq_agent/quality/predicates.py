"""Compile one CheckSpec into Polars, sample SQL, and apply predicates."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Protocol

import polars as pl

from airflow_dq_agent.contracts.models import Dimension
from airflow_dq_agent.contracts.tables import get_table_contract

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SCHEMA = "warehouse"


class CheckView(Protocol):
    check_id: str
    table: str
    column: str | None
    dimension: Dimension
    contains: str | None
    window_start_column: str | None
    window_end_column: str | None
    business_key: list[str] | None


def _ident(name: str) -> str:
    if not _IDENT.fullmatch(name):
        raise ValueError(f"Refusing unvalidated identifier {name!r}")
    return name


def _quoted(name: str) -> str:
    return f'"{_ident(name)}"'


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _require_column(spec: CheckView) -> str:
    if spec.column is None:
        raise ValueError(f"{spec.check_id} does not name a column")
    return _ident(spec.column)


def _primary_key(table: str) -> str:
    contract = get_table_contract(table)
    if len(contract.primary_key) != 1:
        raise ValueError(f"{table} does not have a single-column primary key")
    return _ident(contract.primary_key[0])


def _foreign_key(spec: CheckView) -> tuple[str, str, str]:
    column = _require_column(spec)
    contract = get_table_contract(spec.table)
    for foreign_key in contract.foreign_keys:
        if foreign_key[0] == column:
            return foreign_key
    raise ValueError(f"{spec.check_id} column is not a contracted foreign key")


def _allowed_values(spec: CheckView) -> list[str]:
    column = _require_column(spec)
    allowed = get_table_contract(spec.table).column(column).allowed_values
    if not allowed:
        raise ValueError(f"{spec.check_id} has no allowed_values on the table contract")
    return list(allowed)


def _like_pattern(needle: str) -> str:
    if any(character in needle for character in "%_\\'"):
        raise ValueError(f"contains {needle!r} cannot be compiled to LIKE")
    return f"'%{needle}%'"


def _in_list(values: Sequence[str]) -> str:
    return ", ".join(_sql_str(value) for value in values)


def _qualified(table: str) -> str:
    return f"{_SCHEMA}.{_ident(table)}"


def sample_sql_for(spec: CheckView) -> str:
    table = spec.table
    if spec.dimension is Dimension.SCHEMA_DRIFT:
        return (
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = {_sql_str(_SCHEMA)} AND table_name = {_sql_str(_ident(table))} "
            "ORDER BY ordinal_position LIMIT :limit"
        )
    pk = _primary_key(table)
    qualified = _qualified(table)
    if spec.dimension is Dimension.COMPLETENESS:
        column = _require_column(spec)
        return (
            f"SELECT {pk}, {column} FROM {qualified} "
            f"WHERE {column} IS NULL ORDER BY {pk} LIMIT :limit"
        )
    if spec.dimension is Dimension.VALIDITY:
        return _validity_sample_sql(spec, qualified, pk)
    if spec.dimension is Dimension.UNIQUENESS:
        keys = spec.business_key
        if not keys:
            raise ValueError(f"{spec.check_id} does not declare a business_key")
        columns = ", ".join(_ident(key) for key in keys)
        return (
            f"SELECT {columns}, COUNT(*) AS n FROM {qualified} "
            f"GROUP BY {columns} HAVING COUNT(*) > 1 LIMIT :limit"
        )
    if spec.dimension is Dimension.REFERENTIAL_INTEGRITY:
        fk_column, ref_table, ref_column = _foreign_key(spec)
        return (
            f"SELECT t.{pk}, t.{_ident(fk_column)} FROM {qualified} t "
            f"LEFT JOIN {_qualified(ref_table)} r "
            f"ON r.{_ident(ref_column)} = t.{_ident(fk_column)} "
            f"WHERE r.{_ident(ref_column)} IS NULL ORDER BY t.{pk} LIMIT :limit"
        )
    raise ValueError(f"{spec.check_id} has no sample SQL for {spec.dimension}")


def _validity_sample_sql(spec: CheckView, qualified: str, pk: str) -> str:
    column = _require_column(spec)
    if spec.contains is not None:
        pattern = _like_pattern(spec.contains)
        return (
            f"SELECT {pk}, {column} FROM {qualified} "
            f"WHERE {column} IS NULL OR {column} NOT LIKE {pattern} "
            f"ORDER BY {pk} LIMIT :limit"
        )
    if spec.window_start_column and spec.window_end_column:
        start = _ident(spec.window_start_column)
        end = _ident(spec.window_end_column)
        return (
            f"SELECT {pk}, {column}, {start}, {end} FROM {qualified} "
            f"WHERE {column} IS NOT NULL AND ({column} < {start} OR {column} > {end}) "
            f"ORDER BY {pk} LIMIT :limit"
        )
    allowed = _in_list(_allowed_values(spec))
    return (
        f"SELECT {pk}, {column} FROM {qualified} "
        f"WHERE {column} NOT IN ({allowed}) ORDER BY {pk} LIMIT :limit"
    )


def quarantine_predicate_for(spec: CheckView) -> str | None:
    if spec.dimension is Dimension.COMPLETENESS:
        column = _quoted(_require_column(spec))
        return f"t.{column} IS NULL"
    if spec.dimension is Dimension.VALIDITY:
        column = _quoted(_require_column(spec))
        if spec.contains is not None:
            pattern = _like_pattern(spec.contains)
            return f"t.{column} IS NULL OR t.{column} NOT LIKE {pattern}"
        if spec.window_start_column and spec.window_end_column:
            start = _quoted(spec.window_start_column)
            end = _quoted(spec.window_end_column)
            return f"t.{column} IS NOT NULL AND (t.{column} < t.{start} OR t.{column} > t.{end})"
        allowed = _in_list(_allowed_values(spec))
        return f"t.{column} NOT IN ({allowed})"
    return None


def failed_rows(spec: CheckView, frames: Mapping[str, pl.DataFrame]) -> pl.DataFrame:
    if spec.dimension is Dimension.SCHEMA_DRIFT:
        return _schema_drift_rows(spec, frames)
    df = frames[spec.table]
    if spec.dimension is Dimension.COMPLETENESS:
        return df.filter(pl.col(_require_column(spec)).is_null())
    if spec.dimension is Dimension.VALIDITY:
        return _validity_rows(spec, df)
    if spec.dimension is Dimension.UNIQUENESS:
        keys = spec.business_key
        if not keys:
            raise ValueError(f"{spec.check_id} does not declare a business_key")
        duplicates = df.group_by(keys).len().filter(pl.col("len") > 1).select(keys)
        return df.join(duplicates, on=keys, how="inner")
    if spec.dimension is Dimension.REFERENTIAL_INTEGRITY:
        fk_column, ref_table, ref_column = _foreign_key(spec)
        right = frames[ref_table].select(pl.col(ref_column).alias("_ref"))
        pk = _primary_key(spec.table)
        return (
            frames[spec.table]
            .join(right, left_on=fk_column, right_on="_ref", how="anti")
            .select([pk, fk_column])
        )
    raise ValueError(f"{spec.check_id} has no evaluator for {spec.dimension}")


def _validity_rows(spec: CheckView, df: pl.DataFrame) -> pl.DataFrame:
    column = _require_column(spec)
    if spec.contains is not None:
        return df.filter(~pl.col(column).str.contains(spec.contains, literal=True).fill_null(False))
    if spec.window_start_column and spec.window_end_column:
        start = spec.window_start_column
        end = spec.window_end_column
        return df.filter(
            pl.col(column).is_not_null()
            & ((pl.col(column) < pl.col(start)) | (pl.col(column) > pl.col(end)))
        )
    allowed = _allowed_values(spec)
    return df.filter(~pl.col(column).is_in(allowed))


def _schema_drift_rows(spec: CheckView, frames: Mapping[str, pl.DataFrame]) -> pl.DataFrame:
    expected = set(get_table_contract(spec.table).column_names)
    observed = set(frames[spec.table].columns)
    extra = sorted(observed - expected)
    missing = sorted(expected - observed)
    rows = [{"kind": "extra", "column": column} for column in extra] + [
        {"kind": "missing", "column": column} for column in missing
    ]
    if not rows:
        return pl.DataFrame({"kind": [], "column": []})
    return pl.DataFrame(rows)
