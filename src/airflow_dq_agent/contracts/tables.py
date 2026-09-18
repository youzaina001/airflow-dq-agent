"""Table contracts are the schema source of truth. Drift is measured against these, not the LLM."""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, Field, field_validator

DType = Literal["int64", "float64", "utf8", "date", "datetime", "bool"]
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _require_ident(value: str) -> str:
    if not _IDENT.fullmatch(value):
        raise ValueError(f"invalid identifier {value!r}")
    return value


class ColumnContract(BaseModel):
    name: str
    dtype: DType
    nullable: bool = False
    unique: bool = False
    allowed_values: list[str] | None = None
    description: str = ""

    @field_validator("name")
    @classmethod
    def name_must_be_identifier(cls, value: str) -> str:
        return _require_ident(value)


class TableContract(BaseModel):
    table: str
    schema_name: str = "warehouse"
    grain: str
    description: str
    columns: list[ColumnContract]
    primary_key: list[str]
    foreign_keys: list[tuple[str, str, str]] = Field(
        default_factory=list,
        description="(column, ref_table, ref_column)",
    )

    @field_validator("table", "schema_name")
    @classmethod
    def names_must_be_identifiers(cls, value: str) -> str:
        return _require_ident(value)

    @field_validator("primary_key")
    @classmethod
    def primary_key_must_be_identifiers(cls, value: list[str]) -> list[str]:
        return [_require_ident(item) for item in value]

    @field_validator("foreign_keys")
    @classmethod
    def foreign_keys_must_be_identifiers(
        cls, value: list[tuple[str, str, str]]
    ) -> list[tuple[str, str, str]]:
        return [
            (_require_ident(column), _require_ident(ref_table), _require_ident(ref_column))
            for column, ref_table, ref_column in value
        ]

    @property
    def contract_id(self) -> str:
        return f"{self.schema_name}.{self.table}"

    @property
    def qualified(self) -> str:
        return f"{self.schema_name}.{self.table}"

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> ColumnContract:
        for col in self.columns:
            if col.name == name:
                return col
        raise KeyError(f"{self.table}.{name} is not in the contract")

    def has_column(self, name: str) -> bool:
        return any(c.name == name for c in self.columns)


TABLE_CONTRACTS: dict[str, TableContract] = {}


def register_contract(contract: TableContract) -> None:
    """Add one table contract to the process-wide catalog."""
    key = contract.table.split(".")[-1]
    if key in TABLE_CONTRACTS:
        raise ValueError(f"table {key!r} is already registered")
    TABLE_CONTRACTS[key] = contract


def get_table_contract(table: str) -> TableContract:
    key = table.split(".")[-1]
    if key not in TABLE_CONTRACTS:
        raise KeyError(f"Unknown table {table!r}. Contracts: {sorted(TABLE_CONTRACTS)}")
    return TABLE_CONTRACTS[key]


def all_qualified_tables() -> list[str]:
    return [c.qualified for c in TABLE_CONTRACTS.values()]
