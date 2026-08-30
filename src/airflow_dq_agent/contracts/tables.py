"""Table contracts are the schema source of truth. Drift is measured against these, not the LLM."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

DType = Literal["int64", "float64", "utf8", "date", "datetime", "bool"]


class ColumnContract(BaseModel):
    name: str
    dtype: DType
    nullable: bool = False
    unique: bool = False
    allowed_values: list[str] | None = None
    description: str = ""


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


def _c(
    name: str,
    dtype: DType,
    *,
    nullable: bool = False,
    unique: bool = False,
    allowed: list[str] | None = None,
    description: str = "",
) -> ColumnContract:
    return ColumnContract(
        name=name,
        dtype=dtype,
        nullable=nullable,
        unique=unique,
        allowed_values=allowed,
        description=description,
    )


TABLE_CONTRACTS: dict[str, TableContract] = {
    "dim_customer": TableContract(
        table="dim_customer",
        grain="one row per customer_sk",
        description="Synthetic shoppers. Emails are @example.test — no real PII.",
        primary_key=["customer_sk"],
        columns=[
            _c("customer_sk", "int64", unique=True),
            _c("customer_nk", "utf8", unique=True, description="Business key"),
            _c("email", "utf8", description="Must contain @"),
            _c("country", "utf8"),
            _c("signup_date", "date"),
            _c("is_active", "bool"),
        ],
    ),
    "dim_product": TableContract(
        table="dim_product",
        grain="one row per product_sk",
        description="Catalog of synthetic SKUs.",
        primary_key=["product_sk"],
        columns=[
            _c("product_sk", "int64", unique=True),
            _c("sku", "utf8", unique=True),
            _c("category", "utf8", allowed=["devices", "consumables", "apparel", "lab"]),
            _c("unit_price", "float64"),
            _c("active_flag", "bool"),
        ],
    ),
    "fact_orders": TableContract(
        table="fact_orders",
        grain="one row per order_id",
        description="Order headers. total_amount is completeness-critical.",
        primary_key=["order_id"],
        foreign_keys=[("customer_sk", "dim_customer", "customer_sk")],
        columns=[
            _c("order_id", "int64", unique=True),
            _c("customer_sk", "int64"),
            _c("order_ts", "datetime"),
            _c(
                "status",
                "utf8",
                allowed=["placed", "paid", "shipped", "cancelled", "returned"],
            ),
            _c("total_amount", "float64", description="Must be non-null and >= 0"),
            _c("currency", "utf8", allowed=["USD", "EUR", "GBP"]),
        ],
    ),
    "fact_order_items": TableContract(
        table="fact_order_items",
        grain="one row per order_item_id",
        description="Order lines. product_sk must resolve.",
        primary_key=["order_item_id"],
        foreign_keys=[
            ("order_id", "fact_orders", "order_id"),
            ("product_sk", "dim_product", "product_sk"),
        ],
        columns=[
            _c("order_item_id", "int64", unique=True),
            _c("order_id", "int64"),
            _c("product_sk", "int64"),
            _c("qty", "int64"),
            _c("unit_price", "float64"),
        ],
    ),
    "dim_site": TableContract(
        table="dim_site",
        grain="one row per site_sk",
        description="Synthetic trial sites. No real investigators.",
        primary_key=["site_sk"],
        columns=[
            _c("site_sk", "int64", unique=True),
            _c("site_id", "utf8", unique=True),
            _c("country", "utf8"),
            _c("region", "utf8"),
        ],
    ),
    "dim_patient": TableContract(
        table="dim_patient",
        grain="one row per patient_sk",
        description="Synthetic subjects: IDs + birth year only. No names.",
        primary_key=["patient_sk"],
        foreign_keys=[("site_sk", "dim_site", "site_sk")],
        columns=[
            _c("patient_sk", "int64", unique=True),
            _c("subject_id", "utf8", unique=True),
            _c("site_sk", "int64"),
            _c("sex", "utf8", allowed=["M", "F", "U"]),
            _c("birth_year", "int64"),
            _c("enrolled_on", "date"),
        ],
    ),
    "fact_visits": TableContract(
        table="fact_visits",
        grain="one row per visit_id",
        description="Scheduled visits. visit_date must fall inside the window.",
        primary_key=["visit_id"],
        foreign_keys=[("patient_sk", "dim_patient", "patient_sk")],
        columns=[
            _c("visit_id", "int64", unique=True),
            _c("patient_sk", "int64"),
            _c("visit_code", "utf8"),
            _c("window_start", "date"),
            _c("window_end", "date"),
            _c("visit_date", "date", nullable=True),
            _c("status", "utf8", allowed=["scheduled", "completed", "missed", "window_violation"]),
        ],
    ),
    "fact_adverse_events": TableContract(
        table="fact_adverse_events",
        grain="one row per ae_id",
        description="Synthetic AE log. term_code and severity are validity-critical.",
        primary_key=["ae_id"],
        foreign_keys=[("patient_sk", "dim_patient", "patient_sk")],
        columns=[
            _c("ae_id", "int64", unique=True),
            _c("patient_sk", "int64"),
            _c("term_code", "utf8", description="AE-* code from the study dictionary"),
            _c("severity", "utf8", allowed=["mild", "moderate", "severe"]),
            _c("onset_date", "date"),
            _c("related_flag", "bool"),
        ],
    ),
}


def get_table_contract(table: str) -> TableContract:
    key = table.split(".")[-1]
    if key not in TABLE_CONTRACTS:
        raise KeyError(f"Unknown table {table!r}. Contracts: {sorted(TABLE_CONTRACTS)}")
    return TABLE_CONTRACTS[key]


def all_qualified_tables() -> list[str]:
    return [c.qualified for c in TABLE_CONTRACTS.values()]
