"""Pandera (polars backend) models. These are the typed contracts the suite actually validates."""

from __future__ import annotations

import pandera.polars as pa
import polars as pl


class DimCustomerModel(pa.DataFrameModel):
    customer_sk: int
    customer_nk: str
    email: str
    country: str
    signup_date: pl.Date
    is_active: bool

    class Config:
        coerce = True
        strict = False


class DimProductModel(pa.DataFrameModel):
    product_sk: int
    sku: str
    category: str = pa.Field(isin=["devices", "consumables", "apparel", "lab"])
    unit_price: float = pa.Field(ge=0)
    active_flag: bool

    class Config:
        coerce = True
        strict = False


class FactOrdersModel(pa.DataFrameModel):
    order_id: int
    customer_sk: int
    order_ts: pl.Datetime
    status: str
    total_amount: float = pa.Field(nullable=True, ge=0)
    currency: str = pa.Field(isin=["USD", "EUR", "GBP"])

    class Config:
        coerce = True
        strict = False


class FactOrderItemsModel(pa.DataFrameModel):
    order_item_id: int
    order_id: int
    product_sk: int
    qty: int = pa.Field(gt=0)
    unit_price: float = pa.Field(ge=0)

    class Config:
        coerce = True
        strict = False


class DimSiteModel(pa.DataFrameModel):
    site_sk: int
    site_id: str
    country: str
    region: str

    class Config:
        coerce = True
        strict = False


class DimPatientModel(pa.DataFrameModel):
    patient_sk: int
    subject_id: str
    site_sk: int
    sex: str = pa.Field(nullable=True, isin=["M", "F", "U"])
    birth_year: int = pa.Field(ge=1920, le=2020)
    enrolled_on: pl.Date

    class Config:
        coerce = True
        strict = False


class FactVisitsModel(pa.DataFrameModel):
    visit_id: int
    patient_sk: int
    visit_code: str
    window_start: pl.Date
    window_end: pl.Date
    visit_date: pl.Date = pa.Field(nullable=True)
    status: str = pa.Field(isin=["scheduled", "completed", "missed", "window_violation"])

    class Config:
        coerce = True
        strict = False


class FactAdverseEventsModel(pa.DataFrameModel):
    ae_id: int
    patient_sk: int
    term_code: str = pa.Field(nullable=True)
    severity: str
    onset_date: pl.Date
    related_flag: bool

    class Config:
        coerce = True
        strict = False


PANDERA_MODELS: dict[str, type[pa.DataFrameModel]] = {
    "dim_customer": DimCustomerModel,
    "dim_product": DimProductModel,
    "fact_orders": FactOrdersModel,
    "fact_order_items": FactOrderItemsModel,
    "dim_site": DimSiteModel,
    "dim_patient": DimPatientModel,
    "fact_visits": FactVisitsModel,
    "fact_adverse_events": FactAdverseEventsModel,
}
