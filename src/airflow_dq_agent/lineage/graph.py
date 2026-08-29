"""Static lineage for the synthetic warehouse. Enough for citations; not a real catalog crawler."""

from __future__ import annotations

from pydantic import BaseModel, Field


class LineageEdge(BaseModel):
    source: str
    target: str
    kind: str = "fk"
    note: str = ""


class LineageGraph(BaseModel):
    edges: list[LineageEdge] = Field(default_factory=list)

    def upstream(self, table: str) -> list[str]:
        key = table.split(".")[-1]
        return [e.source for e in self.edges if e.target == key]

    def downstream(self, table: str) -> list[str]:
        key = table.split(".")[-1]
        return [e.target for e in self.edges if e.source == key]

    def neighbors(self, table: str) -> dict[str, list[str]]:
        return {"upstream": self.upstream(table), "downstream": self.downstream(table)}


LINEAGE = LineageGraph(
    edges=[
        LineageEdge(source="stg_shop_orders", target="fact_orders", kind="pipeline"),
        LineageEdge(source="stg_shop_items", target="fact_order_items", kind="pipeline"),
        LineageEdge(source="stg_subjects", target="dim_patient", kind="pipeline"),
        LineageEdge(source="dim_customer", target="fact_orders", kind="fk"),
        LineageEdge(source="fact_orders", target="fact_order_items", kind="fk"),
        LineageEdge(source="dim_product", target="fact_order_items", kind="fk"),
        LineageEdge(source="dim_site", target="dim_patient", kind="fk"),
        LineageEdge(source="dim_patient", target="fact_visits", kind="fk"),
        LineageEdge(source="dim_patient", target="fact_adverse_events", kind="fk"),
    ]
)


def get_lineage(table: str) -> dict[str, list[str]]:
    return LINEAGE.neighbors(table)
