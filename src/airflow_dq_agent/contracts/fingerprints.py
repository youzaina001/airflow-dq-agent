"""Canonical, content-addressed fingerprints for governed artifacts."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import BaseModel, TypeAdapter


def canonical_json(value: Any) -> str:
    """Serialize governed data deterministically before it is fingerprinted."""
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    else:
        value = TypeAdapter(Any).dump_python(value, mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def canonical_fingerprint(value: Any) -> str:
    """Return a stable SHA-256 fingerprint with an explicit algorithm prefix."""
    digest = hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"
