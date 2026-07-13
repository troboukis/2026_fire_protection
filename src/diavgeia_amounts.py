"""Shared amount resolution for Diavgeia raw rows.

The database intentionally keeps one text amount column on ``public.diavgeia``.
The raw CSV keeps the source-specific enrichment columns so the provenance and
meaning of each value remain available alongside ``decisionType``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


DIAVGEIA_AMOUNT_SOURCE_COLUMNS = (
    "spending_contractors_value",
    "commitment_amount_with_vat",
    "direct_value",
    "payment_value",
)

_EMPTY_TEXT_VALUES = {"", "nan", "none", "nat", "<na>", "[]"}


def amount_text(value: Any) -> str | None:
    """Return a non-empty raw amount as text without numeric normalization."""
    if value is None:
        return None
    if isinstance(value, (list, tuple, dict, set)) and not value:
        return None

    text = str(value).strip()
    if text.lower() in _EMPTY_TEXT_VALUES:
        return None
    return text


def resolve_diavgeia_amount_text(row: Mapping[str, Any]) -> str | None:
    """Resolve the single DB amount from all supported raw amount columns.

    Only one source column is expected for each decision type. Keeping an
    ordered fallback also makes older CSV snapshots safe to ingest.
    """
    for column in DIAVGEIA_AMOUNT_SOURCE_COLUMNS:
        value = amount_text(row.get(column))
        if value is not None:
            return value
    return None
