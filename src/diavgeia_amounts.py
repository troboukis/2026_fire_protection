"""Shared amount resolution for Diavgeia raw rows.

The database intentionally keeps one text amount column on ``public.diavgeia``.
The raw CSV keeps the source-specific enrichment columns so the provenance and
meaning of each value remain available alongside ``decisionType``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import csv
from functools import lru_cache
from pathlib import Path

PDF_PAYMENT_AMOUNTS_PATH = Path(__file__).resolve().parents[1] / 'data' / 'mappings' / 'diavgeia_payment_pdf_amounts.csv'

# These decisions have incorrect API amounts. Their original PDFs
# confirm the amounts below on pages 3 and 4. Apply the literal verified amount
# before any API/CSV value so subsequent imports cannot restore the error.
DIAVGEIA_AMOUNT_OVERRIDES = {
    "61024653Π8-634": "779.140,14",
    "6ΛΕΩ4653Π8-Ξ1Ξ": "146.519,72",
    "Ρ1ΘΠ4653Π8-ΘΔΛ": "666.683,19",
    "Ψ94Σ4653Π8-ΦΚΕ": "178.826,27",
    "ΨΞΩΥ4653Π8-ΡΓΘ": "54.894,97",
}


@lru_cache(maxsize=1)
def load_pdf_payment_amounts() -> dict[str, str]:
    """PDF-derived fallback used only when API amount fields are absent."""
    if not PDF_PAYMENT_AMOUNTS_PATH.exists():
        return {}
    with PDF_PAYMENT_AMOUNTS_PATH.open(encoding='utf-8', newline='') as handle:
        return {row['ada']: row['amount'] for row in csv.DictReader(handle)}


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
    ada = amount_text(row.get("ada"))
    if ada in DIAVGEIA_AMOUNT_OVERRIDES:
        return DIAVGEIA_AMOUNT_OVERRIDES[ada]
    for column in DIAVGEIA_AMOUNT_SOURCE_COLUMNS:
        value = amount_text(row.get(column))
        if value is not None:
            return value
    if row.get('decisionType') == 'ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ':
        ada = amount_text(row.get('ada'))
        if ada:
            return load_pdf_payment_amounts().get(ada)
    return None
