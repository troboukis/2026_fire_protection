"""High-confidence exclusion rules for non-fire-protection Diavgeia records."""

from __future__ import annotations

import unicodedata
from typing import Any


TEE_ORGANIZATION = "ΤΕΧΝΙΚΟ ΕΠΙΜΕΛΗΤΗΡΙΟ ΕΛΛΑΔΑΣ"
PLANNING_ACT = "ΠΡΑΞΕΙΣ ΧΩΡΟΤΑΞΙΚΟΥ - ΠΟΛΕΟΔΟΜΙΚΟΥ ΠΕΡΙΕΧΟΜΕΝΟΥ"
EXCLUDED_DIAVGEIA_ADAS = {
    "6ΤΕ0ΟΞΝΑ-6ΔΨ": "Excluded after user review on 2026-10-23: not a target fire-protection payment.",
}


def _normalize(value: Any) -> str:
    text = unicodedata.normalize("NFD", str(value if value is not None else "").strip().upper())
    return " ".join(
        "".join(char for char in text if unicodedata.category(char) != "Mn").split()
    )


def is_excluded_diavgeia_ada(ada: Any) -> bool:
    return str(ada).strip().upper() in EXCLUDED_DIAVGEIA_ADAS


def is_confirmed_irrelevant_decision(org_name_clean: Any, decision_type: Any, ada: Any = None) -> bool:
    """Exclude reviewed ADAs and TEE planning/building-permit acts."""
    if is_excluded_diavgeia_ada(ada):
        return True
    return (
        _normalize(org_name_clean) == _normalize(TEE_ORGANIZATION)
        and _normalize(decision_type) == _normalize(PLANNING_ACT)
    )
