"""High-confidence exclusion rules for non-fire-protection Diavgeia records."""

from __future__ import annotations

import unicodedata
from typing import Any


TEE_ORGANIZATION = "ΤΕΧΝΙΚΟ ΕΠΙΜΕΛΗΤΗΡΙΟ ΕΛΛΑΔΑΣ"
PLANNING_ACT = "ΠΡΑΞΕΙΣ ΧΩΡΟΤΑΞΙΚΟΥ - ΠΟΛΕΟΔΟΜΙΚΟΥ ΠΕΡΙΕΧΟΜΕΝΟΥ"


def _normalize(value: Any) -> str:
    text = unicodedata.normalize("NFD", str(value or "").strip().upper())
    return " ".join(
        "".join(char for char in text if unicodedata.category(char) != "Mn").split()
    )


def is_confirmed_irrelevant_decision(org_name_clean: Any, decision_type: Any) -> bool:
    """Exclude TEE planning/building-permit acts, not generic TEE decisions."""
    return (
        _normalize(org_name_clean) == _normalize(TEE_ORGANIZATION)
        and _normalize(decision_type) == _normalize(PLANNING_ACT)
    )
