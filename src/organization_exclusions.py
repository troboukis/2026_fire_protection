"""Organization-level exclusions shared by collection and database ingestion."""

from __future__ import annotations

import ast
import json
import re
from collections.abc import Mapping
from typing import Any

try:
    from .keyword_exclusions import normalize_keyword_tokens
except ImportError:  # Direct script execution.
    from keyword_exclusions import normalize_keyword_tokens


# Match hospital nouns, not adjectives such as ΚΕΝΤΡΟ ... ΝΟΣΟΚΟΜΕΙΑΚΩΝ ΥΠΗΡΕΣΙΩΝ.
_HOSPITAL_NOUN = re.compile(r"νοσοκομει(?:ο|ου|α|ων|ον)")
_HOSPITAL_ABBREVIATIONS = frozenset({"γν", "πγν", "γνδα"})
_HOSPITAL_ADJECTIVES = frozenset({
    "γεν", "γενικο", "πανεπιστ", "πανεπιστημιακο", "ογκολογικο",
    "παναρκαδικο", "παθολογικο",
})
_REVIEWED_HOSPITAL_NAMES = frozenset({
    "νοσηλευτικο ιδρυμα μετοχικου ταμειου στρατου",
    "εθνικο κεντρο αποκαταστασησ",
})


def _organization_label(value: object) -> str:
    """Read the issuer label from API objects and their persisted CSV form."""
    if isinstance(value, Mapping):
        for key in ("label", "value"):
            label = value.get(key)
            if isinstance(label, str) and label.strip():
                return label
        return ""
    if not isinstance(value, str):
        return ""
    text = value.strip()
    if text.startswith("{") and text.endswith("}"):
        for parse in (json.loads, ast.literal_eval):
            try:
                parsed = parse(text)
            except (ValueError, SyntaxError):
                continue
            return _organization_label(parsed) if isinstance(parsed, Mapping) else ""
        return ""
    return text


def is_hospital_organization(*organization_names: object, org_type: object = None) -> bool:
    """Identify hospital issuers from their names/type, never from document subjects.

    Hospital labels in the source also use Γ.Ν., Π.Γ.Ν. and Γ.Ν.Δ.Α.; require
    those abbreviations at the beginning of the organization label. Some source
    names spell the last letter of ΝΟΣΟΚΟΜΕΙΟ with a Latin O.
    """
    for value in (*organization_names, org_type):
        tokens = normalize_keyword_tokens(_organization_label(value))
        if " ".join(tokens) in _REVIEWED_HOSPITAL_NAMES:
            return True
        if any(_HOSPITAL_NOUN.fullmatch(token.replace("o", "ο")) for token in tokens):
            return True
        for index, token in enumerate(tokens):
            if token in {"νοσ", "νοσοκ"} and (
                index == 0 or _HOSPITAL_ADJECTIVES.intersection(tokens[:index])
            ):
                return True
        if tokens and (
            tokens[0] in _HOSPITAL_ABBREVIATIONS
            or tokens[:2] == ["γ", "ν"]
            or tokens[:3] == ["π", "γ", "ν"]
        ):
            return True
    return False


def record_has_hospital_organization(record: Mapping[str, Any]) -> bool:
    """Support raw API, CSV and database issuer fields without reading title text."""
    return is_hospital_organization(
        record.get("organization"),
        record.get("org"),
        record.get("org_name_clean"),
        record.get("organization_value"),
        record.get("organization_normalized_value"),
        record.get("typeOfContractingAuthority"),
        org_type=record.get("org_type"),
    )
