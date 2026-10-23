"""Reviewed, reference-specific exclusions for KIMDIS ingestion."""


# ADMIE project DEA 42569 monitors electrical equipment in substations/KYT.
# Its OLMS contracts do not document a forest-fire protection purpose.
EXCLUDED_PROCUREMENT_REFERENCES = frozenset({
    "26SYMV019427117",
    "26SYMV019431551",
})


def is_confirmed_irrelevant_procurement(reference_number: object) -> bool:
    """Match only the reviewed contract references, regardless of case or spaces."""
    return str(reference_number or "").strip().upper() in EXCLUDED_PROCUREMENT_REFERENCES
