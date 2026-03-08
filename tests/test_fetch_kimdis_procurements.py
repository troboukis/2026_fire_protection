from __future__ import annotations

import pandas as pd

from src.fetch_kimdis_procurements import (
    business_key_from_api_item,
    dedupe_df_by_business_key,
    normalize_string,
)


def test_normalize_string_strips_accents_and_collapses_whitespace():
    assert normalize_string("  Δήμος   Αθηναίων  ") == "δημος αθηναιων"


def test_business_key_prefers_reference_number():
    item = {
        "referenceNumber": "26SYMV018574528",
        "diavgeiaADA": "SHOULD_NOT_BE_USED",
        "organization": {"key": "123"},
    }
    assert business_key_from_api_item(item) == "ref:26symv018574528"


def test_dedupe_df_keeps_latest_row_per_business_key():
    df = pd.DataFrame(
        [
            {
                "referenceNumber": "26SYMV1",
                "diavgeiaADA": "",
                "contractNumber": "",
                "organization_key": "ORG",
                "title": "older",
                "submissionDate": "2026-01-01T09:00:00",
                "contractSignedDate": "2026-01-02",
            },
            {
                "referenceNumber": "26SYMV1",
                "diavgeiaADA": "",
                "contractNumber": "",
                "organization_key": "ORG",
                "title": "newer",
                "submissionDate": "2026-01-03T09:00:00",
                "contractSignedDate": "2026-01-04",
            },
        ]
    )

    out = dedupe_df_by_business_key(df)

    assert len(out) == 1
    assert out.iloc[0]["title"] == "newer"
