from __future__ import annotations

import pandas as pd

from ingest.stage2_load_erd import organization_lookup_candidates, procurement_rows


def test_organization_lookup_candidates_normalize_municipal_labels():
    assert organization_lookup_candidates("ΔΗΜΟΣ ΧΑΛΚΗΔΟΝΟΣ") == ["ΔΗΜΟΣ ΧΑΛΚΗΔΟΝΟΣ", "ΧΑΛΚΗΔΟΝΟΣ"]
    assert organization_lookup_candidates("ΔΗΜΟΥ ΧΑΛΚΗΔΟΝΟΣ") == ["ΔΗΜΟΥ ΧΑΛΚΗΔΟΝΟΣ", "ΧΑΛΚΗΔΟΝΟΣ"]


def test_procurement_rows_falls_back_to_normalized_municipality_name():
    raw = pd.DataFrame(
        [
            {
                "title": "Σύμβαση",
                "referenceNumber": "26SYMV018628653",
                "organization_value": "ΔΗΜΟΣ ΧΑΛΚΗΔΟΝΟΣ",
                "typeOfContractingAuthority": "ΝΠΔΔ",
            }
        ]
    )

    rows = procurement_rows(
        raw=raw,
        org_map={},
        organization_lookup={},
        region_lookup={},
        municipality_lookup={"ΧΑΛΚΗΔΟΝΟΣ": "9038"},
    )

    assert rows[0][49] is None
    assert rows[0][50] == "9038"
