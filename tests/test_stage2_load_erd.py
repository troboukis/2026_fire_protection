from __future__ import annotations

import pandas as pd

from ingest.stage2_load_erd import (
    CsvBundle,
    affected_reference_numbers_for_row,
    apply_procurement_chain_dedup,
    organization_lookup_candidates,
    procurement_rows,
    region_lookup_candidates,
    seed_organization_rows,
)


def empty_procurement_context():
    return {
        "org_map": {},
        "organization_lookup": {},
        "region_lookup": {},
        "municipality_lookup": {},
        "municipality_region_lookup": {},
        "municipality_alias_lookup": {},
        "org_municipality_coverage_lookup": {},
    }


def test_organization_lookup_candidates_normalize_municipal_labels():
    assert organization_lookup_candidates("ΔΗΜΟΣ ΧΑΛΚΗΔΟΝΟΣ") == ["ΔΗΜΟΣ ΧΑΛΚΗΔΟΝΟΣ", "ΧΑΛΚΗΔΟΝΟΣ"]
    assert organization_lookup_candidates("ΔΗΜΟΥ ΧΑΛΚΗΔΟΝΟΣ") == ["ΔΗΜΟΥ ΧΑΛΚΗΔΟΝΟΣ", "ΧΑΛΚΗΔΟΝΟΣ"]


def test_organization_lookup_candidates_extract_region_labels():
    candidates = region_lookup_candidates(
        "ΙΟΝΙΑ ΑΝΑΠΤΥΞΗ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΟΤΑ ΠΕΡΙΦΕΡΕΙΑΣ ΙΟΝΙΩΝ ΝΗΣΩΝ (ΑΟΠΙΝ) Α.Ε.",
    )
    assert "ΙΟΝΙΩΝ ΝΗΣΩΝ" in candidates
    assert "ΠΕΡΙΦΕΡΕΙΑ ΙΟΝΙΩΝ ΝΗΣΩΝ" in candidates


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
        **{
            **empty_procurement_context(),
            "municipality_lookup": {"ΧΑΛΚΗΔΟΝΟΣ": "9038"},
        },
    )

    assert rows[0][49] is None
    assert rows[0][50] == "9038"


def test_procurement_rows_falls_back_to_region_lookup_from_org_label():
    raw = pd.DataFrame(
        [
            {
                "title": "Ευπρεπισμός Οδικού Δικτύου ΠΕ Κέρκυρας",
                "referenceNumber": "26SYMV018611768",
                "organization_value": "ΙΟΝΙΑ ΑΝΑΠΤΥΞΗ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΟΤΑ ΠΕΡΙΦΕΡΕΙΑΣ ΙΟΝΙΩΝ ΝΗΣΩΝ (ΑΟΠΙΝ) Α.Ε.",
                "typeOfContractingAuthority": "ΝΠΙΔ",
            }
        ]
    )

    rows = procurement_rows(
        raw=raw,
        **{
            **empty_procurement_context(),
            "region_lookup": {"ΙΟΝΙΩΝ ΝΗΣΩΝ": "ΙΟΝΙΩΝ ΝΗΣΩΝ", "ΠΕΡΙΦΕΡΕΙΑ ΙΟΝΙΩΝ ΝΗΣΩΝ": "ΙΟΝΙΩΝ ΝΗΣΩΝ"},
        },
    )

    assert rows[0][48] == "ΙΟΝΙΩΝ ΝΗΣΩΝ"
    assert rows[0][49] is None


def test_apply_procurement_chain_dedup_zeroes_superseded_and_forward_linked_amounts():
    raw = pd.DataFrame(
        [
            {"referenceNumber": "A", "prevReferenceNo": "", "nextRefNo": "", "totalCostWithoutVAT": "100"},
            {"referenceNumber": "B", "prevReferenceNo": "A", "nextRefNo": "", "totalCostWithoutVAT": "200"},
            {"referenceNumber": "C", "prevReferenceNo": "", "nextRefNo": "D", "totalCostWithoutVAT": "300"},
            {"referenceNumber": "D", "prevReferenceNo": "C", "nextRefNo": "", "totalCostWithoutVAT": "400"},
            {"referenceNumber": "E", "prevReferenceNo": "", "nextRefNo": "", "totalCostWithoutVAT": "500"},
        ]
    )

    deduped = apply_procurement_chain_dedup(raw)

    amounts = dict(zip(deduped["referenceNumber"], deduped["totalCostWithoutVAT"]))
    assert amounts["A"] == "0"
    assert amounts["B"] == "200"
    assert amounts["C"] == "0"
    assert amounts["D"] == "400"
    assert amounts["E"] == "500"


def test_affected_reference_numbers_for_row_targets_only_superseded_contracts():
    row = pd.Series(
        {
            "referenceNumber": "B",
            "prevReferenceNo": "A",
            "nextRefNo": "C",
        }
    )

    assert affected_reference_numbers_for_row(row) == {"A", "B"}


def test_seed_organization_rows_keeps_regional_organizations_as_organizations():
    bundle = CsvBundle(
        raw=pd.DataFrame(),
        diav=pd.DataFrame(),
        fire=pd.DataFrame(),
        fund=pd.DataFrame(
            columns=[
                "region_id",
                "municipality_id",
                "source_entity_type",
                "source_value",
                "normalized_value",
                "source_system",
                "source_key",
                "notes",
            ]
        ),
        org_map=pd.DataFrame(
            [
                {
                    "org_type": "ΝΠΙΔ",
                    "org_name_clean": "ΙΟΝΙΑ ΑΝΑΠΤΥΞΗ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΟΤΑ ΠΕΡΙΦΕΡΕΙΑΣ ΙΟΝΙΩΝ ΝΗΣΩΝ (ΑΟΠΙΝ) Α.Ε.",
                    "authority_level": "region",
                }
            ]
        ),
        region_map=pd.DataFrame(
            [{"region_id": "ΙΟΝΙΩΝ ΝΗΣΩΝ"}]
        ),
        expanded_map=pd.DataFrame(
            [
                {
                    "source_entity_type": "organization",
                    "source_value": "ΙΟΝΙΑ ΑΝΑΠΤΥΞΗ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΟΤΑ ΠΕΡΙΦΕΡΕΙΑΣ ΙΟΝΙΩΝ ΝΗΣΩΝ (ΑΟΠΙΝ) Α.Ε.",
                    "normalized_value": "ΙΟΝΙΑ ΑΝΑΠΤΥΞΗ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΟΤΑ ΠΕΡΙΦΕΡΕΙΑΣ ΙΟΝΙΩΝ ΝΗΣΩΝ (ΑΟΠΙΝ) Α.Ε.",
                    "source_system": "kimdis",
                    "source_key": "ΝΠΙΔ::ΙΟΝΙΑ ΑΝΑΠΤΥΞΗ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΟΤΑ ΠΕΡΙΦΕΡΕΙΑΣ ΙΟΝΙΩΝ ΝΗΣΩΝ (ΑΟΠΙΝ) Α.Ε.",
                    "notes": "org_type=ΝΠΙΔ",
                }
            ]
        ),
    )

    rows = seed_organization_rows(bundle)

    assert len(rows) == 1
    assert rows[0][1] == "ΙΟΝΙΑ ΑΝΑΠΤΥΞΗ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΟΤΑ ΠΕΡΙΦΕΡΕΙΑΣ ΙΟΝΙΩΝ ΝΗΣΩΝ (ΑΟΠΙΝ) Α.Ε."
    assert rows[0][3] == "region"
