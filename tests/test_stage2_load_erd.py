from __future__ import annotations

import pandas as pd

from ingest.stage2_load_erd import (
    CsvBundle,
    affected_reference_numbers_for_row,
    apply_procurement_chain_dedup,
    build_municipality_metadata_rows,
    build_organization_metadata_rows,
    build_region_metadata_rows,
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
    assert organization_lookup_candidates("ΔΗΜΟ ΧΑΛΚΗΔΟΝΟΣ") == ["ΔΗΜΟ ΧΑΛΚΗΔΟΝΟΣ", "ΧΑΛΚΗΔΟΝΟΣ"]


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


def test_procurement_rows_maps_accusative_municipal_labels():
    raw = pd.DataFrame(
        [
            {
                "title": "Σύμβαση",
                "referenceNumber": "25SYMV017483983",
                "organization_value": "ΔΗΜΟ ΑΡΓΟΥΣ ΟΡΕΣΤΙΚΟΥ",
                "typeOfContractingAuthority": "ΝΠΔΔ",
            }
        ]
    )

    rows = procurement_rows(
        raw=raw,
        **{
            **empty_procurement_context(),
            "municipality_lookup": {"ΑΡΓΟΥΣ ΟΡΕΣΤΙΚΟΥ": "9065"},
        },
    )

    assert rows[0][49] is None
    assert rows[0][50] == "9065"


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


def test_build_municipality_metadata_rows_prefers_clean_dominant_values():
    raw = pd.DataFrame(
        [
            {
                "referenceNumber": "A",
                "organization_value": "ΔΗΜΟΣ ΑΛΕΞΑΝΔΡΟΥΠΟΛΗΣ",
                "organization_key": "1001",
                "organizationVatNumber": "997712303",
                "nutsCode_key": "EL511",
                "nutsCode_value": "Έβρος",
                "nutsCity": "Αλεξανδρούπολη",
                "nutsPostalCode": "68100",
                "typeOfContractingAuthority": "ΝΠΔΔ",
            },
            {
                "referenceNumber": "B",
                "organization_value": "ΔΗΜΟΣ ΑΛΕΞΑΝΔΡΟΥΠΟΛΗΣ",
                "organization_key": "1001",
                "organizationVatNumber": "997712303",
                "nutsCode_key": "EL5",
                "nutsCode_value": "ΒΟΡΕΙΑ ΕΛΛΑΔΑ",
                "nutsCity": "ΑΛΕΞΑΝΔΡΟΥΠΟΛΗ",
                "nutsPostalCode": "681 00",
                "typeOfContractingAuthority": "ΝΠΔΔ",
            },
            {
                "referenceNumber": "C",
                "organization_value": "ΔΗΜΟΣ ΑΛΕΞΑΝΔΡΟΥΠΟΛΗΣ",
                "organization_key": "1002",
                "organizationVatNumber": "9977123603",
                "nutsCode_key": "EL511",
                "nutsCode_value": "Έβρος",
                "nutsCity": "ΑΛΕΞΑΝΔΡΟΥΠΟΛΗ",
                "nutsPostalCode": "68100",
                "typeOfContractingAuthority": "ΝΠΔΔ",
            },
            {
                "referenceNumber": "D",
                "organization_value": "ΔΗΜΟΣΙΑ ΥΠΗΡΕΣΙΑ ΑΠΑΣΧΟΛΗΣΗΣ",
                "organization_key": "2001",
                "organizationVatNumber": "123456789",
                "nutsCode_key": "EL511",
                "nutsCode_value": "Έβρος",
                "nutsCity": "Αλεξανδρούπολη",
                "nutsPostalCode": "68100",
                "typeOfContractingAuthority": "ΝΠΔΔ",
            },
        ]
    )

    rows = build_municipality_metadata_rows(
        raw=raw,
        **{
            **empty_procurement_context(),
            "municipality_lookup": {"ΑΛΕΞΑΝΔΡΟΥΠΟΛΗΣ": "9006"},
        },
    )

    assert rows == [
        ("9006", "1001", "997712303", "68100", "ΑΛΕΞΑΝΔΡΟΥΠΟΛΗ", "Έβρος", "EL511"),
    ]


def test_build_region_metadata_rows_prefers_region_scope_rows():
    raw = pd.DataFrame(
        [
            {
                "referenceNumber": "A",
                "organization_value": "ΠΕΡΙΦΕΡΕΙΑ ΙΟΝΙΩΝ ΝΗΣΩΝ",
                "organization_key": "7001",
                "organizationVatNumber": "997913715",
                "nutsCode_key": "EL62",
                "nutsCode_value": "Ιόνια Νησιά",
                "nutsCity": "ΚΕΡΚΥΡΑ",
                "nutsPostalCode": "49100",
                "typeOfContractingAuthority": "ΝΠΔΔ",
            },
            {
                "referenceNumber": "B",
                "organization_value": "ΠΕΡΙΦΕΡΕΙΑ ΙΟΝΙΩΝ ΝΗΣΩΝ",
                "organization_key": "7001",
                "organizationVatNumber": "997913715",
                "nutsCode_key": "EL6",
                "nutsCode_value": "ΝΟΤΙΑ ΕΛΛΑΔΑ",
                "nutsCity": "Κέρκυρα",
                "nutsPostalCode": "491 00",
                "typeOfContractingAuthority": "ΝΠΔΔ",
            },
        ]
    )

    rows = build_region_metadata_rows(
        raw=raw,
        **{
            **empty_procurement_context(),
            "region_lookup": {"ΙΟΝΙΩΝ ΝΗΣΩΝ": "ΙΟΝΙΩΝ ΝΗΣΩΝ", "ΠΕΡΙΦΕΡΕΙΑ ΙΟΝΙΩΝ ΝΗΣΩΝ": "ΙΟΝΙΩΝ ΝΗΣΩΝ"},
        },
    )

    assert rows == [
        ("ΙΟΝΙΩΝ ΝΗΣΩΝ", "7001", "997913715", "49100", "ΚΕΡΚΥΡΑ", "Ιόνια Νησιά", "EL62"),
    ]


def test_build_organization_metadata_rows_prefers_resolved_organization_scope_rows():
    raw = pd.DataFrame(
        [
            {
                "referenceNumber": "A",
                "organization_value": "ΔΗΜΟΣΙΑ ΥΠΗΡΕΣΙΑ ΑΠΑΣΧΟΛΗΣΗΣ",
                "organizationVatNumber": "090010376",
                "nutsCode_key": "EL511",
                "nutsCode_value": "Έβρος",
                "nutsCity": "Αλεξανδρούπολη",
                "nutsPostalCode": "68100",
                "typeOfContractingAuthority": "ΝΠΔΔ",
            },
            {
                "referenceNumber": "B",
                "organization_value": "ΔΗΜΟΣΙΑ ΥΠΗΡΕΣΙΑ ΑΠΑΣΧΟΛΗΣΗΣ",
                "organizationVatNumber": "090010376",
                "nutsCode_key": "EL5",
                "nutsCode_value": "ΒΟΡΕΙΑ ΕΛΛΑΔΑ",
                "nutsCity": "ΑΛΕΞΑΝΔΡΟΥΠΟΛΗ",
                "nutsPostalCode": "681 00",
                "typeOfContractingAuthority": "ΝΠΔΔ",
            },
            {
                "referenceNumber": "C",
                "organization_value": "ΠΕΡΙΦΕΡΕΙΑ ΙΟΝΙΩΝ ΝΗΣΩΝ",
                "organizationVatNumber": "997913715",
                "nutsCode_key": "EL62",
                "nutsCode_value": "Ιόνια Νησιά",
                "nutsCity": "ΚΕΡΚΥΡΑ",
                "nutsPostalCode": "49100",
                "typeOfContractingAuthority": "ΝΠΔΔ",
            },
        ]
    )

    rows = build_organization_metadata_rows(
        raw=raw,
        **{
            **empty_procurement_context(),
            "organization_lookup": {"ΔΗΜΟΣΙΑ ΥΠΗΡΕΣΙΑ ΑΠΑΣΧΟΛΗΣΗΣ": "org_1"},
            "region_lookup": {"ΙΟΝΙΩΝ ΝΗΣΩΝ": "ΙΟΝΙΩΝ ΝΗΣΩΝ", "ΠΕΡΙΦΕΡΕΙΑ ΙΟΝΙΩΝ ΝΗΣΩΝ": "ΙΟΝΙΩΝ ΝΗΣΩΝ"},
        },
    )

    assert rows == [
        ("org_1", "090010376", "68100", "ΑΛΕΞΑΝΔΡΟΥΠΟΛΗ", "Έβρος", "EL511"),
    ]
