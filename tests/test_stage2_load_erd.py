from __future__ import annotations

import pandas as pd

from ingest.stage2_load_erd import (
    CsvBundle,
    affected_reference_numbers_for_row,
    apply_procurement_chain_dedup,
    build_municipality_metadata_rows,
    build_organization_metadata_rows,
    build_region_metadata_rows,
    dedupe_forest_fire_rows,
    fund_rows,
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


def test_procurement_rows_resolves_organization_by_afm_before_name():
    raw = pd.DataFrame(
        [
            {
                "title": "Σύμβαση",
                "referenceNumber": "26SYMV018900001",
                "organization_value": "ΜΗ ΓΝΩΣΤΗ ΕΚΔΟΧΗ ΔΥΠΑ",
                "organizationVatNumber": "090010376",
                "typeOfContractingAuthority": "ΝΠΔΔ",
            }
        ]
    )

    rows = procurement_rows(
        raw=raw,
        **{
            **empty_procurement_context(),
            "organization_afm_lookup": {"090010376": "org_1"},
        },
    )

    assert rows[0][49] == "org_1"
    assert rows[0][51] == "organization"


def test_procurement_rows_auto_creates_unknown_organization_from_afm():
    raw = pd.DataFrame(
        [
            {
                "title": "Σύμβαση",
                "referenceNumber": "26SYMV018703821",
                "organization_key": "100044187",
                "organization_value": "ΜΗΤΡΟΠΟΛΙΤΙΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΜΟΥΣΕΙΩΝ ΕΙΚΑΣΤΙΚΩΝ ΤΕΧΝΩΝ ΘΕΣΣΑΛΟΝΙΚΗΣ",
                "organizationVatNumber": "997223821",
                "typeOfContractingAuthority": "ΝΠΙΔ",
            }
        ]
    )
    created_organization_rows: dict[tuple[str, str], tuple[str, str, str, str | None, str | None, str | None]] = {}

    rows = procurement_rows(
        raw=raw,
        **{
            **empty_procurement_context(),
            "created_organization_rows": created_organization_rows,
            "auto_create_organizations": True,
        },
    )

    assert rows[0][49] == "org_afm_997223821"
    assert rows[0][51] == "organization"
    assert created_organization_rows == {
        (
            "org_afm_997223821",
            "ΜΗΤΡΟΠΟΛΙΤΙΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΜΟΥΣΕΙΩΝ ΕΙΚΑΣΤΙΚΩΝ ΤΕΧΝΩΝ ΘΕΣΣΑΛΟΝΙΚΗΣ",
        ): (
            "org_afm_997223821",
            "ΜΗΤΡΟΠΟΛΙΤΙΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΜΟΥΣΕΙΩΝ ΕΙΚΑΣΤΙΚΩΝ ΤΕΧΝΩΝ ΘΕΣΣΑΛΟΝΙΚΗΣ",
            "ΜΗΤΡΟΠΟΛΙΤΙΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΜΟΥΣΕΙΩΝ ΕΙΚΑΣΤΙΚΩΝ ΤΕΧΝΩΝ ΘΕΣΣΑΛΟΝΙΚΗΣ",
            None,
            "raw_procurements",
            "100044187",
        ),
    }


def test_procurement_rows_normalizes_hyphenated_region_labels():
    raw = pd.DataFrame(
        [
            {
                "title": "Σύμβαση",
                "referenceNumber": "26SYMV018999001",
                "organization_value": "ΠΕΡΙΦΕΡΕΙΑ ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ - ΘΡΑΚΗΣ",
                "typeOfContractingAuthority": "ΝΠΔΔ",
            }
        ]
    )

    rows = procurement_rows(
        raw=raw,
        **{
            **empty_procurement_context(),
            "region_lookup": {
                "ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ": "ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ",
                "ΠΕΡΙΦΕΡΕΙΑ ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ": "ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ",
            },
        },
    )

    assert rows[0][48] == "ΑΝΑΤΟΛΙΚΗΣ ΜΑΚΕΔΟΝΙΑΣ ΚΑΙ ΘΡΑΚΗΣ"
    assert rows[0][49] is None
    assert rows[0][51] == "region"


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


def test_fund_rows_reads_current_municipal_funding_schema():
    fund = pd.DataFrame(
        [
            {
                "region_key": "ΚΡΗΤΗΣ",
                "organization_key": "org_123",
                "municipality_key": "9311",
                "year": "2026",
                "allocation_type": "τακτική",
                "recipient_type": "δήμος",
                "recipient_raw": "ΔΗΜΟΣ ΙΕΡΑΠΕΤΡΑΣ",
                "nomos": "ΛΑΣΙΘΙΟΥ",
                "amount_eur": "475000.00",
                "source_file": "apof14727-20260317.pdf",
                "source_ada": "ABC-123",
            }
        ]
    )

    assert fund_rows(fund) == [
        (
            "ΚΡΗΤΗΣ",
            "org_123",
            "9311",
            2026,
            "τακτική",
            "δήμος",
            "ΔΗΜΟΣ ΙΕΡΑΠΕΤΡΑΣ",
            "ΛΑΣΙΘΙΟΥ",
            475000.0,
            "apof14727-20260317.pdf",
            "ABC-123",
        )
    ]


def test_fund_rows_keeps_backward_compatible_aliases():
    fund = pd.DataFrame(
        [
            {
                "region_id": "ΙΟΝΙΩΝ ΝΗΣΩΝ",
                "municipality_code": "9101",
                "year": "2019",
                "allocation_type": "τακτική",
                "recipient_type": "δήμος",
                "recipient_raw": "ΔΗΜΟΣ ΚΕΡΚΥΡΑΣ",
                "nomos": "ΚΕΡΚΥΡΑΣ",
                "amount_eur": "1000.00",
                "source_file": "legacy.pdf",
                "source_ada": "LEG-001",
            }
        ]
    )

    assert fund_rows(fund) == [
        (
            "ΙΟΝΙΩΝ ΝΗΣΩΝ",
            None,
            "9101",
            2019,
            "τακτική",
            "δήμος",
            "ΔΗΜΟΣ ΚΕΡΚΥΡΑΣ",
            "ΚΕΡΚΥΡΑΣ",
            1000.0,
            "legacy.pdf",
            "LEG-001",
        )
    ]


def test_dedupe_forest_fire_rows_removes_exact_duplicates_and_preserves_order():
    first = (
        "9170",
        None,
        2024,
        "2024-08-11",
        "2024-08-12",
        "ΑΤΤΙΚΗΣ",
        "ΠΕΝΤΕΛΗ",
        38.05,
        23.86,
        10.0,
        0.0,
        0.0,
        0.0,
        0.0,
        10.0,
        1.0,
        "fire_incidents_unified.csv",
    )
    second = (
        "9170",
        None,
        2024,
        "2024-08-13",
        "2024-08-13",
        "ΑΤΤΙΚΗΣ",
        "ΝΤΑΟΥ",
        38.08,
        23.9,
        20.0,
        5.0,
        0.0,
        0.0,
        0.0,
        25.0,
        2.5,
        "fire_incidents_unified.csv",
    )

    deduped, skipped = dedupe_forest_fire_rows([first, first, second, second, first])

    assert deduped == [first, second]
    assert skipped == 3


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
