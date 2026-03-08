from __future__ import annotations

from src.locate_work import Document


def make_document() -> Document:
    return Document.__new__(Document)


def test_merge_work_values_deduplicates_case_insensitively():
    doc = make_document()
    merged = doc._merge_work_values("κλάδεμα, αποψίλωση", "Αποψίλωση, καθαρισμός")
    assert merged == ["κλάδεμα", "αποψίλωση", "καθαρισμός"]


def test_deduplicate_findings_pre_geocode_merges_pages_and_shortest_canonical():
    doc = make_document()
    findings = [
        {
            "point_name_raw": "Οδός Α 10",
            "point_name_canonical": "Οδός Α 10, Δήμος Δοκιμής",
            "work": "κλάδεμα",
            "lat": None,
            "lon": None,
            "page": 3,
            "excerpt": "Απόσπασμα 1",
            "formatted_address": None,
            "place_id": None,
        },
        {
            "point_name_raw": "Α 10",
            "point_name_canonical": "Οδός Α 10",
            "work": "κλάδεμα, αποψίλωση",
            "lat": None,
            "lon": None,
            "page": 5,
            "excerpt": "Απόσπασμα 2 πιο αναλυτικό",
            "formatted_address": "Αθήνα",
            "place_id": "pid-1",
        },
    ]

    out = doc._deduplicate_findings_pre_geocode(findings)

    assert len(out) == 1
    assert out[0]["point_name_canonical"] == "Οδός Α 10"
    assert out[0]["pages"] == [3, 5]
    assert out[0]["place_id"] == "pid-1"
    assert "αποψίλωση" in out[0]["work"]
