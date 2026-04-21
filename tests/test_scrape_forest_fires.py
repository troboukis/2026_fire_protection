from __future__ import annotations

import importlib.util
from datetime import date, datetime
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "src" / "scrape_forest_fires.py"
SPEC = importlib.util.spec_from_file_location("scrape_forest_fires", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_resolve_municipality_from_municipality_and_settlement():
    resolved = MODULE.resolve_municipality("ΑΓΡΙΝΙΟΥ - ΝΕΑΠΟΛΗΣ", "ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ")
    assert resolved["municipality_key"] == "9123"
    assert resolved["municipality_normalized_value"] == "ΑΓΡΙΝΙΟΥ"
    assert resolved["municipality_raw"] == "ΑΓΡΙΝΙΟΥ - ΝΕΑΠΟΛΗΣ"


def test_resolve_municipality_with_hyphenated_municipality_name():
    resolved = MODULE.resolve_municipality("ΠΑΠΑΓΟΥ - ΧΟΛΑΡΓΟΥ - ΠΑΠΑΓΟΥ", "ΑΤΤΙΚΗΣ")
    assert resolved["municipality_key"] == "9175"
    assert resolved["municipality_normalized_value"] == "ΠΑΠΑΓΟΥ - ΧΟΛΑΡΓΟΥ"
    assert resolved["municipality_raw"] == "ΠΑΠΑΓΟΥ - ΧΟΛΑΡΓΟΥ - ΠΑΠΑΓΟΥ"


def test_resolve_municipality_prefers_exact_canonical_match_when_alias_is_ambiguous():
    resolved = MODULE.resolve_municipality("ΔΕΛΦΩΝ", "ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ")
    assert resolved["municipality_key"] == "9165"
    assert resolved["municipality_normalized_value"] == "ΔΕΛΦΩΝ"
    assert resolved["municipality_raw"] == "ΔΕΛΦΩΝ"


def test_compute_days_burning_is_calendar_inclusive():
    assert MODULE.compute_days_burning("21/04/2026", date(2026, 4, 21)) == "1"
    assert MODULE.compute_days_burning("20/04/2026", date(2026, 4, 21)) == "2"
    assert MODULE.compute_days_burning("", date(2026, 4, 21)) == ""


def test_compute_status_updated_at_from_relative_greek_text():
    scraped_at = datetime(2026, 4, 21, 10, 5, 23)
    raw = "ΠΕΡΙΦΕΡΕΙΑ ΘΕΣΣΑΛΙΑΣ Τελευταία Ενημέρωση πριν από 17 λεπτά"
    assert MODULE.compute_status_updated_at(raw, scraped_at) == "2026-04-21T09:48:23"


def test_merge_updates_existing_incident_when_closure_row_has_no_start():
    existing = [{
        "incident_key": "inc_1",
        "first_seen_at": "2026-04-20T09:00:00",
        "last_seen_at": "2026-04-20T09:00:00",
        "is_current": "true",
        "category": "ΔΑΣΙΚΕΣ ΠΥΡΚΑΓΙΕΣ",
        "region": "ΘΕΣΣΑΛΙΑΣ",
        "regional_unit": "",
        "municipality_key": "9105",
        "municipality_normalized_value": "ΒΟΛΟΥ",
        "municipality_raw": "ΒΟΛΟΥ - ΝΕΑΣ ΙΩΝΙΑΣ",
        "fuel_type": "ΓΕΩΡΓΙΚΗ ΕΚΤΑΣΗ",
        "start": "20/04/2026",
        "days_burning": "1",
        "status_updated_at": "2026-04-20T08:55:00",
        "status": "ΠΛΗΡΗΣ ΕΛΕΓΧΟΣ",
        "raw": "old raw",
    }]
    merged = MODULE.merge_with_existing([{
        "category": "ΔΑΣΙΚΕΣ ΠΥΡΚΑΓΙΕΣ",
        "region": "ΘΕΣΣΑΛΙΑΣ",
        "regional_unit": "",
        "municipality_key": "9105",
        "municipality_normalized_value": "ΒΟΛΟΥ",
        "municipality_raw": "ΒΟΛΟΥ - ΝΕΑΣ ΙΩΝΙΑΣ",
        "fuel_type": "ΓΕΩΡΓΙΚΗ ΕΚΤΑΣΗ",
        "start": "",
        "status": "ΛΗΞΗ",
        "raw": "ΠΕΡΙΦΕΡΕΙΑ ΘΕΣΣΑΛΙΑΣ Τελευταία Ενημέρωση πριν από 3 λεπτά",
    }], existing)

    assert len(merged) == 1
    assert merged[0]["incident_key"] == "inc_1"
    assert merged[0]["first_seen_at"] == "2026-04-20T09:00:00"
    assert merged[0]["status"] == "ΛΗΞΗ"
    assert merged[0]["is_current"] == "true"


def test_merge_preserves_old_incident_when_not_in_latest_scrape():
    existing = [{
        "incident_key": "inc_2",
        "first_seen_at": "2026-04-19T09:00:00",
        "last_seen_at": "2026-04-20T09:00:00",
        "is_current": "true",
        "category": "ΔΑΣΙΚΕΣ ΠΥΡΚΑΓΙΕΣ",
        "region": "ΔΥΤΙΚΗΣ ΕΛΛΑΔΑΣ",
        "regional_unit": "",
        "municipality_key": "9123",
        "municipality_normalized_value": "ΑΓΡΙΝΙΟΥ",
        "municipality_raw": "ΑΓΡΙΝΙΟΥ - ΝΕΑΠΟΛΗΣ",
        "fuel_type": "ΚΑΛΑΜΙΑ - ΒΑΛΤΟΙ",
        "start": "19/04/2026",
        "days_burning": "2",
        "status_updated_at": "2026-04-20T08:55:00",
        "status": "ΣΕ ΕΞΕΛΙΞΗ",
        "raw": "old raw",
    }]
    merged = MODULE.merge_with_existing([], existing)

    assert len(merged) == 1
    assert merged[0]["incident_key"] == "inc_2"
    assert merged[0]["is_current"] == "false"
    assert merged[0]["last_seen_at"] == "2026-04-20T09:00:00"
