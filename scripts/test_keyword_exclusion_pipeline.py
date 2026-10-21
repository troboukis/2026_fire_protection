"""Checks for the common exclusion policy without API or database access."""

from datetime import date

import pandas as pd
import pytest

from ingest import stage2_load_erd as loader
from src import fetch_diavgeia as diavgeia
from src import fetch_kimdis_procurements as kimdis
from src import keyword_exclusions as policy


@pytest.fixture(autouse=True)
def prevent_external_access(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Exclusion policy tests must not contact APIs or the database")

    monkeypatch.setattr("requests.sessions.Session.request", forbidden)
    monkeypatch.setattr("psycopg2.connect", forbidden)


@pytest.fixture
def collector():
    return kimdis.ProcurementCollector(
        cpvs={}, start_date=date(2026, 1, 1), end_date=date(2026, 1, 2), max_window_days=1
    )


def test_collectors_and_loader_use_the_same_keyword_policy(collector):
    assert collector.exclude_keywords is policy.DEFAULT_EXCLUDE_KEYWORDS
    assert kimdis.DEFAULT_EXCLUDE_KEYWORDS is policy.DEFAULT_EXCLUDE_KEYWORDS
    assert loader.DEFAULT_EXCLUDE_KEYWORDS is policy.DEFAULT_EXCLUDE_KEYWORDS


@pytest.mark.parametrize(
    "subject, organization, excluded",
    [
        ("Συντήρηση σχολικών κτιρίων", "ΔΗΜΟΣ Χ", True),
        ("Πυροπροστασία σχολείου", "ΔΗΜΟΣ Χ", True),
        ("Αναγόμωση πυροσβεστήρων", "ΔΗΜΟΣ Χ", True),
        ("Προμήθεια αντλητικών συγκροτημάτων", "ΔΗΜΟΣ Χ", True),
        ("Αντιμετώπιση χιονοπτώσεων", "ΔΗΜΟΣ Χ", True),
        ("Πιστοποιητικών ενεργητικής πυρασφάλειας", "ΔΗΜΟΣ Χ", True),
        ("Προμήθεια εξοπλισμού πυροπροστασίας", "ΝΟΣΟΚΟΜΕΙΟ Χ", True),
        ("Σύστημα πυρανίχνευσης δασών", "ΔΗΜΟΣ Χ", False),
        ("Ευφυές σύστημα έγκαιρης πυρανίχνευσης", "ΔΗΜΟΣ Χ", False),
        ("Συντήρηση πυροσβεστήρων δημοτικών κτιρίων", "ΔΗΜΟΣ Χ", False),
        ("Μελέτη ενεργητικής πυροπροστασίας κτιρίου", "ΔΗΜΟΣ Χ", False),
        ("Συντήρηση αντιπυρικών ζωνών", "ΔΗΜΟΣ Χ", False),
        ("Συντήρηση χώρων πρασίνου", "ΔΗΜΟΣ Χ", True),
        ("Συντήρηση κοινόχρηστων χώρων πρασίνου", "ΔΗΜΟΣ Χ", False),
        ("Συντήρηση χώρων αστικού πρασίνου", "ΔΗΜΟΣ Χ", False),
    ],
)
def test_kimdis_and_persisted_diavgeia_agree(collector, subject, organization, excluded):
    item = {
        "title": subject,
        "organization": {"value": organization},
        "objectDetailsList": [],
        "cancelled": False,
    }
    persisted = pd.DataFrame({"subject": [subject], "org_name_clean": [organization]})

    assert collector.is_excluded(item) is excluded
    assert diavgeia.filter_excluded_org_rows(persisted).empty is excluded


@pytest.mark.parametrize(
    "field, text",
    [
        ("title", "Συντήρηση σχολικών κτιρίων"),
        ("shortDescriptions", "Αντιμετώπιση χιονοπτώσεων"),
        ("organization_value", "ΝΟΣΟΚΟΜΕΙΟ Χ"),
    ],
)
def test_stage2_excludes_procurement_fields_before_resolving_context(monkeypatch, field, text):
    def forbidden(**kwargs):
        pytest.fail("Excluded procurements must be skipped before context resolution")

    monkeypatch.setattr(loader, "resolve_procurement_context", forbidden)
    frame = pd.DataFrame([{field: text}])
    assert loader.procurement_rows(frame, {}, {}, {}, {}, {}, {}, {}) == []


@pytest.mark.parametrize(
    "fields",
    [
        {"subject": "Αντιμετώπιση χιονοπτώσεων"},
        {"subject": "Προμήθεια εξοπλισμού πυροπροστασίας", "org_name_clean": "ΝΟΣΟΚΟΜΕΙΟ Χ"},
    ],
)
def test_stage2_excludes_diavgeia_before_resolving_context(monkeypatch, fields):
    def forbidden(**kwargs):
        pytest.fail("Excluded decisions must be skipped before context resolution")

    monkeypatch.setattr(loader, "resolve_diavgeia_context", forbidden)
    assert loader.diav_rows(pd.DataFrame([fields]), {}, {}, {}, {}, {}) == []


@pytest.mark.parametrize(
    "subject",
    [
        "Σύστημα πυρανίχνευσης δασών",
        "Συντήρηση πυροσβεστήρων δημοτικών κτιρίων",
        "Συντήρηση κοινόχρηστων χώρων πρασίνου",
    ],
)
def test_stage2_retains_rows_without_a_shared_keyword_match(monkeypatch, subject):
    monkeypatch.setattr(loader, "resolve_procurement_context", lambda **kwargs: (None, None, None, None))
    monkeypatch.setattr(loader, "resolve_diavgeia_context", lambda **kwargs: (None, None, None))

    procurement = pd.DataFrame({"title": [subject], "organization_value": ["ΔΗΜΟΣ Χ"]})
    decision = pd.DataFrame({"subject": [subject], "org_name_clean": ["ΔΗΜΟΣ Χ"]})

    assert len(loader.procurement_rows(procurement, {}, {}, {}, {}, {}, {}, {})) == 1
    assert len(loader.diav_rows(decision, {}, {}, {}, {}, {})) == 1


def test_phrases_do_not_span_fields_and_word_gaps_are_bounded():
    assert not policy.text_has_exclusion("Πιστοποιητικό", "Πυρασφάλειας")
    assert policy.text_has_exclusion("Πιστοποιητικό ενεργητικής και πυρασφάλειας")
    assert not policy.text_has_exclusion("Πιστοποιητικό ενεργητικής και παθητικής πυρασφάλειας")


def test_sql_keyword_filter_reads_each_phrases_gap_without_database_access():
    class CaptureCursor:
        def __init__(self):
            self.calls = []

        def execute(self, statement, params=None):
            self.calls.append((statement, params))

        def fetchone(self):
            return (False,) if len(self.calls) == 1 else (0,)

    cursor = CaptureCursor()
    assert loader.prune_excluded_procurements(
        cursor, None, ["ΣΥΝΤΉΡΗΣΗ ΧΏΡΩΝ ΠΡΑΣΊΝΟΥ", "πιστοποιητικ πυρασφαλ"], dry_run=True
    ) == 0
    patterns = cursor.calls[-1][1]
    assert "{0,0}" in patterns[0]
    assert "{0,2}" in patterns[2]
    assert all(statement.lstrip().startswith("SELECT") for statement, _ in cursor.calls)
