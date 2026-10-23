"""Checks for the common exclusion policy without API or database access."""

from datetime import date

import pandas as pd
import pytest

from ingest import stage2_load_erd as loader
from src import fetch_diavgeia as diavgeia
from src import fetch_kimdis_procurements as kimdis
from src import filter_relevance as relevance
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


def test_reviewed_ada_is_excluded_from_collection_persisted_rows_relevance_and_import():
    reviewed = '6ΤΕ0ΟΞΝΑ-6ΔΨ'
    decision = {
        'ada': reviewed, 'subject': 'Καθαρισμός δασών για πυροπροστασία',
        'org_name_clean': 'ΦΟΡΕΑΣ ΔΟΚΙΜΗΣ', 'decisionType': 'ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ',
    }
    frame = pd.DataFrame([decision, {**decision, 'ada': 'OTHER-ADA'}])
    assert diavgeia.record_is_confirmed_irrelevant(decision)
    assert not diavgeia.record_is_confirmed_irrelevant({**decision, 'ada': 'OTHER-ADA'})
    assert list(diavgeia.filter_excluded_org_rows(frame)['ada']) == ['OTHER-ADA']
    assert loader.diavgeia_source_is_excluded(pd.Series(decision))
    assert not loader.diavgeia_source_is_excluded(pd.Series({**decision, 'ada': 'OTHER-ADA'}))
    result, _ = relevance.apply_relevance_filter(frame, {}, [('πυροπροστασία', 'πυροπροστασια')])
    assert not bool(result.loc[0, 'is_relevant'])


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
        ("Συντήρηση πυροσβεστήρων δημοτικών κτιρίων", "ΔΗΜΟΣ Χ", True),
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


@pytest.mark.parametrize("text", [
    "Πυροπροστασία γυμνασίου", "Πυροπροστασία λυκείου", "Νηπιαγωγείου",
    "Δημοτικό σχολείο", "Θεατρικής σκηνής", "Πανεπιστημιακές εγκαταστάσεις",
    "Προμήθεια πυροσβεστήρων πολιτικής προστασίας", "ΓΝΘ", "Γ.Ν.Θ.",
    "Ανακαίνιση κτιρίου", "Νοσοκομειακού εξοπλισμού", "Αλατοδιανομέων", "Εκχιονισμού οδών",
])
@pytest.mark.parametrize("field", ["title", "organization", "description"])
def test_requested_keywords_apply_in_all_collection_and_ingestion_fields(collector, monkeypatch, field, text):
    def forbidden(**kwargs):
        pytest.fail("Excluded rows must be skipped before context resolution")

    monkeypatch.setattr(loader, "resolve_procurement_context", forbidden)
    monkeypatch.setattr(loader, "resolve_diavgeia_context", forbidden)
    title = text if field == "title" else "Αντιπυρική προστασία"
    organization = text if field == "organization" else "ΔΗΜΟΣ Χ"
    description = text if field == "description" else ""
    item = {
        "title": title,
        "organization": {"value": organization},
        "objectDetailsList": [{"shortDescription": description}],
        "cancelled": False,
    }
    procurement = pd.DataFrame({
        "title": [title], "organization_value": [organization], "shortDescriptions": [description],
    })

    assert collector.is_excluded(item)
    assert loader.procurement_rows(procurement, {}, {}, {}, {}, {}, {}, {}) == []
    if field != "description":
        decision = pd.DataFrame({"subject": [title], "org_name_clean": [organization]})
        assert diavgeia.filter_excluded_org_rows(decision).empty
        assert loader.diav_rows(decision, {}, {}, {}, {}, {}) == []


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
        "Προμήθεια πυροσβεστικών οχημάτων πολιτικής προστασίας",
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


@pytest.mark.parametrize("subject,description", [
    ("Μισθοδοσία υπαλλήλων", ""),
    ("Υπερωρίες διοικητικού προσωπικού", ""),
    ("Εισφορές εργαζομένων", ""),
    ("Καταβολή αποδοχών προσωπικού", ""),
    ("", "Μισθοδοσία υπαλλήλων"),
    ("", ""),
])
def test_payroll_report_rule_does_not_exclude_collection_or_ingestion(monkeypatch, subject, description):
    record = {
        "ada": "KEEP-PAYROLL", "subject": subject, "description": description,
        "org_name_clean": "ΔΗΜΟΣ Χ", "organization": {"label": "ΔΗΜΟΣ Χ"},
    }
    monkeypatch.setattr(diavgeia, "KEYWORDS", ["πυρο"])
    monkeypatch.setattr(diavgeia, "fetch_search_page", lambda *args: {
        "info": {"total": 1}, "decisions": [record],
    })
    monkeypatch.setattr(loader, "resolve_diavgeia_context", lambda **kwargs: (None, None, None))

    assert diavgeia.fetch_new_decisions(None) == [record]
    frame = pd.DataFrame([record])
    assert diavgeia.filter_excluded_org_rows(frame)["ada"].tolist() == [record["ada"]]
    assert not loader.diavgeia_source_is_excluded(frame.iloc[0])
    assert [row[3] for row in loader.diav_rows(frame, {}, {}, {}, {}, {})] == [record["ada"]]

    # The ordinary PDF relevance rule remains sufficient for collection.
    enriched, stats = relevance.apply_relevance_filter(
        frame, {record["ada"]: "Αντιπυρική περίοδος"},
        relevance.build_keyword_specs(["αντιπυρ"]), progress_every=100,
    )
    assert bool(enriched.iloc[0]["pdf_match"])
    assert bool(enriched.iloc[0]["is_relevant"])
    assert stats["rows_relevant"] == 1


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


@pytest.mark.parametrize("excluded_fields", [
    {"title": "Πυροπροστασία γυμνασίου"},
    {"shortDescriptions": "Προμήθεια πυροσβεστήρων"},
    {"organization_value": "ΠΑΝΕΠΙΣΤΗΜΙΟ Χ"},
    {"organization_value": "Γ.Ν. Χ"},
    {"referenceNumber": "26SYMV019427117"},
])
def test_main_keeps_cpv_and_payment_with_their_contract_after_exclusion(monkeypatch, excluded_fields):
    raw = pd.DataFrame([
        {
            "referenceNumber": "A", "title": "Συντήρηση αντιπυρικών ζωνών",
            "cpv_keys": "77231000-8", "cpv_values": "Δασικές υπηρεσίες",
            "totalCostWithoutVAT": "1000", "totalCostWithVAT": "1240",
            "nextRefNo": "",
        },
        {
            "referenceNumber": "B", "title": "Συντήρηση αντιπυρικών ζωνών",
            "cpv_keys": "35111300-8", "cpv_values": "Εξαιρούμενο είδος",
            "totalCostWithoutVAT": "2000", "totalCostWithVAT": "2480",
            "prevReferenceNo": "A", **excluded_fields,
        },
        {
            "referenceNumber": "C", "title": "Καθαρισμός δασικών εκτάσεων",
            "cpv_keys": "90511300-5", "cpv_values": "Υπηρεσίες καθαρισμού",
            "totalCostWithoutVAT": "3000", "totalCostWithVAT": "3720",
        },
    ]).fillna("")
    bundle = loader.CsvBundle(
        raw=raw,
        diav=pd.DataFrame([
            {"ada": "KEEP", "subject": "Συντήρηση αντιπυρικών ζωνών", "org_name_clean": "ΔΗΜΟΣ Χ"},
            {"ada": "DROP-TITLE", "subject": "Πυροπροστασία γυμνασίου", "org_name_clean": "ΔΗΜΟΣ Χ"},
            {"ada": "DROP-ORG", "subject": "Πυροπροστασία", "org_name_clean": "ΠΑΝΕΠΙΣΤΗΜΙΟ Χ"},
            {"ada": "DROP-HOSPITAL", "subject": "Πυροπροστασία", "org_name_clean": "Γ.Ν. Χ"},
            {"ada": "KEEP-PAYROLL", "subject": "Μισθοδοσία υπαλλήλων", "org_name_clean": "ΠΟΛΙΤΙΚΗΣ ΠΡΟΣΤΑΣΙΑΣ"},
            {"ada": "KEEP-SUBJECTLESS", "subject": "", "description": "Μισθοδοσία υπαλλήλων", "org_name_clean": "ΔΗΜΟΣ Χ"},
        ]),
        fire=pd.DataFrame(), fund=pd.DataFrame(),
        org_map=pd.DataFrame(), region_map=pd.DataFrame(),
        expanded_map=pd.DataFrame(columns=["source_entity_type"]),
    )
    inserted_contracts = {}
    batches = {}

    class MemoryCursor:
        rowcount = 0

        def execute(self, statement, params=None):
            if "INSERT INTO public.procurement (" in statement:
                self.inserted_id = len(inserted_contracts) + 1
                inserted_contracts[params[1]] = self.inserted_id

        def fetchall(self):
            return []

        def fetchone(self):
            return (self.inserted_id,)

        def close(self):
            pass

    class MemoryConnection:
        def cursor(self):
            return MemoryCursor()

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            pass

    def capture_batch(cursor, statement, rows, **kwargs):
        for table in ("cpv", "payment"):
            if f"INSERT INTO public.{table} (" in statement:
                batches[table] = list(rows)

    monkeypatch.setattr("sys.argv", [
        "stage2_load_erd.py", "--tables", "procurement,cpv,payment",
        "--skip-prune-excluded-procurements",
    ])
    monkeypatch.setattr(loader, "read_csvs", lambda **kwargs: bundle)
    monkeypatch.setattr(loader, "resolve_database_url", lambda value: "memory")
    monkeypatch.setattr(loader.psycopg2, "connect", lambda value: MemoryConnection())
    monkeypatch.setattr(loader.psycopg2.extras, "execute_batch", capture_batch)
    monkeypatch.setattr(loader, "zero_superseded_payment_amounts", lambda cursor: 0)

    loader.main()

    assert bundle.diav["ada"].tolist() == ["KEEP", "KEEP-PAYROLL", "KEEP-SUBJECTLESS"]
    assert list(inserted_contracts) == ["A", "C"]
    assert batches["cpv"] == [
        ("77231000-8", "Δασικές υπηρεσίες", inserted_contracts["A"]),
        ("90511300-5", "Υπηρεσίες καθαρισμού", inserted_contracts["C"]),
    ]
    payments = {row[0]: (row[7], row[8]) for row in batches["payment"]}
    # The excluded successor still zeroes its predecessor before it is filtered.
    assert payments[inserted_contracts["A"]] == (1240.0, 0.0)
    assert payments[inserted_contracts["C"]] == (3720.0, 3000.0)
