import pandas as pd

from ingest import stage2_load_erd as loader


def test_ingest_checks_only_new_documents_and_blocks_deleted_history(monkeypatch):
    source = pd.DataFrame([
        {"ada": "EXISTING", "subject": "Συντήρηση πυροσβεστήρων", "org_name_clean": "Χ"},
        {"ada": "DELETED", "subject": "Συντήρηση πυροσβεστήρων", "org_name_clean": "Χ"},
        {"ada": "NEW", "subject": "Συντήρηση αντιπυρικών ζωνών", "org_name_clean": "Χ"},
    ], index=[10, 20, 30])
    checked_adas = []
    original_policy = loader.diavgeia_source_is_excluded

    def record_policy_check(row):
        checked_adas.append(row["ada"])
        return original_policy(row)

    monkeypatch.setattr(loader, "diavgeia_source_is_excluded", record_policy_check)
    selected = loader.filter_new_diavgeia_rows(source, {"EXISTING"})

    assert selected["ada"].tolist() == ["EXISTING", "NEW"]
    assert checked_adas == ["DELETED", "NEW"]
    pd.testing.assert_series_equal(selected.loc[10], source.loc[10])
    assert source["ada"].tolist() == ["EXISTING", "DELETED", "NEW"]

    # Tuple construction must use the accepted frame for document rows and
    # ancillary document-type/beneficiary builders, without a second policy pass.
    rows = loader.diav_rows(selected, {}, {}, {}, {}, {}, apply_exclusions=False)
    assert [row[3] for row in rows] == ["EXISTING", "NEW"]
    assert checked_adas == ["DELETED", "NEW"]


def test_existing_documents_do_not_invoke_exclusion_policy(monkeypatch):
    source = pd.DataFrame([
        {"ada": "EXISTING", "subject": "Αλλαγή σημερινών κανόνων"},
    ])

    def unexpected_policy_check(row):
        raise AssertionError("Existing document was checked again")

    monkeypatch.setattr(loader, "diavgeia_source_is_excluded", unexpected_policy_check)
    selected = loader.filter_new_diavgeia_rows(source, {"EXISTING"})
    pd.testing.assert_frame_equal(selected, source)


def test_documents_without_ada_are_checked_as_potential_inserts():
    source = pd.DataFrame([
        {"ada": "", "subject": "Συντήρηση πυροσβεστήρων"},
        {"ada": None, "subject": "Συντήρηση αντιπυρικών ζωνών"},
    ])
    selected = loader.filter_new_diavgeia_rows(source, {"EXISTING"})
    assert selected["subject"].tolist() == ["Συντήρηση αντιπυρικών ζωνών"]

    selected_without_column = loader.filter_new_diavgeia_rows(source.drop(columns="ada"), set())
    assert selected_without_column["subject"].tolist() == ["Συντήρηση αντιπυρικών ζωνών"]


def test_empty_diavgeia_source_needs_no_exclusion_check(monkeypatch):
    def unexpected_policy_check(row):
        raise AssertionError("Empty source was checked")

    monkeypatch.setattr(loader, "diavgeia_source_is_excluded", unexpected_policy_check)
    source = pd.DataFrame(columns=["ada", "subject"])
    pd.testing.assert_frame_equal(loader.filter_new_diavgeia_rows(source, set()), source)


def test_existing_ada_lookup_is_restricted_to_source_candidates():
    class Cursor:
        def __init__(self):
            self.calls = []

        def execute(self, query, params):
            self.calls.append((query, params))

        def fetchall(self):
            return [("EXISTING",)]

    source = pd.DataFrame({"ada": ["EXISTING", "NEW", "EXISTING", "", None]})
    cursor = Cursor()
    assert loader.load_existing_diavgeia_adas(cursor, source) == {"EXISTING"}
    assert len(cursor.calls) == 1
    query, params = cursor.calls[0]
    assert query.startswith("SELECT ada FROM public.diavgeia")
    assert params == (["EXISTING", "NEW"],)


def test_no_ada_lookup_query_for_missing_document_identifiers():
    class Cursor:
        def execute(self, *args):
            raise AssertionError("No ADA query is needed")

    for source in (
        pd.DataFrame(columns=["ada"]),
        pd.DataFrame({"subject": ["Εργασίες"]}),
        pd.DataFrame({"ada": ["", None]}),
    ):
        assert loader.load_existing_diavgeia_adas(Cursor(), source) == set()
