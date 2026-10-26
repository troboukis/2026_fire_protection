from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src import filter_relevance as mod


FIXTURES = Path(__file__).parent / "fixtures" / "relevance"


def test_build_pdf_text_lookup_keeps_longest_text_for_duplicate_ada():
    lookup = mod.build_pdf_text_lookup(FIXTURES / "pdf_pages_sample.csv", chunksize=1)

    assert lookup["ADA-2"] == "Περιλαμβάνει έργα πρόληψης δασικών πυρκαγιών σε δασικές εκτάσεις"


def test_apply_relevance_filter_marks_subject_and_pdf_matches():
    raw_df = pd.read_csv(FIXTURES / "raw_diavgeia_sample.csv")
    pdf_lookup = mod.build_pdf_text_lookup(FIXTURES / "pdf_pages_sample.csv")
    keyword_specs = mod.build_keyword_specs(["αποψιλ", "δασικων πυρκαγιων"])

    enriched, stats = mod.apply_relevance_filter(raw_df, pdf_lookup, keyword_specs, progress_every=1)

    assert stats["rows_total"] == 3
    assert stats["rows_relevant"] == 2
    assert bool(enriched.loc[0, "subject_match"]) is True
    assert bool(enriched.loc[1, "pdf_match"]) is True
    assert bool(enriched.loc[2, "is_relevant"]) is False


def test_apply_relevance_filter_rejects_tee_planning_act_even_with_keyword():
    raw_df = pd.DataFrame([{
        "ada": "TEST-TEE",
        "subject": "Ενημέρωση μελέτης πυρασφάλειας",
        "org_name_clean": "ΤΕΧΝΙΚΟ ΕΠΙΜΕΛΗΤΗΡΙΟ ΕΛΛΑΔΑΣ",
        "decisionType": "ΠΡΑΞΕΙΣ ΧΩΡΟΤΑΞΙΚΟΥ - ΠΟΛΕΟΔΟΜΙΚΟΥ ΠΕΡΙΕΧΟΜΕΝΟΥ",
    }])

    enriched, _ = mod.apply_relevance_filter(
        raw_df,
        pdf_text_by_ada={},
        keyword_specs=mod.build_keyword_specs(["πυρασφαλειας"]),
        progress_every=1,
    )

    assert bool(enriched.loc[0, "subject_match"]) is True
    assert bool(enriched.loc[0, "is_relevant"]) is False


def test_run_relevance_filter_writes_raw_filtered_and_log(tmp_path: Path):
    input_csv = tmp_path / "raw.csv"
    pdf_csv = tmp_path / "pdf.csv"
    filtered_csv = tmp_path / "filtered.csv"
    log_csv = tmp_path / "log.csv"

    input_csv.write_text((FIXTURES / "raw_diavgeia_sample.csv").read_text(encoding="utf-8"), encoding="utf-8")
    pdf_csv.write_text((FIXTURES / "pdf_pages_sample.csv").read_text(encoding="utf-8"), encoding="utf-8")

    stats = mod.run_relevance_filter(input_csv, pdf_csv, filtered_csv, log_csv, progress_every=1)

    filtered = pd.read_csv(filtered_csv)
    log_df = pd.read_csv(log_csv)
    raw_out = pd.read_csv(input_csv)

    assert stats["rows_relevant"] == 2
    assert len(filtered) == 2
    assert len(log_df) == 1
    assert "is_relevant" in raw_out.columns


def test_run_relevance_filter_preserves_saved_decisions_and_checks_only_new_rows(tmp_path, monkeypatch):
    input_csv = tmp_path / "raw.csv"
    filtered_csv = tmp_path / "filtered.csv"
    log_csv = tmp_path / "log.csv"
    rows = pd.DataFrame([
        {
            "ada": "OLD-KEEP",
            "subject": "Ιστορική απόφαση με διατηρημένη αξιολόγηση",
            "org_name_clean": "ΓΕΝΙΚΟ ΝΟΣΟΚΟΜΕΙΟ ΑΘΗΝΩΝ",
            "subject_match": True,
            "pdf_match": False,
            "pdf_available_for_filter": False,
            "is_relevant": True,
            "matched_keywords_subject": "παλιό κριτήριο",
            "matched_keywords_pdf": None,
        },
        {
            "ada": "OLD-REJECT",
            "subject": "Αποψίλωση για την προστασία δασών",
            "subject_match": False,
            "pdf_match": False,
            "pdf_available_for_filter": False,
            "is_relevant": False,
            "matched_keywords_subject": None,
            "matched_keywords_pdf": None,
        },
        {"ada": "NEW-KEEP", "subject": "Αποψίλωση για την προστασία δασών"},
    ])
    rows.to_csv(input_csv, index=False)
    saved_before = pd.read_csv(input_csv).iloc[:2].copy()
    checked_adas = []
    checked_subjects = []
    actual_find_matches = mod.find_matching_keywords

    def check_new_organization(row):
        checked_adas.append(row["ada"])
        assert row["ada"] == "NEW-KEEP"
        return False

    def check_new_subject(subject, specs):
        checked_subjects.append(subject)
        assert subject == "Αποψίλωση για την προστασία δασών"
        return actual_find_matches(subject, specs)

    def check_new_reviewed_ada(org, decision_type, ada):
        assert ada == "NEW-KEEP"
        return False

    monkeypatch.setattr(mod, "record_has_hospital_organization", check_new_organization)
    monkeypatch.setattr(mod, "find_matching_keywords", check_new_subject)
    monkeypatch.setattr(mod, "is_confirmed_irrelevant_decision", check_new_reviewed_ada)
    monkeypatch.setattr(mod, "build_pdf_text_lookup", lambda path: {})

    stats = mod.run_relevance_filter(input_csv, tmp_path / "pdf.csv", filtered_csv, log_csv)

    saved_after = pd.read_csv(input_csv)
    pd.testing.assert_frame_equal(saved_before, saved_after.iloc[:2], check_dtype=False)
    assert checked_adas == ["NEW-KEEP"]
    assert checked_subjects == ["Αποψίλωση για την προστασία δασών"]
    assert pd.read_csv(filtered_csv)["ada"].tolist() == ["OLD-KEEP", "NEW-KEEP"]
    assert stats["rows_total"] == 3
    assert stats["rows_relevant"] == 2
    assert stats["rows_not_relevant"] == 1
    assert stats["rows_subject_match"] == 2
    assert stats["rows_processed"] == 1
    assert stats["rows_skipped"] == 2


def test_run_relevance_filter_with_no_new_rows_preserves_false_and_skips_all_filters(tmp_path, monkeypatch):
    input_csv = tmp_path / "raw.csv"
    filtered_csv = tmp_path / "filtered.csv"
    log_csv = tmp_path / "log.csv"
    pd.DataFrame([
        {"ada": "OLD-KEEP", "subject": "Ιστορική απόφαση", "is_relevant": " True "},
        {"ada": "OLD-REJECT", "subject": "Αποψίλωση δασών", "is_relevant": " fAlSe "},
    ]).to_csv(input_csv, index=False)

    def unexpected_recheck(*args, **kwargs):
        pytest.fail("Saved relevance decisions must not be recomputed")

    monkeypatch.setattr(mod, "apply_relevance_filter", unexpected_recheck)
    monkeypatch.setattr(mod, "build_pdf_text_lookup", unexpected_recheck)

    stats = mod.run_relevance_filter(input_csv, tmp_path / "pdf.csv", filtered_csv, log_csv)

    assert pd.read_csv(input_csv)["is_relevant"].tolist() == [" True ", " fAlSe "]
    assert pd.read_csv(filtered_csv)["ada"].tolist() == ["OLD-KEEP"]
    assert stats["rows_relevant"] == 1
    assert stats["rows_not_relevant"] == 1
    assert stats["rows_processed"] == 0
    assert stats["rows_skipped"] == 2


def test_run_relevance_filter_evaluates_missing_and_invalid_decisions_once(tmp_path, monkeypatch):
    input_csv = tmp_path / "raw.csv"
    filtered_csv = tmp_path / "filtered.csv"
    log_csv = tmp_path / "log.csv"
    pd.DataFrame([
        {"ada": "MISSING", "subject": "Αποψίλωση δασών", "is_relevant": None},
        {"ada": "INVALID", "subject": "Άσχετη ανακοίνωση", "is_relevant": "pending"},
    ]).to_csv(input_csv, index=False)
    monkeypatch.setattr(mod, "build_pdf_text_lookup", lambda path: {})

    first = mod.run_relevance_filter(input_csv, tmp_path / "pdf.csv", filtered_csv, log_csv)
    second = mod.run_relevance_filter(input_csv, tmp_path / "pdf.csv", filtered_csv, log_csv)

    assert first["rows_processed"] == 2
    assert first["rows_relevant"] == 1
    assert second["rows_processed"] == 0
    assert second["rows_skipped"] == 2
    assert pd.read_csv(filtered_csv)["ada"].tolist() == ["MISSING"]
    assert len(pd.read_csv(log_csv)) == 2


def test_run_relevance_filter_handles_all_empty_decision_columns(tmp_path, monkeypatch):
    input_csv = tmp_path / "raw.csv"
    pd.DataFrame([
        {"ada": "NEW", "subject": "Αποψίλωση δασών", **dict.fromkeys(mod.RELEVANCE_COLUMNS)},
    ]).to_csv(input_csv, index=False)
    monkeypatch.setattr(mod, "build_pdf_text_lookup", lambda path: {})

    stats = mod.run_relevance_filter(
        input_csv, tmp_path / "pdf.csv", tmp_path / "filtered.csv", tmp_path / "log.csv"
    )

    assert stats["rows_processed"] == 1
    assert stats["rows_relevant"] == 1
    assert bool(pd.read_csv(input_csv).loc[0, "is_relevant"]) is True
