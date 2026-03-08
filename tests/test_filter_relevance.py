from __future__ import annotations

from pathlib import Path

import pandas as pd

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
