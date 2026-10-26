import json
from pathlib import Path

import pandas as pd
import pytest

from src import fetch_diavgeia as mod


@pytest.fixture
def collector(monkeypatch, tmp_path: Path):
    data_dir = tmp_path / "data"
    state_dir = tmp_path / "state"
    log_dir = tmp_path / "logs"
    data_dir.mkdir()
    state_dir.mkdir()
    monkeypatch.setattr(mod, "DATA_DIR", data_dir)
    monkeypatch.setattr(mod, "CSV_PATH", data_dir / "diavgeia.csv")
    monkeypatch.setattr(mod, "STATE_DIR", state_dir)
    monkeypatch.setattr(mod, "STATE_FILE", state_dir / "state.json")
    monkeypatch.setattr(mod, "LEGACY_CSV_PATH", tmp_path / "legacy.csv")
    monkeypatch.setattr(mod, "LEGACY_STATE_FILE", tmp_path / "legacy-state.json")
    monkeypatch.setattr(mod, "LOG_DIR", log_dir)
    monkeypatch.setattr(mod, "RUN_LOG_FILE", log_dir / "fetch_runs.csv")
    monkeypatch.setenv("DOWNLOAD_DIAVGEIA_PDFS", "0")

    # Keep the real local normalization and exclusion policy. Only replace the
    # decision-view enrichment, which would otherwise make external requests.
    for enrich_name, ensure_name in (
        ("enrich_spending_approvals", "ensure_spending_enrichment_columns"),
        ("enrich_commitment_decisions", "ensure_commitment_enrichment_columns"),
        ("enrich_direct_assignment_decisions", "ensure_direct_enrichment_columns"),
        ("enrich_payment_finalization_decisions", "ensure_payment_enrichment_columns"),
    ):
        monkeypatch.setattr(mod, enrich_name, getattr(mod, ensure_name))

    def forbid_http(*args, **kwargs):
        raise AssertionError("Incremental collector tests must not make HTTP requests")

    monkeypatch.setattr(mod.requests.sessions.Session, "request", forbid_http)
    return mod


def new_record(ada="NEW", *, subject="Συντήρηση αντιπυρικών ζωνών", decision_type="ΚΑΝΟΝΙΣΤΙΚΗ ΠΡΑΞΗ"):
    return {
        "ada": ada,
        "subject": subject,
        "organization": {"label": "ΔΗΜΟΣ ΑΘΗΝΑΙΩΝ"},
        "decisionType": {"label": decision_type},
        "submissionTimestamp": "22/10/2026 12:00:00",
    }


def historical_record(*, subject="Πυροπροστασία σχολείου"):
    return {
        "ada": "HISTORICAL",
        "subject": subject,
        "organization": "{'label': 'ΔΗΜΟΣ ΑΘΗΝΑΙΩΝ'}",
        "org": "ΔΗΜΟΣ ΑΘΗΝΑΙΩΝ",
        "org_type": "Ιστορικός τύπος",
        "org_name_clean": "Αρχική Ονομασία",
        "decisionType": "{'label': 'ΚΑΝΟΝΙΣΤΙΚΗ ΠΡΑΞΗ'}",
        "submissionTimestamp": "21/10/2026 17:07:35",
        "review_note": "Διατηρείται η χειροκίνητη επιμέλεια",
    }


def read_csv(path):
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def test_exclusions_apply_only_to_new_rows_and_preserve_historical_values(collector):
    historical = pd.DataFrame([historical_record()])
    historical.to_csv(collector.CSV_PATH, index=False)

    added = collector.append_to_csv([
        new_record("NEW-ALLOWED"),
        new_record("NEW-EXCLUDED", subject="Πυροπροστασία σχολείου"),
    ])

    saved = read_csv(collector.CSV_PATH)
    assert added == 1
    assert saved["ada"].tolist() == ["HISTORICAL", "NEW-ALLOWED"]
    assert saved.iloc[0][historical.columns].to_dict() == historical.iloc[0].to_dict()


def test_historical_duplicates_do_not_hide_new_additions(collector):
    historical = pd.DataFrame([historical_record(subject="Αντιπυρικές ζώνες")] * 3)
    historical.to_csv(collector.CSV_PATH, index=False)

    assert collector.append_to_csv([new_record()]) == 1

    saved = read_csv(collector.CSV_PATH)
    assert saved["ada"].tolist() == ["HISTORICAL"] * 3 + ["NEW"]
    pd.testing.assert_frame_equal(
        saved.iloc[:3][historical.columns].reset_index(drop=True),
        historical,
    )


def test_duplicate_only_fetch_does_not_rewrite_csv(collector):
    record = new_record()
    assert collector.append_to_csv([record]) == 1
    before = collector.CSV_PATH.read_bytes()

    assert collector.append_to_csv([record]) == 0
    assert collector.CSV_PATH.read_bytes() == before


def test_existing_relevance_annotations_do_not_turn_a_repeat_into_a_new_row(collector):
    record = new_record()
    assert collector.append_to_csv([record]) == 1
    historical = read_csv(collector.CSV_PATH)
    historical["is_relevant"] = "True"
    historical["matched_keywords_subject"] = "αντιπυρ"
    historical["review_note"] = "Ελεγμένο ιστορικό"
    historical.to_csv(collector.CSV_PATH, index=False)
    before = collector.CSV_PATH.read_bytes()

    assert collector.append_to_csv([record]) == 0
    assert collector.CSV_PATH.read_bytes() == before


def test_counts_unique_new_rows_and_keeps_different_decision_types(collector):
    record = new_record()
    other_type = new_record(decision_type="ΛΟΙΠΕΣ ΑΤΟΜΙΚΕΣ ΔΙΟΙΚΗΤΙΚΕΣ ΠΡΑΞΕΙΣ")

    assert collector.append_to_csv([record, record, other_type]) == 2

    saved = read_csv(collector.CSV_PATH)
    assert saved["ada"].tolist() == ["NEW", "NEW"]
    assert saved["decisionType"].tolist() == [
        "ΚΑΝΟΝΙΣΤΙΚΗ ΠΡΑΞΗ",
        "ΛΟΙΠΕΣ ΑΤΟΜΙΚΕΣ ΔΙΟΙΚΗΤΙΚΕΣ ΠΡΑΞΕΙΣ",
    ]


def test_all_new_exclusions_leave_historical_csv_unchanged(collector):
    pd.DataFrame([historical_record()]).to_csv(collector.CSV_PATH, index=False)
    before = collector.CSV_PATH.read_bytes()

    assert collector.append_to_csv([new_record(subject="Πυροπροστασία σχολείου")]) == 0
    assert collector.CSV_PATH.read_bytes() == before


def test_failed_csv_write_keeps_existing_file(collector, monkeypatch):
    pd.DataFrame([historical_record()]).to_csv(collector.CSV_PATH, index=False)
    before = collector.CSV_PATH.read_bytes()

    def fail_write(frame, path_or_buf=None, *args, **kwargs):
        if hasattr(path_or_buf, "write"):
            path_or_buf.write("partial CSV write")
        else:
            Path(path_or_buf).write_text("partial CSV write", encoding="utf-8")
        raise OSError("CSV write failed")

    monkeypatch.setattr(pd.DataFrame, "to_csv", fail_write)

    with pytest.raises(OSError, match="CSV write failed"):
        collector.append_to_csv([new_record()])

    assert collector.CSV_PATH.read_bytes() == before


@pytest.mark.parametrize("stored_cutoff, expected", [
    ("23/10/2026 17:51:05", "21/10/2026 17:07:35"),
    ("20/10/2026 12:00:00", "20/10/2026 12:00:00"),
])
def test_state_does_not_skip_unsaved_csv_dates(collector, stored_cutoff, expected):
    pd.DataFrame([historical_record()]).to_csv(collector.CSV_PATH, index=False)
    state = {"last_fetch": stored_cutoff, "other_setting": "preserve"}
    collector.STATE_FILE.write_text(json.dumps(state), encoding="utf-8")
    before = collector.STATE_FILE.read_bytes()

    assert collector.load_state() == {**state, "last_fetch": expected}
    assert collector.STATE_FILE.read_bytes() == before


def test_state_without_persisted_file_uses_latest_csv_date(collector):
    pd.DataFrame([historical_record()]).to_csv(collector.CSV_PATH, index=False)

    assert collector.load_state() == {"last_fetch": "21/10/2026 17:07:35"}
    assert not collector.STATE_FILE.exists()


def test_verified_earlier_cutoff_remains_intact_without_scanning_csv(collector, monkeypatch):
    pd.DataFrame([historical_record()]).to_csv(collector.CSV_PATH, index=False)
    state = {
        "last_fetch": "20/10/2026 12:00:00",
        "csv_checkpoint_version": collector.CSV_CHECKPOINT_VERSION,
        "other_setting": "preserve",
    }
    collector.STATE_FILE.write_text(json.dumps(state), encoding="utf-8")
    before = collector.STATE_FILE.read_bytes()

    def forbid_scan(*args, **kwargs):
        raise AssertionError("Verified checkpoints must not scan the historical CSV")

    monkeypatch.setattr(pd, "read_csv", forbid_scan)

    assert collector.load_state() == state
    assert collector.STATE_FILE.read_bytes() == before


def test_write_failure_does_not_advance_state(collector, monkeypatch):
    pd.DataFrame([historical_record()]).to_csv(collector.CSV_PATH, index=False)
    collector.STATE_FILE.write_text(json.dumps({"last_fetch": "21/10/2026 17:07:35"}))
    before = collector.STATE_FILE.read_bytes()
    monkeypatch.setattr(collector, "fetch_new_decisions", lambda since: [new_record()])

    def fail_save(records):
        raise OSError("CSV write failed")

    monkeypatch.setattr(collector, "append_to_csv", fail_save)

    with pytest.raises(OSError, match="CSV write failed"):
        collector.main()

    assert collector.STATE_FILE.read_bytes() == before
    log = read_csv(collector.RUN_LOG_FILE).iloc[-1]
    assert log["success"] == "False"
    assert log["csv_updated"] == "False"


def test_intentionally_excluded_new_rows_can_advance_state(collector, monkeypatch):
    pd.DataFrame([historical_record()]).to_csv(collector.CSV_PATH, index=False)
    collector.STATE_FILE.write_text(json.dumps({"last_fetch": "21/10/2026 17:07:35"}))
    before_csv = collector.CSV_PATH.read_bytes()
    monkeypatch.setattr(
        collector,
        "fetch_new_decisions",
        lambda since: [new_record(subject="Πυροπροστασία σχολείου")],
    )

    collector.main()

    saved_state = json.loads(collector.STATE_FILE.read_text())
    assert saved_state["last_fetch"] == "22/10/2026 12:00:00"
    assert collector.CSV_PATH.read_bytes() == before_csv
    log = read_csv(collector.RUN_LOG_FILE).iloc[-1]
    assert log["success"] == "True"
    assert log["rows_added"] == "0"

    def forbid_scan(*args, **kwargs):
        raise AssertionError("Excluded-only success must not replay the old CSV cutoff")

    monkeypatch.setattr(pd, "read_csv", forbid_scan)

    assert collector.load_state() == saved_state
    assert collector.load_state()["last_fetch"] == "22/10/2026 12:00:00"
