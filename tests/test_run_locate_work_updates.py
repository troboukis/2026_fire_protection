from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src import run_locate_work_updates as mod


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql, params):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return self.rows

    def close(self):
        return None


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows

    def cursor(self):
        return FakeCursor(self.rows)

    def close(self):
        return None


def test_load_and_save_state_round_trip(tmp_path: Path):
    state_file = tmp_path / "state.json"
    mod.save_state(state_file, ["B", "A", "A"])
    state = mod.load_state(state_file)

    assert state["processed_reference_numbers"] == ["A", "B"]


def test_fetch_candidate_procurements_filters_already_processed_by_default(monkeypatch):
    rows = [
        ("26SYMV001", "ΔΗΜΟΣ ΔΟΚΙΜΗΣ", "Τίτλος 1"),
        ("26SYMV002", "ΑΔΜΗΕ", "Τίτλος 2"),
    ]
    monkeypatch.setattr(mod.psycopg2, "connect", lambda db_url: FakeConnection(rows))

    out = mod.fetch_candidate_procurements("postgresql://x", {"26SYMV001"}, limit=None)

    assert out == [{
        "reference_number": "26SYMV002",
        "organization_value": "ΑΔΜΗΕ",
        "title": "Τίτλος 2",
    }]


def test_fetch_candidate_procurements_can_reprocess_missing_rows(monkeypatch):
    rows = [
        ("26SYMV001", "ΔΗΜΟΣ ΔΟΚΙΜΗΣ", "Τίτλος 1"),
        ("26SYMV002", "ΑΔΜΗΕ", "Τίτλος 2"),
    ]
    monkeypatch.setattr(mod.psycopg2, "connect", lambda db_url: FakeConnection(rows))

    out = mod.fetch_candidate_procurements(
        "postgresql://x",
        {"26SYMV001"},
        limit=None,
        reprocess_missing_works=True,
    )

    assert out == [
        {
            "reference_number": "26SYMV001",
            "organization_value": "ΔΗΜΟΣ ΔΟΚΙΜΗΣ",
            "title": "Τίτλος 1",
        },
        {
            "reference_number": "26SYMV002",
            "organization_value": "ΑΔΜΗΕ",
            "title": "Τίτλος 2",
        },
    ]


def test_main_logs_and_updates_state_for_success_and_no_findings(tmp_path: Path, monkeypatch):
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "log.csv"

    monkeypatch.setattr(mod, "parse_args", lambda: SimpleNamespace(
        db_path=None,
        state_file=state_file,
        log_csv=log_file,
        limit=None,
        reference_number=[],
        reprocess_missing_works=False,
        debug=False,
    ))
    monkeypatch.setattr(mod, "resolve_database_url", lambda db_path: "postgresql://x")
    monkeypatch.setattr(mod, "fetch_candidate_procurements", lambda db_url, already_processed, limit, reprocess_missing_works=False: [
        {"reference_number": "26SYMV001", "organization_value": "ΔΗΜΟΣ ΔΟΚΙΜΗΣ", "title": "A"},
        {"reference_number": "26SYMV002", "organization_value": "ΑΔΜΗΕ", "title": "B"},
    ])

    def fake_process(reference_number: str, db_url: str, debug: bool) -> int:
        return 2 if reference_number == "26SYMV001" else 0

    monkeypatch.setattr(mod, "process_reference", fake_process)

    mod.main()

    state = json.loads(state_file.read_text(encoding="utf-8"))
    log_text = log_file.read_text(encoding="utf-8")

    assert state["processed_reference_numbers"] == ["26SYMV001", "26SYMV002"]
    assert "success" in log_text
    assert "no_findings" in log_text


def test_main_raises_on_any_processing_error(tmp_path: Path, monkeypatch):
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "log.csv"

    monkeypatch.setattr(mod, "parse_args", lambda: SimpleNamespace(
        db_path=None,
        state_file=state_file,
        log_csv=log_file,
        limit=None,
        reference_number=[],
        reprocess_missing_works=False,
        debug=False,
    ))
    monkeypatch.setattr(mod, "resolve_database_url", lambda db_path: "postgresql://x")
    monkeypatch.setattr(mod, "fetch_candidate_procurements", lambda db_url, already_processed, limit, reprocess_missing_works=False: [
        {"reference_number": "26SYMV003", "organization_value": "ΠΕΡΙΦΕΡΕΙΑ ΔΟΚΙΜΗΣ", "title": "C"},
    ])
    monkeypatch.setattr(mod, "process_reference", lambda reference_number, db_url, debug: (_ for _ in ()).throw(RuntimeError("boom")))

    with pytest.raises(SystemExit):
        mod.main()

    assert "error" in log_file.read_text(encoding="utf-8")


def test_main_processes_explicit_reference_numbers_without_candidate_query(tmp_path: Path, monkeypatch):
    state_file = tmp_path / "state.json"
    log_file = tmp_path / "log.csv"
    state_file.write_text(json.dumps({"processed_reference_numbers": ["26SYMV001"]}), encoding="utf-8")

    monkeypatch.setattr(mod, "parse_args", lambda: SimpleNamespace(
        db_path=None,
        state_file=state_file,
        log_csv=log_file,
        limit=None,
        reference_number=["26symv001", "26SYMV002, 26SYMV003"],
        reprocess_missing_works=False,
        debug=False,
    ))
    monkeypatch.setattr(mod, "resolve_database_url", lambda db_path: "postgresql://x")
    monkeypatch.setattr(mod, "fetch_candidate_procurements", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not fetch candidates")))

    processed: list[str] = []

    def fake_process(reference_number: str, db_url: str, debug: bool) -> int:
        processed.append(reference_number)
        return 1

    monkeypatch.setattr(mod, "process_reference", fake_process)

    mod.main()

    assert processed == ["26SYMV001", "26SYMV002", "26SYMV003"]
