from __future__ import annotations

from datetime import datetime, timezone

from src import fetch_copernicus as mod


def test_resolve_fetch_window_bootstrap_when_db_empty(monkeypatch):
    monkeypatch.setattr(mod, "get_latest_lastupdate", lambda db_path: None)

    window = mod.resolve_fetch_window(
        start_date="2024-01-01",
        end_date="2026-03-08",
        db_path=None,
        full_refresh=False,
        lookback_days=7,
    )

    assert window["mode"] == "bootstrap"
    assert window["lastupdate_gte"] is None


def test_resolve_fetch_window_incremental_uses_lookback(monkeypatch):
    latest = datetime(2026, 3, 8, 10, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(mod, "get_latest_lastupdate", lambda db_path: latest)

    window = mod.resolve_fetch_window(
        start_date="2024-01-01",
        end_date="2026-03-08",
        db_path=None,
        full_refresh=False,
        lookback_days=3,
    )

    assert window["mode"] == "incremental"
    assert window["latest_lastupdate_in_db"] == latest.isoformat()
    assert window["lastupdate_gte"].startswith("2026-03-05T10:00:00")


def test_filter_rows_by_firedate_drops_invalid_and_out_of_range():
    rows = [
        {"firedate": "2026-03-01T10:00:00Z"},
        {"firedate": "2025-12-31T23:00:00Z"},
        {"firedate": "bad-date"},
        {"firedate": ""},
    ]

    kept, dropped = mod.filter_rows_by_firedate(rows, "2026-01-01", "2026-03-31")

    assert kept == [{"firedate": "2026-03-01T10:00:00Z"}]
    assert dropped == 3


def test_parse_helpers_handle_common_shapes():
    assert mod._parse_bbox("[1, 2, 3, 4]") == [1.0, 2.0, 3.0, 4.0]
    assert mod._parse_json_value('{"a": 1}') == {"a": 1}
    assert mod._parse_bool("yes") is True
    assert mod._parse_bool("0") is False
