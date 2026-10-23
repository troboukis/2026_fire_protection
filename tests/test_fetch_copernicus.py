from __future__ import annotations

import argparse
import gzip
import json
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest
import requests

from src import fetch_copernicus as mod


START = "2026-01-01"
END = "2026-10-22"


def fire(source_id=1, **changes):
    row = {
        "id": source_id,
        "country": "EL",
        "firedate": "2026-03-01T10:00:00Z",
        "lastupdate": "2026-03-02T00:00:00Z",
        "area_ha": "25.50",
        "shape": {
            "type": "Polygon",
            "coordinates": [[[23, 38], [23.01, 38], [23.01, 38.01], [23, 38]]],
        },
        "centroid": {"type": "Point", "coordinates": [23.005, 38.005]},
    }
    row.update(changes)
    return row


class Response:
    def __init__(self, payload=None, error=None):
        self.payload = payload
        self.error = error

    def raise_for_status(self):
        if self.error:
            raise self.error

    def json(self):
        return deepcopy(self.payload)


def serve_pages(monkeypatch, *pages):
    calls = []
    remaining = iter(pages)

    def get(url, **kwargs):
        calls.append((url, kwargs))
        response = next(remaining)
        return response if isinstance(response, Response) else Response(response)

    monkeypatch.setattr(mod.requests, "get", get)
    return calls


@pytest.mark.parametrize("full_refresh", [False, True])
def test_every_run_fetches_complete_window_without_reading_latest_timestamp(monkeypatch, full_refresh):
    def unexpected_connection(*args, **kwargs):
        pytest.fail("Choosing the fetch window must not read database timestamps")

    monkeypatch.setattr(mod.psycopg2, "connect", unexpected_connection)
    window = mod.resolve_fetch_window(START, END, None, full_refresh, 7)

    assert window["mode"] == "full_refresh"
    assert window["lastupdate_gte"] is None
    assert window["firedate_gte"] == START
    assert window["firedate_lte"] == END


def test_fetch_complete_snapshot_keeps_revisions_with_old_lastupdate(monkeypatch):
    revised = fire(1, area_ha="12.00", lastupdate="2026-01-02T00:00:00Z")
    next_url = mod.API_URL + "?offset=100"
    calls = serve_pages(
        monkeypatch,
        {"count": 2, "results": [revised], "next": next_url},
        {"count": 2, "results": [fire(2)], "next": None},
    )

    rows = mod.fetch_copernicus_rows(START, END, verbose=False)

    assert rows == [revised, fire(2)]
    assert calls[0][1]["params"]["ordering"].lstrip("-") == "id"
    assert "lastupdate__gte" not in calls[0][1]["params"]
    assert calls[1][0] == next_url
    assert calls[1][1]["params"] is None


@pytest.mark.parametrize(
    "payload",
    [
        [],
        {"results": [fire()], "next": None},
        {"count": True, "results": [fire()], "next": None},
        {"count": 1, "results": {}, "next": None},
        {"count": 1, "results": [fire()]},
        {"count": 1, "results": [fire()], "next": 123},
        {"count": 2, "results": [fire()], "next": None},
        {"count": 1, "results": [fire(), fire(2)], "next": None},
        {"count": 2, "results": [fire(), fire()], "next": None},
        {"count": 0, "results": [], "next": None},
    ],
)
def test_fetch_rejects_untrustworthy_snapshot_before_returning(monkeypatch, payload):
    serve_pages(monkeypatch, payload)
    with pytest.raises(ValueError):
        mod.fetch_copernicus_rows(START, END, verbose=False)


def test_fetch_rejects_count_changed_between_pages(monkeypatch):
    serve_pages(
        monkeypatch,
        {"count": 2, "results": [fire()], "next": mod.API_URL + "?offset=100"},
        {"count": 3, "results": [fire(2)], "next": None},
    )
    with pytest.raises(ValueError, match="count changed"):
        mod.fetch_copernicus_rows(START, END, verbose=False)


@pytest.mark.parametrize(
    "next_url",
    [
        "https://other.example/rest/2/burntareas/current/?offset=100",
        "https://api.effis.emergency.copernicus.eu/another-endpoint/",
        "https://api.effis.emergency.copernicus.eu@other.example/rest/2/burntareas/current/",
        "file:///tmp/copernicus.json",
        mod.API_URL,
    ],
)
def test_fetch_rejects_unsafe_or_repeated_next_url_without_following_it(monkeypatch, next_url):
    calls = serve_pages(monkeypatch, {"count": 2, "results": [fire()], "next": next_url})
    with pytest.raises(ValueError):
        mod.fetch_copernicus_rows(START, END, verbose=False)
    assert len(calls) == 1


def test_fetch_upgrades_service_http_next_link_to_https(monkeypatch):
    calls = serve_pages(
        monkeypatch,
        {"count": 2, "results": [fire()], "next": mod.API_URL.replace("https:", "http:") + "?offset=100"},
        {"count": 2, "results": [fire(2)], "next": None},
    )
    assert len(mod.fetch_copernicus_rows(START, END, verbose=False)) == 2
    assert calls[1][0] == mod.API_URL + "?offset=100"


def test_partial_pagination_http_failure_does_not_return_partial_snapshot(monkeypatch):
    serve_pages(
        monkeypatch,
        {"count": 2, "results": [fire()], "next": mod.API_URL + "?offset=100"},
        Response(error=requests.HTTPError("503 Service Unavailable")),
    )
    with pytest.raises(requests.HTTPError):
        mod.fetch_copernicus_rows(START, END, verbose=False)


@pytest.mark.parametrize(
    "changes",
    [
        {"id": True}, {"id": 1.5}, {"id": 0}, {"country": "IT"},
        {"firedate": None}, {"firedate": "bad-date"},
        {"firedate": "2026-03-01T10:00:00"},
        {"firedate": "2025-12-31T23:59:59Z"},
        {"firedate": "2026-10-23T00:00:00Z"},
        {"area_ha": "-0.01"}, {"area_ha": "NaN"},
        {"area_ha": "Infinity"}, {"area_ha": "not-an-area"},
        {"shape": "invalid", "centroid": "invalid"},
    ],
)
def test_validate_snapshot_rejects_records_that_cannot_safely_drive_pruning(changes):
    with pytest.raises(ValueError):
        mod.validate_snapshot_rows([fire(**changes)], START, END, "EL")


def test_validate_snapshot_uses_utc_bounds_and_keeps_end_day_fractional_seconds():
    rows = [
        fire(1, firedate="2026-01-01T02:00:00+02:00"),
        fire(2, firedate="2026-10-22T23:59:59.999999Z"),
    ]
    assert mod.validate_snapshot_rows(rows, START, END, "EL") is None
    start, end = mod.snapshot_bounds(START, END)
    assert start == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert end == datetime(2026, 10, 23, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "perimeter",
    [
        {"type": "Polygon", "coordinates": []},
        {"type": "Polygon", "coordinates": [[[23, 38], [24, 39], [23, 39], [24, 38], [23, 38]]]},
        {"type": "Point", "coordinates": [23, 38]},
    ],
)
def test_validate_snapshot_rejects_invalid_perimeter_even_with_valid_centroid(perimeter):
    with pytest.raises(ValueError, match="geometry|perimeter"):
        mod.validate_snapshot_rows([fire(shape=perimeter)], START, END, "EL")


def test_nullable_area_and_numeric_zero_keep_distinct_storage_meanings():
    assert mod.validate_snapshot_rows([fire(1, area_ha=None), fire(2, area_ha=0)], START, END, "EL") is None
    assert mod._parse_decimal(None) is None
    assert mod._parse_decimal(0) == 0.0
    assert mod._parse_decimal("0.00") == 0.0


@pytest.mark.parametrize("rows", [[], [fire(), fire()]])
def test_validate_snapshot_refuses_empty_or_duplicate_source_ids(rows):
    with pytest.raises(ValueError):
        mod.validate_snapshot_rows(rows, START, END, "EL")


@pytest.mark.parametrize("drop_stage", ["geometry", "enrichment"])
def test_main_does_not_mutate_database_when_processing_loses_a_source_id(monkeypatch, tmp_path, drop_stage):
    source = [fire(1), fire(2)]
    all_fires = pd.DataFrame(source).assign(municipality_key=None)
    one_fire = all_fires.iloc[:1].copy()
    monkeypatch.setattr(mod, "parse_args", lambda: argparse.Namespace(
        start_date=START, end_date=END, country="EL",
        geojson=tmp_path / "municipalities.geojson", output_csv=tmp_path / "snapshot.csv",
        db_path=None, full_refresh=False, lookback_days=7,
        backup_dir=tmp_path / "backups", quiet=True,
    ))
    monkeypatch.setattr(mod, "fetch_copernicus_rows", lambda *args, **kwargs: source)
    monkeypatch.setattr(mod, "rows_to_geodataframe", lambda rows: one_fire if drop_stage == "geometry" else all_fires)
    monkeypatch.setattr(mod, "load_municipalities", lambda path: object())
    monkeypatch.setattr(mod, "load_normalized_name_lookup", lambda path: {})
    monkeypatch.setattr(mod, "assign_municipalities", lambda *args: one_fire if drop_stage == "enrichment" else all_fires)

    def unexpected_write(*args, **kwargs):
        pytest.fail("A snapshot that lost a source ID must not reach the database writer")

    monkeypatch.setattr(mod, "upsert_copernicus", unexpected_write)
    with pytest.raises((ValueError, RuntimeError)):
        mod.main()


class Cursor:
    def __init__(self, results=(), failure_statement=None):
        self.executed = []
        self.results = iter(results)
        self.failure_statement = failure_statement

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def execute(self, query, params=None):
        self.executed.append((query, params))
        if self.failure_statement and query.lstrip().startswith(self.failure_statement):
            raise RuntimeError("simulated write failure")

    def executemany(self, query, params):
        self.execute(query, list(params))

    def fetchall(self):
        return next(self.results, [])

    def fetchone(self):
        return (0,)

    def close(self):
        pass


class Connection:
    def __init__(self, cursor=None):
        self.recording_cursor = cursor or Cursor()
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self.recording_cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def enriched_rows():
    return mod.rows_to_geodataframe([fire()]).assign(
        municipality_key=None, municipality_normalized_value=None,
        municipality_match_method=None, municipality_overlap_ratio=None,
        municipality_matches=[[]],
    )


def test_failed_backup_aborts_before_any_mutation_and_rolls_back(monkeypatch, tmp_path):
    connection = Connection()
    monkeypatch.setattr(mod, "resolve_database_url", lambda path: "postgresql://test")
    monkeypatch.setattr(mod.psycopg2, "connect", lambda url: connection)

    def failed_backup(*args, **kwargs):
        raise RuntimeError("backup unavailable")

    monkeypatch.setattr(mod, "backup_copernicus", failed_backup)
    with pytest.raises(RuntimeError, match="backup unavailable"):
        mod.upsert_copernicus(enriched_rows(), None, start_date=START, end_date=END, backup_dir=tmp_path)

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.closed
    assert not any(query.lstrip().split()[0] in {"INSERT", "UPDATE", "DELETE"} for query, _ in connection.recording_cursor.executed)


def install_fake_database(monkeypatch, connection, tmp_path):
    monkeypatch.setattr(mod, "resolve_database_url", lambda path: "postgresql://test")
    monkeypatch.setattr(mod.psycopg2, "connect", lambda url: connection)
    backup = tmp_path / "before.json.gz"

    def save_backup(cursor, directory, metadata):
        assert not any(query.lstrip().split()[0] in {"INSERT", "UPDATE", "DELETE"} for query, _ in cursor.executed)
        assert metadata["removed_ids"] == [99]
        backup.write_bytes(b"verified pre-change backup")
        return backup

    monkeypatch.setattr(mod, "backup_copernicus", save_backup)
    return backup


def test_reconcile_backs_up_then_updates_revisions_and_prunes_only_requested_scope(monkeypatch, tmp_path):
    cursor = Cursor(results=[[(99,)], [(99,)], [(1, Decimal("25.50"))]])
    connection = Connection(cursor)
    backup = install_fake_database(monkeypatch, connection, tmp_path)

    result = mod.upsert_copernicus(enriched_rows(), None, start_date=START, end_date=END, backup_dir=tmp_path)

    assert result["upserted_rows"] == 1
    assert result["removed_ids"] == [99]
    assert result["removed_rows"] == 1
    assert result["backup_path"] == str(backup)
    assert connection.commits == 1
    assert connection.rollbacks == 0
    assert connection.closed
    deletions = [(query, params) for query, params in cursor.executed if query.lstrip().startswith("DELETE FROM public.copernicus\n")]
    assert len(deletions) == 1
    query, params = deletions[0]
    assert "country = %s AND firedate >= %s AND firedate < %s" in query
    assert "NOT (copernicus_id = ANY(%s))" in query
    assert params == ("EL", datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 10, 23, tzinfo=timezone.utc), [1])
    insertion = next(params for query, params in cursor.executed if query.lstrip().startswith("INSERT INTO public.copernicus ("))
    assert insertion[9] == 25.5
    assert insertion[20] == "2026-03-02T00:00:00Z"


@pytest.mark.parametrize("failure_statement", ["INSERT INTO public.copernicus (", "DELETE FROM public.copernicus_municipality"])
def test_write_failure_rolls_back_and_preserves_prechange_backup(monkeypatch, tmp_path, failure_statement):
    cursor = Cursor(results=[[(99,)]], failure_statement=failure_statement)
    connection = Connection(cursor)
    backup = install_fake_database(monkeypatch, connection, tmp_path)

    with pytest.raises(RuntimeError, match="simulated write failure"):
        mod.upsert_copernicus(enriched_rows(), None, start_date=START, end_date=END, backup_dir=tmp_path)

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.closed
    assert backup.read_bytes() == b"verified pre-change backup"


@pytest.mark.parametrize(
    "returned_deleted,stored",
    [
        ([(98,)], [(1, Decimal("25.50"))]),
        ([(99,)], [(2, Decimal("25.50"))]),
        ([(99,)], [(1, Decimal("25.51"))]),
    ],
)
def test_postwrite_verification_mismatch_rolls_back(monkeypatch, tmp_path, returned_deleted, stored):
    cursor = Cursor(results=[[(99,)], returned_deleted, stored])
    connection = Connection(cursor)
    backup = install_fake_database(monkeypatch, connection, tmp_path)

    with pytest.raises(RuntimeError, match="differ"):
        mod.upsert_copernicus(enriched_rows(), None, start_date=START, end_date=END, backup_dir=tmp_path)

    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert connection.closed
    assert backup.exists()


def test_backup_contains_complete_parent_and_bridge_tables_with_exact_decimal_values(tmp_path):
    class BackupCursor:
        def execute(self, query):
            if "public.copernicus_municipality " in query:
                self.description = [("copernicus_id",), ("municipality_key",), ("overlap_area_ha",)]
                self.rows = [(1, "1234", Decimal("12.25")), (99, "5678", Decimal("0.01"))]
            else:
                self.description = [("copernicus_id",), ("area_ha",), ("shape",)]
                self.rows = [(1, Decimal("25.50"), fire()["shape"]), (99, Decimal("12095.00"), fire(99)["shape"])]

        def fetchall(self):
            return self.rows

    path = mod.backup_copernicus(BackupCursor(), tmp_path, {"removed_ids": [99]})

    with gzip.open(path, "rt", encoding="utf-8") as stream:
        backup = json.load(stream)
    assert backup["metadata"] == {"removed_ids": [99]}
    assert backup["tables"]["copernicus"] == [
        {"copernicus_id": 1, "area_ha": "25.50", "shape": fire()["shape"]},
        {"copernicus_id": 99, "area_ha": "12095.00", "shape": fire(99)["shape"]},
    ]
    assert backup["tables"]["copernicus_municipality"] == [
        {"copernicus_id": 1, "municipality_key": "1234", "overlap_area_ha": "12.25"},
        {"copernicus_id": 99, "municipality_key": "5678", "overlap_area_ha": "0.01"},
    ]
    assert not list(tmp_path.glob("*.tmp"))


def test_parse_helpers_handle_common_shapes():
    assert mod._parse_bbox("[1, 2, 3, 4]") == [1.0, 2.0, 3.0, 4.0]
    assert mod._parse_json_value('{"a": 1}') == {"a": 1}
    assert mod._parse_bool("yes") is True
    assert mod._parse_bool("0") is False
