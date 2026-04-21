from __future__ import annotations

import json

import pandas as pd

import src.fetch_kimdis_procurements as fetch_kimdis_procurements
from src.fetch_kimdis_procurements import (
    CollectorConfig,
    ProcurementCollector,
    business_key_from_api_item,
    dedupe_df_by_business_key,
    normalize_string,
    parse_kimdis_datetime,
)


def test_normalize_string_strips_accents_and_collapses_whitespace():
    assert normalize_string("  Δήμος   Αθηναίων  ") == "δημος αθηναιων"


def test_business_key_prefers_reference_number():
    item = {
        "referenceNumber": "26SYMV018574528",
        "diavgeiaADA": "SHOULD_NOT_BE_USED",
        "organization": {"key": "123"},
    }
    assert business_key_from_api_item(item) == "ref:26symv018574528"


def test_dedupe_df_keeps_latest_row_per_business_key():
    df = pd.DataFrame(
        [
            {
                "referenceNumber": "26SYMV1",
                "diavgeiaADA": "",
                "contractNumber": "",
                "organization_key": "ORG",
                "title": "older",
                "submissionDate": "2026-01-01T09:00:00",
                "contractSignedDate": "2026-01-02",
            },
            {
                "referenceNumber": "26SYMV1",
                "diavgeiaADA": "",
                "contractNumber": "",
                "organization_key": "ORG",
                "title": "newer",
                "submissionDate": "2026-01-03T09:00:00",
                "contractSignedDate": "2026-01-04",
            },
        ]
    )

    out = dedupe_df_by_business_key(df)

    assert len(out) == 1
    assert out.iloc[0]["title"] == "newer"


def test_parse_kimdis_datetime_handles_mixed_iso_date_shapes():
    values = pd.Series(["2026-01-02", "2026-01-03T09:00:00", "", None])

    out = parse_kimdis_datetime(values)

    assert str(out.iloc[0]) == "2026-01-02 00:00:00"
    assert str(out.iloc[1]) == "2026-01-03 09:00:00"
    assert pd.isna(out.iloc[2])
    assert pd.isna(out.iloc[3])


def test_is_excluded_drops_school_contracts_by_title_keyword():
    collector = ProcurementCollector(
        cpvs={},
        start_date=pd.Timestamp("2026-01-01").date(),
        end_date=pd.Timestamp("2026-01-02").date(),
        max_window_days=1,
    )

    item = {
        "title": "Συντήρηση σχολικών κτιρίων και αύλειων χώρων",
        "organization": {"value": "ΔΗΜΟΣ Χ"},
        "objectDetailsList": [],
        "cancelled": False,
    }

    assert collector.is_excluded(item) is True


def test_parse_item_preserves_all_contracting_members():
    collector = ProcurementCollector(
        cpvs={},
        start_date=pd.Timestamp("2026-01-01").date(),
        end_date=pd.Timestamp("2026-01-02").date(),
        max_window_days=1,
    )

    row = collector.parse_item(
        {
            "referenceNumber": "26SYMV018661963",
            "contractingDataDetails": {
                "contractingMembersDataList": [
                    {
                        "vatNumber": "046212303",
                        "name": "ΡΕΒΕΛΙΩΤΗΣ ΠΑΝΑΓΙΩΤΗΣ ΤΟΥ ΑΝΔΡΕΑ",
                        "country": {"key": "GR", "value": "Ελλάδα"},
                    },
                    {
                        "vatNumber": "045497240",
                        "name": "ΛΑΜΠΙΡΗΣ ΓΕΩΡΓΙΟΣ ΤΟΥ ΒΛΑΣΙΟΥ",
                        "country": {"key": "GR", "value": "Ελλάδα"},
                    },
                ]
            },
            "objectDetailsList": [],
        }
    )

    assert row["contractingMembers_count"] == 2
    assert row["contractingMembers_vatNumbers"] == ["046212303", "045497240"]
    assert row["contractingMembers_names"] == [
        "ΡΕΒΕΛΙΩΤΗΣ ΠΑΝΑΓΙΩΤΗΣ ΤΟΥ ΑΝΔΡΕΑ",
        "ΛΑΜΠΙΡΗΣ ΓΕΩΡΓΙΟΣ ΤΟΥ ΒΛΑΣΙΟΥ",
    ]
    assert row["contractingMembers_details"] == [
        {
            "vatNumber": "046212303",
            "name": "ΡΕΒΕΛΙΩΤΗΣ ΠΑΝΑΓΙΩΤΗΣ ΤΟΥ ΑΝΔΡΕΑ",
            "countryKey": "GR",
            "countryValue": "Ελλάδα",
        },
        {
            "vatNumber": "045497240",
            "name": "ΛΑΜΠΙΡΗΣ ΓΕΩΡΓΙΟΣ ΤΟΥ ΒΛΑΣΙΟΥ",
            "countryKey": "GR",
            "countryValue": "Ελλάδα",
        },
    ]
    assert row["firstMember_vatNumber"] == "046212303"
    assert row["firstMember_name"] == "ΡΕΒΕΛΙΩΤΗΣ ΠΑΝΑΓΙΩΤΗΣ ΤΟΥ ΑΝΔΡΕΑ"


def test_main_incremental_rebuild_drops_rows_now_matching_exclude_keywords(tmp_path, monkeypatch):
    output_csv = tmp_path / "raw_procurements.csv"
    backup_json = tmp_path / "raw_items_backup.json"
    log_csv = tmp_path / "kimdis_fetch_runs.csv"
    state_file = tmp_path / "kimdis_state.json"

    excluded_item = {
        "title": "Συντήρηση σχολικών κτιρίων",
        "referenceNumber": "A1",
        "submissionDate": "2026-01-01T09:00:00",
        "contractSignedDate": "2026-01-02",
        "cancelled": False,
        "organization": {"key": "ORG-1", "value": "ΔΗΜΟΣ Χ"},
        "objectDetailsList": [],
    }
    kept_item = {
        "title": "Εργασίες πυροπροστασίας",
        "referenceNumber": "A2",
        "submissionDate": "2026-01-03T09:00:00",
        "contractSignedDate": "2026-01-04",
        "cancelled": False,
        "organization": {"key": "ORG-1", "value": "ΔΗΜΟΣ Χ"},
        "objectDetailsList": [],
    }

    backup_json.write_text(json.dumps([excluded_item, kept_item], ensure_ascii=False), encoding="utf-8")
    pd.DataFrame(
        [
            {
                "title": excluded_item["title"],
                "referenceNumber": excluded_item["referenceNumber"],
                "diavgeiaADA": "",
                "contractNumber": "",
                "organization_key": "ORG-1",
                "submissionDate": excluded_item["submissionDate"],
                "contractSignedDate": excluded_item["contractSignedDate"],
            },
            {
                "title": kept_item["title"],
                "referenceNumber": kept_item["referenceNumber"],
                "diavgeiaADA": "",
                "contractNumber": "",
                "organization_key": "ORG-1",
                "submissionDate": kept_item["submissionDate"],
                "contractSignedDate": kept_item["contractSignedDate"],
            },
        ]
    ).to_csv(output_csv, index=False)

    monkeypatch.setattr(
        fetch_kimdis_procurements,
        "parse_args",
        lambda: CollectorConfig(
            start_date=pd.Timestamp("2026-01-01").date(),
            end_date=pd.Timestamp("2026-01-05").date(),
            max_window_days=180,
            output_csv=output_csv,
            backup_json=backup_json,
            log_csv=log_csv,
            from_backup=False,
            full_refresh=False,
            state_file=state_file,
            request_timeout=60,
            retry_sleep_seconds=1,
            request_wait_seconds=0.0,
        ),
    )
    monkeypatch.setattr(
        fetch_kimdis_procurements.ProcurementCollector,
        "fetch_all",
        lambda self, backup_path=None: [],
    )

    fetch_kimdis_procurements.main()

    out = pd.read_csv(output_csv, dtype=str, keep_default_na=False)

    assert out["referenceNumber"].tolist() == ["A2"]
