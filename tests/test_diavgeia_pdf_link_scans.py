"""Regression coverage for PDF scan persistence and cache invalidation.

All database checks use an in-memory transaction double; these tests never
connect to PostgreSQL or modify the live database.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib

import pytest

import scripts.migrate_diavgeia_pdf_link_scans as migration
import scripts.sync_diavgeia_contract_pdf_links as sync


ADA = "Ψ94Σ4653Π8-ΦΚΕ"
ADAM = "26SYMV018682054"


@pytest.fixture
def contract_document():
    return {
        "id": 20,
        "source_type": "contract",
        "source_id": ADAM,
        "document_url": "https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment/" + ADAM,
        "subject": "Σύμβαση πυροπροστασίας",
        "status": "False",
    }


@pytest.fixture
def successful_scan(contract_document):
    return {
        "source_type": "contract",
        "source_id": ADAM,
        "status": "read",
        "extractor_version": sync.EXTRACTOR_VERSION,
        "fingerprint": sync.fingerprint(contract_document),
        "checked_at": "2026-10-13T12:00:00+00:00",
        "pdf_sha256": "a" * 64,
        "references": [{"reference": ADA, "page": 2, "evidence": f"ΑΔΑ: {ADA}"}],
    }


def test_extractor_version_invalidates_legacy_unversioned_results():
    assert sync.EXTRACTOR_VERSION == 2


@pytest.mark.parametrize(
    "text",
    [
        "Ημερομηνίες 2025-04, 2022-2024, 01-01 και 31-12.",
        "E-MAIL: example@example.test. ISO-9001.",
        "ΔΑΣΙΚΕΣ-ΕΚΤΑΣΕΙΣ και ΔΗΜΟΣ-ΠΕΡΙΦΕΡΕΙΑ.",
        "Αριθμός 1234-567 και σειρά 1234567-890.",
    ],
)
def test_contract_extractor_excludes_known_false_ada_tokens(text):
    assert sync.extract_references(text, "contract") == []


def test_contract_extractor_keeps_actual_ada_shapes_and_unique_page_evidence():
    text = (
        "ΑΔΑ: Ω123-ΑΒΓ και ΑΒΓΔΕΖΗΘΙΚΛΜ-123. "
        f"ΑΔΑ: {ADA}\fΕπανάληψη: {ADA} και ΨΑΒΓΩΚΓ-ΑΒΓ."
    )
    references = sync.extract_references(text, "contract")
    assert [(r["reference"], r["page"]) for r in references] == [
        ("Ω123-ΑΒΓ", 1),
        ("ΑΒΓΔΕΖΗΘΙΚΛΜ-123", 1),
        (ADA, 1),
        ("ΨΑΒΓΩΚΓ-ΑΒΓ", 2),
    ]
    assert all(r["evidence"] for r in references)


@pytest.mark.parametrize("wrapped", ["Ψ94Σ4653Π8-\nΦΚΕ", "Ψ94Σ4653Π8 \n - ΦΚΕ"])
def test_contract_extractor_accepts_hyphen_wrapped_across_pdf_lines(wrapped):
    refs = sync.extract_references(f"ΑΔΑ: {wrapped}", "contract")
    assert [r["reference"] for r in refs] == [ADA]


@pytest.mark.parametrize("text,expected", [
    ("ΑΔΑ:6ΦΗ3Ω68-3ΕΞ-\n και", "6ΦΗ3Ω68-3ΕΞ"),
    ("ΑΔΑ: ΨΣ5Λ46ΝΚΟΤ-ΤΟΥ- ΑΔΑΜ:", "ΨΣ5Λ46ΝΚΟΤ-ΤΟΥ"),
])
def test_extractor_keeps_valid_ada_followed_by_pdf_hyphen_punctuation(text, expected):
    refs = sync.extract_references(text, "contract")
    assert [r["reference"] for r in refs] == [expected]


def test_extractor_rejects_ada_prefix_of_full_hyphenated_longer_identifier():
    assert sync.extract_references(f"{ADA}-EXTRA", "contract") == []


@pytest.mark.parametrize("token", ["Ω12-ΑΒΓ", "ΑΒΓΔΕΖΗΘΙΚΛΜΝ-ΑΒΓ", "Ψ94Σ4653Π8-ΦΚΕ1"])
def test_extractor_never_matches_a_valid_prefix_inside_a_longer_identifier(token):
    assert sync.extract_references(token, "contract") == []


def test_versioned_successful_negative_scan_survives_restart(contract_document, successful_scan):
    previous = {**successful_scan, "references": []}
    assert not sync.needs_pdf_read(contract_document, previous)
    assert sync.needs_pdf_read(contract_document, previous, refresh=True)


@pytest.mark.parametrize("change", [{"subject": "Νέα σύμβαση"}, {"document_url": "https://example.test/revised.pdf"}, {"status": "True"}])
def test_changed_source_metadata_requires_new_pdf_read(contract_document, successful_scan, change):
    assert sync.needs_pdf_read({**contract_document, **change}, successful_scan)


@pytest.mark.parametrize("version", [None, 1, 3])
def test_extractor_version_change_requires_new_read(contract_document, successful_scan, version):
    previous = dict(successful_scan)
    if version is None:
        previous.pop("extractor_version")
    else:
        previous["extractor_version"] = version
    assert sync.needs_pdf_read(contract_document, previous)


def test_failed_attempt_is_not_a_successful_negative(contract_document, successful_scan):
    assert sync.needs_pdf_read(contract_document, {**successful_scan, "status": "error"})


def test_public_scan_payload_keeps_successful_evidence_and_omits_diagnostics(successful_scan):
    record = {**successful_scan, "error": "private downloader traceback", "internal_url": "private"}
    payload = sync.scan_payload(record)
    assert set(payload) == {
        "fingerprint", "extractor_version", "checked_at", "pdf_sha256", "references"
    }
    assert payload["references"] == successful_scan["references"]
    assert record["error"] == "private downloader traceback"


def test_only_current_successful_cache_is_used_for_matching(contract_document, successful_scan):
    another = {**contract_document, "source_id": "26SYMV018682055"}
    documents = [contract_document, another]
    records = {
        f"contract:{ADAM}": successful_scan,
        "contract:26SYMV018682055": {
            **successful_scan,
            "source_id": "26SYMV018682055",
            "fingerprint": "outdated source fingerprint",
        },
        "contract:26SYMV018682056": {**successful_scan, "source_id": "26SYMV018682056"},
    }
    assert sync.valid_cached_records(records, documents) == {f"contract:{ADAM}": successful_scan}


@pytest.mark.parametrize("change", [{"extractor_version": 1}, {"status": "error"}])
def test_cached_only_matching_cannot_use_obsolete_or_failed_results(contract_document, successful_scan, change):
    records = {f"contract:{ADAM}": {**successful_scan, **change}}
    assert sync.valid_cached_records(records, [contract_document]) == {}


def test_unresolved_reference_can_match_later_import_without_reread(contract_document, successful_scan):
    records = sync.valid_cached_records({f"contract:{ADAM}": successful_scan}, [contract_document])
    contracts = {ADAM: {"id": 20}}
    assert sync.match_links(records, contracts, {}) == []
    links = sync.match_links(records, contracts, {ADA: {"id": 10}})
    assert {(link["diavgeia_id"], link["procurement_id"]) for link in links} == {(10, 20)}
    assert not sync.needs_pdf_read(contract_document, successful_scan)


class TransactionConnection:
    """Model pending writes so rollback tests assert persisted data, not counters."""

    def __init__(self, source_row, *, previous_scan=None, fail_update=False):
        self.source_row = deepcopy(source_row)
        self.scan = deepcopy(previous_scan)
        self.pairs = set()
        self.pending_scan = deepcopy(previous_scan)
        self.pending_pairs = set()
        self.fail_update = fail_update
        self.events = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, **kwargs):
        return TransactionCursor(self)

    def commit(self):
        self.scan = deepcopy(self.pending_scan)
        self.pairs = set(self.pending_pairs)
        self.commits += 1
        self.events.append("commit")

    def rollback(self):
        self.pending_scan = deepcopy(self.scan)
        self.pending_pairs = set(self.pairs)
        self.rollbacks += 1
        self.events.append("rollback")


class TransactionCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rowcount = 0
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        statement = " ".join(sql.upper().split())
        if statement.startswith("SELECT") and "FOR UPDATE" in statement:
            self.connection.events.append("lock source")
            self.result = deepcopy(self.connection.source_row)
            self.rowcount = int(self.result is not None)
        elif statement.startswith("UPDATE") and "PDF_LINK_SCAN" in statement:
            self.connection.events.append("update scan")
            if self.connection.fail_update:
                raise RuntimeError("scan write failed")
            payload = next(param.adapted for param in params if hasattr(param, "adapted"))
            self.connection.pending_scan = deepcopy(payload)
            self.rowcount = int(self.connection.source_row is not None)
        elif statement.startswith("SELECT COUNT(*) FROM UNNEST"):
            self.connection.events.append("verify links")
            self.result = (0,)
        else:
            raise AssertionError(f"Unexpected database operation in transaction test: {statement}")

    def fetchone(self):
        return self.result

    def fetchall(self):
        return [self.result] if self.result is not None else []


def install_link_insert(monkeypatch, *, fail=False):
    def insert(cursor, sql, rows, **kwargs):
        assert "INSERT INTO PUBLIC.DIAVGEIA_PROCUREMENT" in " ".join(sql.upper().split())
        cursor.connection.events.append("insert links")
        if fail:
            raise RuntimeError("link write failed")
        rows = {tuple(row) for row in rows}
        added = rows - cursor.connection.pending_pairs
        cursor.connection.pending_pairs.update(rows)
        return list(added)

    monkeypatch.setattr(sync, "execute_values", insert)


@pytest.fixture
def contract_source_row(contract_document):
    return {
        "id": contract_document["id"],
        "reference_number": contract_document["source_id"],
        "title": contract_document["subject"],
        "cancelled": False,
    }


def test_scan_and_links_commit_together_after_source_guard(
    monkeypatch, contract_document, successful_scan, contract_source_row
):
    connection = TransactionConnection(contract_source_row)
    install_link_insert(monkeypatch)
    added = sync.persist_scan_and_links(
        connection, contract_document, successful_scan, {ADAM: {"id": 20}}, {ADA: {"id": 10}}
    )
    assert added == {(10, 20)}
    assert connection.scan == sync.scan_payload(successful_scan)
    assert connection.pairs == {(10, 20)}
    assert connection.events == ["lock source", "update scan", "insert links", "commit"]
    assert connection.commits == 1


def test_successful_negative_scan_is_durable_even_without_links(
    monkeypatch, contract_document, successful_scan, contract_source_row
):
    connection = TransactionConnection(contract_source_row)
    install_link_insert(monkeypatch)
    negative = {**successful_scan, "references": []}
    assert sync.persist_scan_and_links(connection, contract_document, negative, {ADAM: {"id": 20}}, {}) == set()
    assert connection.scan == sync.scan_payload(negative)
    assert connection.pairs == set()
    assert connection.events == ["lock source", "update scan", "commit"]
    assert not sync.needs_pdf_read(contract_document, {**connection.scan, "status": "read"})


def test_repeated_scan_does_not_duplicate_existing_bridge_pairs(
    monkeypatch, contract_document, successful_scan, contract_source_row
):
    connection = TransactionConnection(contract_source_row)
    install_link_insert(monkeypatch)
    arguments = (connection, contract_document, successful_scan, {ADAM: {"id": 20}}, {ADA: {"id": 10}})
    assert sync.persist_scan_and_links(*arguments) == {(10, 20)}
    assert sync.persist_scan_and_links(*arguments) == set()
    assert connection.pairs == {(10, 20)}


@pytest.mark.parametrize("failure", ["update", "insert"])
def test_scan_or_link_write_failure_rolls_back_both_and_preserves_previous_success(
    monkeypatch, contract_document, successful_scan, contract_source_row, failure
):
    previous = {**sync.scan_payload(successful_scan), "pdf_sha256": "b" * 64, "references": []}
    connection = TransactionConnection(contract_source_row, previous_scan=previous, fail_update=failure == "update")
    install_link_insert(monkeypatch, fail=failure == "insert")
    with pytest.raises(RuntimeError, match="write failed"):
        sync.persist_scan_and_links(
            connection, contract_document, successful_scan, {ADAM: {"id": 20}}, {ADA: {"id": 10}}
        )
    assert connection.scan == previous
    assert connection.pairs == set()
    assert connection.commits == 0
    assert connection.rollbacks == 1


@pytest.mark.parametrize("source_change", ["deleted", "title changed", "identifier changed"])
def test_deleted_or_changed_source_cannot_receive_scan_or_links(
    monkeypatch, contract_document, successful_scan, contract_source_row, source_change
):
    source = deepcopy(contract_source_row)
    if source_change == "deleted":
        source = None
    elif source_change == "title changed":
        source["title"] = "Αντικατάσταση της σύμβασης"
    else:
        source["reference_number"] = "26SYMV018682099"
    connection = TransactionConnection(source)
    install_link_insert(monkeypatch)
    with pytest.raises(sync.SourceChangedError):
        sync.persist_scan_and_links(
            connection, contract_document, successful_scan, {ADAM: {"id": 20}}, {ADA: {"id": 10}}
        )
    assert connection.scan is None
    assert connection.pairs == set()
    assert connection.commits == 0
    assert connection.rollbacks == 1
    assert "update scan" not in connection.events
    assert "insert links" not in connection.events


def test_failed_pdf_attempt_does_not_replace_previous_successful_scan(
    monkeypatch, contract_document, successful_scan, contract_source_row
):
    previous = sync.scan_payload(successful_scan)
    connection = TransactionConnection(contract_source_row, previous_scan=previous)
    install_link_insert(monkeypatch)
    failure = {**successful_scan, "status": "error", "error": "PDF temporarily unavailable"}
    with pytest.raises(ValueError, match="successful"):
        sync.persist_scan_and_links(connection, contract_document, failure, {ADAM: {"id": 20}}, {})
    assert connection.scan == previous
    assert connection.pairs == set()
    assert connection.commits == 0


@pytest.mark.parametrize("change", [{"extractor_version": 1}, {"fingerprint": "obsolete scan"}])
def test_stale_successful_record_is_rejected_before_opening_write_transaction(
    monkeypatch, contract_document, successful_scan, contract_source_row, change
):
    connection = TransactionConnection(contract_source_row)
    install_link_insert(monkeypatch)
    with pytest.raises(ValueError, match="obsolete"):
        sync.persist_scan_and_links(
            connection, contract_document, {**successful_scan, **change}, {ADAM: {"id": 20}}, {}
        )
    assert connection.events == []
    assert connection.scan is None
    assert connection.commits == 0


def test_diavgeia_scan_persists_into_decision_and_links_to_contract(monkeypatch):
    source = {
        "id": 10,
        "ada": ADA,
        "document_url": "https://example.test/decision.pdf",
        "subject": "Απόφαση για σύμβαση",
        "status": "PUBLISHED",
        "version_comment": "",
    }
    document = sync.decision_document(source)
    record = {
        "source_type": "diavgeia",
        "source_id": ADA,
        "status": "read",
        "extractor_version": sync.EXTRACTOR_VERSION,
        "fingerprint": sync.fingerprint(document),
        "checked_at": "2026-10-13T12:00:00+00:00",
        "pdf_sha256": "c" * 64,
        "references": [{"reference": ADAM, "page": 3, "evidence": f"ΑΔΑΜ: {ADAM}"}],
    }
    connection = TransactionConnection(source)
    install_link_insert(monkeypatch)
    assert sync.persist_scan_and_links(
        connection, document, record, {ADAM: {"id": 20}}, {ADA: source}
    ) == {(10, 20)}
    assert connection.scan == sync.scan_payload(record)
    assert connection.pairs == {(10, 20)}
    assert connection.commits == 1


def test_cached_matching_rechecks_source_changed_after_initial_snapshot(
    monkeypatch, contract_document, successful_scan, contract_source_row
):
    source = {**contract_source_row, "title": "Άλλαξε μετά το αρχικό SELECT", "pdf_link_scan": sync.scan_payload(successful_scan)}
    connection = TransactionConnection(source)
    install_link_insert(monkeypatch)
    records = {f"contract:{ADAM}": successful_scan}
    assert sync.valid_cached_records(records, [contract_document]) == records
    assert sync.commit_cached_scans(
        connection, records, [contract_document], {ADAM: {"id": 20}}, {ADA: {"id": 10}}
    ) == set()
    assert connection.pairs == set()
    assert connection.events == ["lock source", "rollback"]
    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_cached_matching_commits_fresh_source_and_releases_source_locks(
    monkeypatch, contract_document, successful_scan, contract_source_row
):
    source = {**contract_source_row, "pdf_link_scan": sync.scan_payload(successful_scan)}
    connection = TransactionConnection(source)
    install_link_insert(monkeypatch)
    assert sync.commit_cached_scans(
        connection, {f"contract:{ADAM}": successful_scan}, [contract_document],
        {ADAM: {"id": 20}}, {ADA: {"id": 10}}
    ) == {(10, 20)}
    assert connection.pairs == {(10, 20)}
    assert connection.events == ["lock source", "insert links", "commit", "verify links", "rollback"]
    assert connection.commits == 1
    assert connection.rollbacks == 1


def test_migration_reextracts_from_sha_bound_saved_text_and_recovers_missed_reference(
    tmp_path, contract_document, successful_scan
):
    text = tmp_path / "exports" / "saved-run" / "text" / f"{ADAM}.txt"
    text.parent.mkdir(parents=True)
    text.write_text(f"Ημερομηνία 2025-04 και E-MAIL. ΑΔΑ: {ADA}\nΑΔΑ: Ω123-ΑΒΓ")
    text.with_suffix(".sha256").write_text(successful_scan["pdf_sha256"])
    old = {
        **successful_scan,
        "extractor_version": 1,
        "references": [{"reference": "2025-04", "page": 1, "evidence": "date"}],
    }
    key = f"contract:{ADAM}"
    prepared, coverage, issues = migration.prepare_import(
        {key: old}, [contract_document], {ADAM: {"id": 20}}, {ADA: {"id": 10}},
        migration.text_index(tmp_path / "exports"), tmp_path / "output"
    )
    imported = prepared[key]["record"]
    assert imported["extractor_version"] == sync.EXTRACTOR_VERSION
    assert [r["reference"] for r in imported["references"]] == [ADA, "Ω123-ΑΒΓ"]
    assert prepared[key]["payload"] == sync.scan_payload(imported)
    assert not sync.needs_pdf_read(contract_document, imported)
    assert migration.resolved_pairs({key: imported}, {ADAM: {"id": 20}}, {ADA: {"id": 10}}) == {(10, 20)}
    assert coverage["saved_text"] == 1
    assert issues == []


def test_migration_does_not_trust_text_with_different_pdf_hash(
    monkeypatch, tmp_path, successful_scan
):
    monkeypatch.setattr(migration, "ROOT", tmp_path)
    text = tmp_path / "exports" / "saved-run" / "text" / f"{ADAM}.txt"
    text.parent.mkdir(parents=True)
    text.write_text(f"ΑΔΑ: {ADA}")
    text.with_suffix(".sha256").write_text("b" * 64)
    found, reason = migration.saved_text(
        successful_scan, migration.text_index(tmp_path / "exports"), tmp_path / "output"
    )
    assert found is None
    assert reason == "missing_local_pdf_or_matching_text"


def test_migration_local_pdf_reextraction_prepares_output_directory_without_downloads(
    monkeypatch, tmp_path, successful_scan
):
    monkeypatch.setattr(migration, "ROOT", tmp_path)
    pdf = tmp_path / "pdf" / f"{ADAM}.pdf"
    pdf.parent.mkdir()
    pdf.write_bytes(b"%PDF locally retained regression fixture")
    record = {**successful_scan, "pdf_sha256": hashlib.sha256(pdf.read_bytes()).hexdigest()}

    def collect(row, output, *, extract_amount, download_pdf):
        assert row["ada"] == ADAM
        assert extract_amount is False
        assert (output / "text").is_dir()
        with pytest.raises(RuntimeError, match="never downloads"):
            download_pdf("https://example.test/unused.pdf", pdf)
        text = output / "text" / f"{ADAM}.txt"
        text.write_text(f"ΑΔΑ: {ADA}")
        return {"status": "read", "pdf_sha256": record["pdf_sha256"], "text_path": str(text)}

    monkeypatch.setattr(migration.sync, "collect", collect)
    text, method = migration.saved_text(record, {}, tmp_path / "output")
    assert text == f"ΑΔΑ: {ADA}"
    assert method == "local_pdf"


def test_migration_without_saved_text_filters_noise_but_keeps_legacy_version_pending(
    monkeypatch, tmp_path, contract_document, successful_scan
):
    monkeypatch.setattr(migration, "saved_text", lambda *args: (None, "missing_local_pdf_or_matching_text"))
    old = {
        **successful_scan,
        "extractor_version": 1,
        "references": successful_scan["references"] + [
            {"reference": "2025-04", "page": 1, "evidence": "date"},
            {"reference": "E-MAIL", "page": 1, "evidence": "email"},
            {"reference": "Ω123-ΑΒΓ", "page": 2, "evidence": "unresolved genuine candidate"},
        ],
    }
    key = f"contract:{ADAM}"
    prepared, coverage, issues = migration.prepare_import(
        {key: old}, [contract_document], {ADAM: {"id": 20}}, {ADA: {"id": 10}}, {}, tmp_path
    )
    imported = prepared[key]["record"]
    assert imported["extractor_version"] == 1
    assert [r["reference"] for r in imported["references"]] == [ADA, "Ω123-ΑΒΓ"]
    assert sync.needs_pdf_read(contract_document, imported)
    assert coverage["extractor_version_1"] == 1
    assert issues == [{
        "source_type": "contract", "source_id": ADAM,
        "reason": "missing_local_pdf_or_matching_text", "extractor_version": 1,
    }]


def test_migration_preserves_legacy_source_fingerprint_when_current_metadata_changed(
    monkeypatch, tmp_path, contract_document, successful_scan
):
    monkeypatch.setattr(migration, "saved_text", lambda *args: (f"ΑΔΑ: {ADA}", "saved_text"))
    current = {**contract_document, "subject": "Νέα έκδοση της σύμβασης"}
    key = f"contract:{ADAM}"
    prepared, coverage, _ = migration.prepare_import(
        {key: successful_scan}, [current], {ADAM: {"id": 20}}, {ADA: {"id": 10}}, {}, tmp_path
    )
    imported = prepared[key]["record"]
    assert imported["fingerprint"] == successful_scan["fingerprint"]
    assert imported["fingerprint"] != sync.fingerprint(current)
    assert imported["extractor_version"] == sync.EXTRACTOR_VERSION
    assert sync.needs_pdf_read(current, imported)
    assert coverage["stale_source_fingerprint"] == 1


def test_migration_archives_missing_sources_and_failures_without_importing_them(
    monkeypatch, tmp_path, contract_document, successful_scan
):
    def unexpected_text_read(*args):
        raise AssertionError("Missing sources and failed scans must not be reextracted")

    monkeypatch.setattr(migration, "saved_text", unexpected_text_read)
    missing = {**successful_scan, "source_id": "26SYMV018682099"}
    failed = {**successful_scan, "status": "error", "error": "unavailable PDF"}
    records = {"contract:26SYMV018682099": missing, f"contract:{ADAM}": failed}
    prepared, coverage, issues = migration.prepare_import(
        records, [contract_document], {ADAM: {"id": 20}}, {}, {}, tmp_path
    )
    assert prepared == {}
    assert coverage["archived_missing_contract"] == 1
    assert coverage["archived_unsuccessful_contract"] == 1
    assert issues == []
    assert records[f"contract:{ADAM}"]["error"] == "unavailable PDF"


@pytest.mark.parametrize("change,field", [
    ({"fingerprint": "invalid"}, "fingerprint"),
    ({"pdf_sha256": None}, "pdf_sha256"),
    ({"checked_at": "2026-10-13T12:00:00"}, "checked_at"),
    ({"references": [{"reference": ADA, "page": True, "evidence": "text"}]}, "references"),
])
def test_migration_archives_invalid_scan_fields_before_reextraction(
    monkeypatch, tmp_path, contract_document, successful_scan, change, field
):
    def unexpected_text_read(*args):
        raise AssertionError("Invalid legacy rows must remain in backup and await a new scan")

    monkeypatch.setattr(migration, "saved_text", unexpected_text_read)
    invalid = {**successful_scan, **change}
    assert migration.invalid_scan_fields(invalid) == [field]
    prepared, coverage, issues = migration.prepare_import(
        {f"contract:{ADAM}": invalid}, [contract_document], {ADAM: {"id": 20}}, {}, {}, tmp_path
    )
    assert prepared == {}
    assert coverage["archived_invalid_contract"] == 1
    assert issues == [{
        "source_type": "contract", "source_id": ADAM,
        "reason": "invalid_legacy_fields", "invalid_fields": [field],
    }]


def test_migration_rejects_inconsistent_cache_identity(tmp_path, contract_document, successful_scan):
    with pytest.raises(ValueError, match="Invalid cache identity"):
        migration.prepare_import(
            {"contract:wrong-source": successful_scan}, [contract_document], {}, {}, {}, tmp_path
        )


def test_migration_cannot_overwrite_existing_successful_scan(contract_document, successful_scan):
    prepared = {f"contract:{ADAM}": {"document": contract_document, "payload": sync.scan_payload(successful_scan)}}
    snapshot = {
        "diavgeia": [],
        "procurement": [{"id": 20, "reference_number": ADAM, "pdf_link_scan": sync.scan_payload(successful_scan)}],
    }
    with pytest.raises(RuntimeError, match="Refusing to overwrite"):
        migration.require_empty_targets(prepared, snapshot)


@pytest.mark.parametrize("source", [None, {"id": 20, "reference_number": "different source", "pdf_link_scan": None}])
def test_migration_rejects_deleted_or_replaced_target_rows(contract_document, source):
    prepared = {f"contract:{ADAM}": {"document": contract_document}}
    snapshot = {"diavgeia": [], "procurement": [] if source is None else [source]}
    with pytest.raises(RuntimeError, match="Source identity changed"):
        migration.require_empty_targets(prepared, snapshot)
