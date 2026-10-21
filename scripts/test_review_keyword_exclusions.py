"""Offline checks for KIMDIS contract/CPV count validation in API reviews."""

from copy import deepcopy
from datetime import date
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import review_keyword_exclusions as review

CPV = "75251100-1"


def contract(reference, matching_occurrences=1):
    return {
        "referenceNumber": reference,
        "objectDetailsList": [
            {"cpvs": [{"key": CPV}]} for _ in range(matching_occurrences)
        ],
    }


def page(number, content, *, total=4, pages=2, size=2):
    # Same Spring-style metadata shape as actual saved KIMDIS API responses.
    return {
        "content": content,
        "pageable": {
            "pageNumber": number, "pageSize": size,
            "sort": {"sorted": False, "unsorted": True, "empty": True},
            "offset": number * size, "unpaged": False, "paged": True,
        },
        "last": number == pages - 1, "totalElements": total,
        "totalPages": pages, "first": number == 0,
        "numberOfElements": len(content), "size": size, "number": number,
        "sort": {"sorted": False, "unsorted": True, "empty": True},
        "empty": not content,
    }


@pytest.fixture(autouse=True)
def prevent_network(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Review pagination tests must not contact live APIs")

    monkeypatch.setattr("requests.sessions.Session.request", forbidden)


def test_short_pages_are_accepted_only_with_exact_cpv_occurrence_evidence():
    pages = [page(0, [contract("A", 2)]), page(1, [contract("B", 2)], total=3)]
    validation = review.validate_kimdis_page_counts(pages, [CPV])

    assert validation["count_basis"] == "matching_cpv_occurrences"
    assert validation["unique_contracts"] == 2
    assert validation["matching_cpv_occurrences"] == 4
    assert validation["first_reported_total"] == 4
    assert validation["last_reported_total"] == 3
    assert validation["final_total_adjustment"] == "last page offset plus returned contract count"
    assert "inferred" in validation["count_basis_inference"]


def test_contract_crossing_page_boundary_is_not_counted_twice():
    first = contract("A", 3)
    pages = [page(0, [first]), page(1, [deepcopy(first), contract("B")])]
    validation = review.validate_kimdis_page_counts(pages, [CPV])

    assert validation["received_rows"] == 3
    assert validation["unique_contracts"] == 2
    assert validation["matching_cpv_occurrences"] == 4


def test_matching_contract_count_retains_normal_count_basis():
    pages = [page(0, [contract("A"), contract("B")]), page(1, [contract("C"), contract("D")])]
    assert review.validate_kimdis_page_counts(pages, [CPV])["count_basis"] == "unique_contracts"


def test_missing_page_is_rejected_even_when_cpv_count_matches():
    with pytest.raises(ValueError, match="page count"):
        review.validate_kimdis_page_counts([page(0, [contract("A", 4)])], [CPV])


def test_missing_rows_are_rejected_instead_of_excusing_short_pages():
    pages = [page(0, [contract("A", 2)]), page(1, [], total=2)]
    with pytest.raises(ValueError, match="both differ"):
        review.validate_kimdis_page_counts(pages, [CPV])


def test_unrelated_cpv_occurrences_do_not_reconcile_the_count():
    first = contract("A", 2)
    other = {"referenceNumber": "B", "objectDetailsList": [{"cpvs": [{"key": "unrequested"}]}]}
    pages = [page(0, [first]), page(1, [other], total=3)]
    with pytest.raises(ValueError, match="both differ"):
        review.validate_kimdis_page_counts(pages, [CPV])


@pytest.mark.parametrize("mutate, message", [
    (lambda pages: pages[1].update(number=0), "gap or duplicate"),
    (lambda pages: pages[1].update(totalPages=3), "page count|totalPages"),
    (lambda pages: pages[1].update(numberOfElements=2), "numberOfElements"),
    (lambda pages: pages[1].update(totalElements=1), "offset and content"),
])
def test_inconsistent_metadata_is_rejected(mutate, message):
    pages = [page(0, [contract("A", 2)]), page(1, [contract("B", 2)], total=3)]
    mutate(pages)
    with pytest.raises(ValueError, match=message):
        review.validate_kimdis_page_counts(pages, [CPV])


@pytest.mark.parametrize("missing_last_row, expected_complete", [(False, True), (True, False)])
def test_mocked_collection_preserves_raw_pages_and_rejects_genuine_loss(tmp_path, monkeypatch, missing_last_row, expected_complete):
    pages = [page(0, [contract("A", 2)]), page(1, [contract("B", 2)], total=3)]
    if missing_last_row:
        pages[1] = page(1, [], total=2)

    class FakeSession:
        def __init__(self):
            self.responses = iter(pages)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def request(self, *args, **kwargs):
            return SimpleNamespace(
                status_code=200, headers={}, raise_for_status=lambda: None,
                json=lambda: next(self.responses),
            )

    monkeypatch.setattr(review.requests, "Session", FakeSession)
    args = SimpleNamespace(
        start_date=date(2026, 10, 14), end_date=date(2026, 10, 20),
        window_days=7, max_pages=None, timeout=1, attempts=1, request_wait=0,
    )
    candidates, coverage = review.collect_kimdis(args, tmp_path)

    assert coverage["complete"] is expected_complete
    assert len(candidates) == (1 if missing_last_row else 2)
    assert len(list((tmp_path / "pages").glob("kimdis-*.json"))) == 2
    assert (tmp_path / "kimdis_raw.json").exists()
    if expected_complete:
        validation = coverage["queries"][0]["count_validation"]
        assert validation["count_basis"] == "matching_cpv_occurrences"
    else:
        assert "both differ" in coverage["errors"][0]["error"]
