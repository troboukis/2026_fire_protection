#!/usr/bin/env python3
"""Collect fresh, unfiltered API candidates into a separate review directory.

This script never invokes the production CSV/state writers or connects to a
database. Raw API pages are retained before any exclusion is evaluated. A run
with a failed request, a page limit, or inconsistent pagination exits with 2 and
records the incomplete coverage in summary.json.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import time
from collections import Counter
from datetime import date, datetime, time as day_time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
ATHENS = ZoneInfo("Europe/Athens")
FIELDS = (
    "source", "id", "title", "details", "organization", "submission_date",
    "decision_type", "cpvs", "excluded", "matched_terms", "non_keyword_reason",
    "document_url", "raw_candidate_index",
)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def api_request(
    session: requests.Session, method: str, url: str, *, coverage: dict[str, Any],
    timeout: float, attempts: int, wait: float, **kwargs: Any,
) -> dict[str, Any]:
    """Retry only transport errors, rate limits and server errors."""
    last_error = "request failed"
    for attempt in range(1, attempts + 1):
        if wait:
            time.sleep(wait)
        coverage["requests"] += 1
        try:
            response = session.request(method, url, timeout=timeout, **kwargs)
            if response.status_code == 429 or response.status_code >= 500:
                last_error = f"HTTP {response.status_code}"
                retry_after = response.headers.get("Retry-After", "")
                delay = min(float(retry_after), 120) if retry_after.isdigit() else 2 ** attempt
            else:
                response.raise_for_status()
                result = response.json()
                if not isinstance(result, dict):
                    raise ValueError("API returned a non-object JSON payload")
                return result
        except (requests.Timeout, requests.ConnectionError) as exc:
            # Avoid recording exception strings that can include request URLs.
            last_error = type(exc).__name__
            delay = 2 ** attempt
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            raise RuntimeError(f"HTTP {status}") from None
        if attempt < attempts:
            print(f"  {last_error}; retry {attempt + 1}/{attempts} in {delay}s", flush=True)
            time.sleep(delay)
    raise RuntimeError(f"{last_error} after {attempts} attempts")


def validate_kimdis_page_counts(pages: list[dict], cpv_keys: list[str]) -> dict[str, Any]:
    """Validate complete pagination against contract or CPV-occurrence totals.

    The public API can page over matching CPV entries while returning each
    contract only once per page. A contract can also cross a page boundary.
    Count CPV occurrences after deduplicating contracts; never accept merely
    short pages as proof of this behavior.
    """
    from src.fetch_kimdis_procurements import business_key_from_api_item

    if not pages:
        raise ValueError("no KIMDIS pages were received")
    first_total_pages = int(pages[0]["totalPages"])
    expected_pages = max(first_total_pages, 1)
    if len(pages) != expected_pages:
        raise ValueError("KIMDIS received page count differs from first reported totalPages")
    for index, payload in enumerate(pages):
        if "number" in payload and int(payload["number"]) != index:
            raise ValueError("KIMDIS page numbers contain a gap or duplicate")
        if int(payload["totalPages"]) != first_total_pages:
            raise ValueError("KIMDIS reported totalPages changed during collection")
        if "numberOfElements" in payload and int(payload["numberOfElements"]) != len(payload["content"]):
            raise ValueError("KIMDIS content count differs from numberOfElements")
    unique = {
        business_key_from_api_item(item): item
        for payload in pages for item in payload["content"]
    }
    requested_cpvs = set(cpv_keys)
    matching_cpv_occurrences = sum(
        (cpv or {}).get("key") in requested_cpvs
        for item in unique.values()
        for obj in item.get("objectDetailsList") or []
        for cpv in (obj or {}).get("cpvs") or []
    )
    totals = [payload.get("totalElements") for payload in pages]
    result: dict[str, Any] = {
        "first_reported_total": totals[0], "last_reported_total": totals[-1],
        "reported_totals_by_page": totals, "received_pages": len(pages),
        "received_rows": sum(len(payload["content"]) for payload in pages),
        "unique_contracts": len(unique),
        "matching_cpv_occurrences": matching_cpv_occurrences,
    }
    if totals[0] is None:
        result["count_basis"] = "reported_pages_only_no_totalElements"
        return result
    expected_total = int(totals[0])
    if any(total is None or int(total) != expected_total for total in totals[:-1]):
        raise ValueError("KIMDIS reported totalElements changed before the final page")
    if totals[-1] is None:
        raise ValueError("KIMDIS final page lacks totalElements")
    if int(totals[-1]) != expected_total:
        final = pages[-1]
        final_offset = (final.get("pageable") or {}).get("offset")
        if final_offset is None or int(totals[-1]) != int(final_offset) + len(final["content"]):
            raise ValueError("KIMDIS final totalElements change is inconsistent with page offset and content")
        result["final_total_adjustment"] = "last page offset plus returned contract count"
    if len(unique) == expected_total:
        result["count_basis"] = "unique_contracts"
    elif matching_cpv_occurrences == expected_total:
        result["count_basis"] = "matching_cpv_occurrences"
        result["count_basis_inference"] = (
            "The first API total exactly matches requested CPV occurrences in unique "
            "returned contracts. CPV-join counting is inferred from returned data; "
            "the API implementation has not been inspected."
        )
    else:
        raise ValueError(
            "KIMDIS received unique contract count and matching CPV occurrence count "
            "both differ from first reported totalElements"
        )
    return result


def collect_kimdis(args: argparse.Namespace, output: Path) -> tuple[list[dict], dict]:
    from src import fetch_kimdis_procurements as kimdis

    collector = kimdis.ProcurementCollector(
        cpvs=kimdis.FETCH_CPVS, start_date=args.start_date, end_date=args.end_date,
        max_window_days=args.window_days,
        title_include_keywords=kimdis.SECONDARY_TITLE_KEYWORDS,
    )
    coverage: dict[str, Any] = {
        "complete": True, "requests": 0, "pages": 0, "queries": [],
        "cpvs": list(collector.cpvs), "errors": [],
    }
    raw: list[dict] = []
    with requests.Session() as session:
        for window_index, (start, end) in enumerate(collector.iter_windows()):
            query = {"start": start.isoformat(), "end": end.isoformat(), "pages": 0, "received_rows": 0}
            coverage["queries"].append(query)
            page = 0
            page_payloads: list[dict] = []
            try:
                while True:
                    if args.max_pages is not None and page >= args.max_pages:
                        raise RuntimeError(f"page limit {args.max_pages} reached before completion")
                    payload = api_request(
                        session, "POST", f"{collector.BASE}?page={page}", coverage=coverage,
                        timeout=args.timeout, attempts=args.attempts, wait=args.request_wait,
                        headers=collector.HEADERS,
                        json={"cpvItems": collector.cpvs, "dateFrom": start.isoformat(), "dateTo": end.isoformat()},
                    )
                    write_json(output / "pages" / f"kimdis-{window_index:03d}-{page:05d}.json", payload)
                    if "content" not in payload or "totalPages" not in payload:
                        raise ValueError("KIMDIS response lacks content or totalPages")
                    batch = payload["content"]
                    if not isinstance(batch, list) or any(not isinstance(row, dict) for row in batch):
                        raise ValueError("KIMDIS content is not a list of objects")
                    total_pages = int(payload["totalPages"])
                    page_payloads.append(payload)
                    raw.extend(batch)
                    write_json(output / "kimdis_raw.json", raw)
                    coverage["pages"] += 1
                    query["pages"] += 1
                    query["received_rows"] += len(batch)
                    query["reported_pages"] = total_pages
                    query["reported_total"] = payload.get("totalElements")
                    query.setdefault("first_reported_total", payload.get("totalElements"))
                    query["last_reported_total"] = payload.get("totalElements")
                    print(f"KIMDIS {start}..{end}: page {page + 1}/{total_pages}; {len(batch)} rows", flush=True)
                    if page >= total_pages - 1:
                        query["count_validation"] = validate_kimdis_page_counts(page_payloads, collector.cpvs)
                        break
                    if not batch:
                        raise ValueError("empty KIMDIS page before the reported final page")
                    page += 1
            except (requests.RequestException, RuntimeError, ValueError, TypeError, KeyError) as exc:
                coverage["complete"] = False
                message = f"{type(exc).__name__}: {exc}" if not isinstance(exc, requests.RequestException) else type(exc).__name__
                query["error"] = message
                coverage["errors"].append({"query": window_index, "page": page, "error": message})
                print(f"KIMDIS incomplete: {message}", flush=True)

    unique: dict[str, dict] = {}
    for item in raw:
        unique[kimdis.business_key_from_api_item(item)] = item
    coverage["raw_rows"] = len(raw)
    coverage["unique_candidates"] = len(unique)
    return list(unique.values()), coverage


def submission_date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def collect_diavgeia(args: argparse.Namespace, output: Path) -> tuple[list[dict], dict]:
    from src import fetch_diavgeia as diavgeia

    coverage: dict[str, Any] = {
        "complete": True, "requests": 0, "pages": 0, "queries": [], "errors": [],
        "out_of_range": 0, "unparsed_submission_dates": 0,
    }
    raw: list[dict] = []
    since = datetime.combine(args.start_date, day_time.min)
    until = datetime.combine(args.end_date, day_time.max)
    with requests.Session() as session:
        for batch_index, keywords in enumerate(diavgeia.chunked_keywords(diavgeia.KEYWORDS)):
            query: dict[str, Any] = {"keywords": keywords, "pages": 0, "received_rows": 0}
            coverage["queries"].append(query)
            page = 0
            try:
                while True:
                    if args.max_pages is not None and page >= args.max_pages:
                        raise RuntimeError(f"page limit {args.max_pages} reached before completion")
                    params = diavgeia.build_search_params(keywords, page, since, until)
                    payload = api_request(
                        session, "GET", diavgeia.SEARCH_URL, coverage=coverage,
                        timeout=args.timeout, attempts=args.attempts, wait=args.request_wait,
                        params=params, headers={"Accept": "application/json"},
                    )
                    write_json(output / "pages" / f"diavgeia-{batch_index:03d}-{page:05d}.json", payload)
                    total = int(payload["info"]["total"])
                    pages = (total + diavgeia.PAGE_SIZE - 1) // diavgeia.PAGE_SIZE
                    batch = payload["decisions"]
                    if not isinstance(batch, list) or any(not isinstance(row, dict) for row in batch):
                        raise ValueError("Diavgeia decisions is not a list of objects")
                    raw.extend(batch)
                    write_json(output / "diavgeia_raw.json", raw)
                    coverage["pages"] += 1
                    query["pages"] += 1
                    query["received_rows"] += len(batch)
                    query["reported_pages"] = pages
                    query["reported_total"] = total
                    print(f"Diavgeia batch {batch_index + 1}: page {page + 1}/{pages}; {len(batch)} rows", flush=True)
                    if page >= pages - 1:
                        if query["received_rows"] != total:
                            raise ValueError("Diavgeia received row count differs from reported total")
                        break
                    if not batch:
                        raise ValueError("empty Diavgeia page before the reported final page")
                    page += 1
            except (requests.RequestException, RuntimeError, ValueError, TypeError, KeyError) as exc:
                coverage["complete"] = False
                message = f"{type(exc).__name__}: {exc}" if not isinstance(exc, requests.RequestException) else type(exc).__name__
                query["error"] = message
                coverage["errors"].append({"query": batch_index, "page": page, "error": message})
                print(f"Diavgeia incomplete: {message}", flush=True)

    unique: dict[tuple, dict] = {}
    for item in raw:
        parsed = submission_date(item.get("submissionTimestamp"))
        if parsed is not None and not args.start_date <= parsed <= args.end_date:
            coverage["out_of_range"] += 1
            continue
        if parsed is None:
            coverage["unparsed_submission_dates"] += 1
        unique[diavgeia.decision_dedupe_key(item)] = item
    coverage["raw_rows"] = len(raw)
    coverage["unique_candidates"] = len(unique)
    return list(unique.values()), coverage


def review_kimdis(candidates: list[dict]) -> list[dict]:
    from src import fetch_kimdis_procurements as kimdis
    from src.keyword_exclusions import matching_exclusion_keywords

    collector = kimdis.ProcurementCollector(
        cpvs=kimdis.FETCH_CPVS, start_date=date.today(), end_date=date.today(),
        max_window_days=1, title_include_keywords=kimdis.SECONDARY_TITLE_KEYWORDS,
    )
    rows = []
    for index, item in enumerate(candidates):
        organization = (item.get("organization") or {}).get("value", "")
        details = " | ".join(str(obj.get("shortDescription") or "") for obj in item.get("objectDetailsList") or [] if isinstance(obj, dict))
        cpvs = [cpv.get("key", "") for obj in item.get("objectDetailsList") or [] for cpv in (obj or {}).get("cpvs") or []]
        terms = matching_exclusion_keywords(item.get("title"), organization, details)
        excluded = collector.is_excluded(item)
        reasons = []
        if item.get("cancelled"):
            reasons.append("cancelled")
        # Re-evaluate without keyword matching to isolate the independent title gate.
        if excluded:
            probe = kimdis.ProcurementCollector(
                cpvs=kimdis.FETCH_CPVS, start_date=date.today(), end_date=date.today(),
                max_window_days=1, exclude_keywords=["__never_matches_review_probe__"],
                title_include_keywords=kimdis.SECONDARY_TITLE_KEYWORDS,
            )
            uncancelled = {**item, "cancelled": False}
            if probe.is_excluded(uncancelled):
                reasons.append("cpv_title_gate")
        rows.append({
            "source": "kimdis", "id": item.get("referenceNumber", ""),
            "title": item.get("title", ""), "details": details,
            "organization": organization, "submission_date": item.get("submissionDate", ""),
            "decision_type": "", "cpvs": " | ".join(cpvs), "excluded": excluded,
            "matched_terms": json.dumps(list(terms), ensure_ascii=False),
            "non_keyword_reason": " | ".join(reasons),
            "document_url": item.get("documentUrl", ""), "raw_candidate_index": index,
        })
    return rows


def review_diavgeia(candidates: list[dict]) -> list[dict]:
    from src import fetch_diavgeia as diavgeia
    from src.keyword_exclusions import matching_exclusion_keywords

    rows = []
    for index, item in enumerate(candidates):
        organization = diavgeia.extract_org_label(item.get("organization"))
        organization = organization if isinstance(organization, str) else ""
        terms = matching_exclusion_keywords(item.get("subject"), organization)
        reasons = []
        if diavgeia.record_is_confirmed_irrelevant(item):
            reasons.append("TEE_planning_act")
        # Keep compatibility with a deployment that still has the explicit org list.
        has_org_filter = getattr(diavgeia, "record_has_excluded_org", lambda _: False)(item)
        if has_org_filter and not terms:
            reasons.append("organization_filter")
        decision_type = diavgeia.extract_label(item.get("decisionType"))
        rows.append({
            "source": "diavgeia", "id": item.get("ada", ""),
            "title": item.get("subject", ""), "details": "", "organization": organization,
            "submission_date": item.get("submissionTimestamp", ""),
            "decision_type": decision_type if isinstance(decision_type, str) else "",
            "cpvs": "", "excluded": bool(terms or reasons or has_org_filter),
            "matched_terms": json.dumps(list(terms), ensure_ascii=False),
            "non_keyword_reason": " | ".join(reasons),
            "document_url": item.get("documentUrl", ""), "raw_candidate_index": index,
        })
    return rows


def main() -> int:
    today = datetime.now(ATHENS).date()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("both", "kimdis", "diavgeia"), default="both")
    parser.add_argument("--start-date", type=date.fromisoformat, default=today - timedelta(days=6))
    parser.add_argument("--end-date", type=date.fromisoformat, default=today)
    parser.add_argument("--output", type=Path, default=ROOT / "exports" / f"keyword-exclusions-review-{today}")
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--window-days", type=int, default=7)
    parser.add_argument("--request-wait", type=float, default=1)
    parser.add_argument("--max-pages", type=int, help="Per query/window cap; reaching it marks the run incomplete")
    args = parser.parse_args()
    if args.end_date < args.start_date:
        parser.error("end-date must be on or after start-date")
    if args.timeout <= 0 or args.attempts < 1 or args.window_days < 1 or args.request_wait < 0:
        parser.error("timeout, attempts and window-days must be positive; request-wait must be nonnegative")
    if args.max_pages is not None and args.max_pages < 1:
        parser.error("max-pages must be positive")
    output = args.output.resolve()
    # Never replace prior evidence, even when a collection ended prematurely.
    if (output / "summary.json").exists() or (output / "pages").exists():
        parser.error(f"review output already contains a run: {output}; choose a new directory")
    output.mkdir(parents=True, exist_ok=True)
    from src.keyword_exclusions import DEFAULT_EXCLUDE_KEYWORDS

    summary: dict[str, Any] = {
        "started_at": datetime.now(ATHENS).isoformat(),
        "start_date": args.start_date.isoformat(), "end_date": args.end_date.isoformat(),
        "output": str(output), "complete": True, "sources": {},
        "policy_terms": list(DEFAULT_EXCLUDE_KEYWORDS),
        "source_sha256": {
            str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (ROOT / "src/keyword_exclusions.py", ROOT / "src/fetch_kimdis_procurements.py", ROOT / "src/fetch_diavgeia.py")
        },
        "scope": "Read-only API collection; no canonical CSV/state updates, enrichment, PDF downloads, or database access.",
    }
    write_json(output / "summary.json", summary)
    sources = ("kimdis", "diavgeia") if args.source == "both" else (args.source,)
    for source in sources:
        collect = collect_kimdis if source == "kimdis" else collect_diavgeia
        review = review_kimdis if source == "kimdis" else review_diavgeia
        candidates, coverage = collect(args, output)
        write_json(output / f"{source}_candidates.json", candidates)
        rows = review(candidates)
        kept = [row for row in rows if not row["excluded"]]
        excluded = [row for row in rows if row["excluded"]]
        write_csv(output / f"{source}_all.csv", rows)
        write_csv(output / f"{source}_kept.csv", kept)
        write_csv(output / f"{source}_excluded.csv", excluded)
        coverage.update({
            "kept": len(kept), "excluded": len(excluded),
            "matched_term_counts": dict(Counter(term for row in rows for term in json.loads(row["matched_terms"]))),
            "non_keyword_reason_counts": dict(Counter(reason for row in rows for reason in row["non_keyword_reason"].split(" | ") if reason)),
        })
        summary["sources"][source] = coverage
        summary["complete"] = summary["complete"] and coverage["complete"]
        write_json(output / "summary.json", summary)
    summary["finished_at"] = datetime.now(ATHENS).isoformat()
    write_json(output / "summary.json", summary)
    print(json.dumps({"output": str(output), "complete": summary["complete"], "sources": {source: {key: data[key] for key in ("unique_candidates", "kept", "excluded")} for source, data in summary["sources"].items()}}, ensure_ascii=False, indent=2))
    return 0 if summary["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
