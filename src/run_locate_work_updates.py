from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from locate_work import Document
try:
    from src.map_copernicus_to_municipalities import ROOT, resolve_database_url
except ModuleNotFoundError:
    from map_copernicus_to_municipalities import ROOT, resolve_database_url

DEFAULT_STATE_PATH = ROOT / "state" / "locate_work_state.json"
DEFAULT_LOG_PATH = ROOT / "logs" / "locate_work_runs.csv"

ADMIE_PATTERNS = (
    "%ΑΔΜΗΕ%",
    "%ADMIE%",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run locate_work only for newly ingested target procurements.",
    )
    parser.add_argument("--db-path", default=None, help="Optional DATABASE_URL override")
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_PATH, help="JSON state file")
    parser.add_argument("--log-csv", type=Path, default=DEFAULT_LOG_PATH, help="CSV log file")
    parser.add_argument("--limit", type=int, default=None, help="Optional max references to process")
    parser.add_argument("--debug", action="store_true", help="Enable verbose per-document logging")
    return parser.parse_args()


def load_state(path: Path) -> dict:
    if not path.exists():
        return {"processed_reference_numbers": []}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"processed_reference_numbers": []}
    refs = data.get("processed_reference_numbers")
    if not isinstance(refs, list):
        refs = []
    return {"processed_reference_numbers": [str(x).strip() for x in refs if str(x).strip()]}


def save_state(path: Path, processed_reference_numbers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "processed_reference_numbers": sorted(set(processed_reference_numbers)),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def append_log_rows(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "run_started_at_utc",
        "reference_number",
        "organization_value",
        "title",
        "status",
        "inserted_rows",
        "error_message",
    ]
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        if fh.tell() == 0:
            writer.writeheader()
        writer.writerows(rows)


def fetch_candidate_procurements(db_url: str, already_processed: set[str], limit: int | None) -> list[dict]:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT ON (p.reference_number)
          p.reference_number,
          COALESCE(org.organization_value, org.organization_normalized_value, '') AS organization_value,
          COALESCE(p.title, '') AS title
        FROM public.procurement p
        LEFT JOIN public.organization org
          ON org.organization_key = p.organization_key
        LEFT JOIN public.works w
          ON w.reference_number = p.reference_number
        WHERE p.reference_number IS NOT NULL
          AND BTRIM(p.reference_number) <> ''
          AND w.reference_number IS NULL
          AND (
            EXTRACT(YEAR FROM p.contract_signed_date) = EXTRACT(YEAR FROM CURRENT_DATE)
            OR (
              p.contract_signed_date IS NULL
              AND EXTRACT(YEAR FROM p.submission_at) = EXTRACT(YEAR FROM CURRENT_DATE)
            )
          )
          AND (
            p.municipality_key IS NOT NULL
            OR (
              p.region_key IS NOT NULL
              AND COALESCE(org.authority_scope, 'other') IN ('region', 'decentralized')
            )
            OR UPPER(COALESCE(org.organization_normalized_value, org.organization_value, '')) LIKE %s
            OR UPPER(COALESCE(org.organization_normalized_value, org.organization_value, '')) LIKE %s
          )
        ORDER BY p.reference_number, p.created_at DESC, p.id DESC
        """,
        ADMIE_PATTERNS,
    )
    rows = [
        {
            "reference_number": str(reference_number).strip(),
            "organization_value": str(organization_value or "").strip(),
            "title": str(title or "").strip(),
        }
        for reference_number, organization_value, title in cur.fetchall()
        if str(reference_number or "").strip() and str(reference_number).strip() not in already_processed
    ]
    cur.close()
    conn.close()
    if limit is not None:
        return rows[:limit]
    return rows


def process_reference(reference_number: str, db_url: str, debug: bool) -> int:
    doc = Document(reference_number, db_path=db_url, debug=debug)
    doc.readDocument()
    doc.locateWork()
    doc.geolocateWork()
    return doc.ingestData()


def main() -> None:
    args = parse_args()
    db_url = resolve_database_url(args.db_path)
    state = load_state(args.state_file)
    processed_refs = set(state["processed_reference_numbers"])
    candidates = fetch_candidate_procurements(db_url, processed_refs, args.limit)

    run_started_at = datetime.now(timezone.utc).isoformat()
    log_rows: list[dict] = []
    newly_processed: list[str] = []

    if not candidates:
      print("[locate_work_updates] no new target procurements to process", flush=True)
      return

    print(f"[locate_work_updates] candidates={len(candidates)}", flush=True)

    for row in candidates:
        reference_number = row["reference_number"]
        try:
            inserted_rows = process_reference(reference_number, db_url=db_url, debug=args.debug)
            status = "success" if inserted_rows > 0 else "no_findings"
            error_message = ""
            newly_processed.append(reference_number)
            print(
                f"[locate_work_updates] reference_number={reference_number} status={status} inserted_rows={inserted_rows}",
                flush=True,
            )
        except Exception as exc:
            inserted_rows = 0
            status = "error"
            error_message = str(exc)
            print(
                f"[locate_work_updates] reference_number={reference_number} status=error error={error_message}",
                flush=True,
            )

        log_rows.append({
            "run_started_at_utc": run_started_at,
            "reference_number": reference_number,
            "organization_value": row["organization_value"],
            "title": row["title"],
            "status": status,
            "inserted_rows": inserted_rows,
            "error_message": error_message,
        })

    append_log_rows(args.log_csv, log_rows)
    if newly_processed:
        save_state(args.state_file, state["processed_reference_numbers"] + newly_processed)

    if any(row["status"] == "error" for row in log_rows):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
