from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import psycopg2


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from locate_work import Document
from src.map_copernicus_to_municipalities import resolve_database_url


DEFAULT_STATE_PATH = ROOT / "state" / "backfill_2026_missing_work_coordinates.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill work coordinates for contracts that have no geocoded work row.",
    )
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--debug", action="store_true")
    return parser.parse_args()


def load_state(path: Path, year: int) -> dict:
    if not path.exists():
        return {"year": year, "completed": {}, "errors": {}}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"year": year, "completed": {}, "errors": {}}
    if state.get("year") != year:
        return {"year": year, "completed": {}, "errors": {}}
    state.setdefault("completed", {})
    state.setdefault("errors", {})
    return state


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temp_path.replace(path)


def fetch_references(db_url: str, year: int) -> list[str]:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT p.reference_number
            FROM public.procurement p
            WHERE p.reference_number IS NOT NULL
              AND BTRIM(p.reference_number) <> ''
              AND (
                EXTRACT(YEAR FROM p.contract_signed_date) = %s
                OR (
                  p.contract_signed_date IS NULL
                  AND EXTRACT(YEAR FROM p.submission_at) = %s
                )
              )
              AND NOT EXISTS (
                SELECT 1
                FROM public.works w
                WHERE w.reference_number = p.reference_number
                  AND w.lat IS NOT NULL
                  AND w.lon IS NOT NULL
              )
            ORDER BY p.reference_number
            """,
            (year, year),
        )
        return [str(row[0]).strip() for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


def process_reference(reference_number: str, db_url: str, debug: bool) -> int:
    document = Document(reference_number, db_path=db_url, debug=debug)
    document.readDocument()
    document.locateWork()
    document.geolocateWork()
    return document.ingestData()


def main() -> None:
    args = parse_args()
    db_url = resolve_database_url(None)
    state = load_state(args.state_file, args.year)
    references = fetch_references(db_url, args.year)
    pending = [ref for ref in references if ref not in state["completed"]]
    if args.limit is not None:
        pending = pending[: args.limit]

    print(
        f"[backfill] year={args.year} live_missing={len(references)} "
        f"already_completed={len(state['completed'])} pending={len(pending)}",
        flush=True,
    )

    def record_result(reference_number: str, inserted_rows: int | None, error: Exception | None) -> None:
        if error is None:
            state["completed"][reference_number] = {
                "inserted_rows": inserted_rows,
                "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            state["errors"].pop(reference_number, None)
            status = "success" if inserted_rows else "no_findings"
            print(
                f"[backfill] done reference_number={reference_number} "
                f"status={status} inserted_rows={inserted_rows}",
                flush=True,
            )
        else:
            state["errors"][reference_number] = {
                "error": str(error),
                "failed_at_utc": datetime.now(timezone.utc).isoformat(),
            }
            print(
                f"[backfill] error reference_number={reference_number} error={error}",
                flush=True,
            )
        save_state(args.state_file, state)

    def run_one(index: int, reference_number: str) -> int:
        print(
            f"[backfill] start index={index}/{len(pending)} reference_number={reference_number}",
            flush=True,
        )
        return process_reference(reference_number, db_url, args.debug)

    workers = max(1, args.workers)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_reference = {
            executor.submit(run_one, index, reference_number): reference_number
            for index, reference_number in enumerate(pending, start=1)
        }
        for future in as_completed(future_to_reference):
            reference_number = future_to_reference[future]
            try:
                inserted_rows = future.result()
                record_result(reference_number, inserted_rows, None)
            except Exception as exc:
                record_result(reference_number, None, exc)

    inserted_total = sum(
        int(result.get("inserted_rows") or 0)
        for result in state["completed"].values()
    )
    print(
        f"[backfill] complete completed={len(state['completed'])} "
        f"errors={len(state['errors'])} inserted_rows={inserted_total}",
        flush=True,
    )


if __name__ == "__main__":
    main()
