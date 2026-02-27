"""
fetch_kimdis_procurements.py
----------------------------
Collect raw procurement contracts from KIMDIS OpenData and build:
  data/raw_procurements.csv

Based on the logic in src/kimdis.ipynb, with CLI options for automation.
"""

from __future__ import annotations

import argparse
import csv
import json
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests

REPO_DIR = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_CSV = REPO_DIR / "data" / "raw_procurements.csv"
DEFAULT_BACKUP_JSON = REPO_DIR / "data" / "raw_items_backup.json"
DEFAULT_LOG_CSV = REPO_DIR / "logs" / "kimdis_fetch_runs.csv"
DEFAULT_STATE_FILE = REPO_DIR / "state" / "kimdis_state.json"


DEFAULT_CPVS: dict[str, str] = {
    "75251100-1": "Υπηρεσίες καταπολέμησης πυρκαγιών",
    "75251110-4": "Υπηρεσίες πρόληψης πυρκαγιών",
    "75251120-7": "Υπηρεσίες καταπολέμησης δασοπυρκαγιών",
    "77200000-2": "Υπηρεσίες δασοκομίας",
    "77312000-0": "Υπηρεσίες εκκαθάρισης από αγριόχορτα",
    "77314000-4": "Υπηρεσίες συντήρησης οικοπέδων",
    "77340000-5": "Κλάδεμα δένδρων και θάμνων",
    "45343000-3": "Εργασίες εγκαταστάσεων πρόληψης πυρκαγιάς",
    "77314100-5": "Υπηρεσίες χορτοκάλυψης",
    "34144200-0": "Οχήματα για τις υπηρεσίες εκτάκτου ανάγκης",
    "34144212-7": "Υδροφόρες πυροσβεστικών οχημάτων",
    "35111000-5": "Πυροσβεστικός εξοπλισμός",
    "35111200-7": "Υλικά πυρόσβεσης",
}

DEFAULT_EXCLUDE_KEYWORDS = [
    "ΝΟΣΟΚΟΜΕΙΟ",
    "ΣΧΟΛΙΚ",
    "ΠΑΝΤΕΙΟ",
    "ΕΠΙΘΕΩΡΗΣΗ ΕΡΓΑΣΙΑΣ",
    "Αναγόμ",
    "αναγομ",
    "Αναγομ",
    "αναγόμ",
]


def normalize_cell_value(value):
    """Normalize nested values to stable strings for CSV + dedupe."""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


def normalize_string(value: str) -> str:
    if not isinstance(value, str):
        return value
    value = value.lower()
    value = unicodedata.normalize("NFD", value)
    value = "".join(ch for ch in value if unicodedata.category(ch) != "Mn")
    value = " ".join(value.split())
    return value


@dataclass
class CollectorConfig:
    start_date: date
    end_date: date
    max_window_days: int
    output_csv: Path
    backup_json: Path
    log_csv: Path
    from_backup: bool
    full_refresh: bool
    state_file: Path
    request_timeout: int
    retry_sleep_seconds: int
    request_wait_seconds: float


class ProcurementCollector:
    BASE = "https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract"
    HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}

    def __init__(
        self,
        cpvs: dict[str, str] | None,
        start_date: date,
        end_date: date,
        max_window_days: int,
        exclude_keywords: list[str] | None = None,
        request_timeout: int = 60,
        retry_sleep_seconds: int = 5,
        request_wait_seconds: float = 1.0,
    ) -> None:
        self.cpvs = list((cpvs or DEFAULT_CPVS).keys())
        self.start_date = start_date
        self.end_date = end_date
        self.max_window_days = max_window_days
        self.exclude_keywords = exclude_keywords or DEFAULT_EXCLUDE_KEYWORDS
        self.request_timeout = request_timeout
        self.retry_sleep_seconds = retry_sleep_seconds
        self.request_wait_seconds = request_wait_seconds
        self.items: list[dict[str, Any]] = []

    def iter_windows(self):
        current = self.start_date
        while current <= self.end_date:
            next_date = min(current + timedelta(days=self.max_window_days - 1), self.end_date)
            yield current, next_date
            current = next_date + timedelta(days=1)

    def fetch_window(self, date_from: date, date_to: date) -> list[dict[str, Any]]:
        print(f"\nWindow: {date_from} -> {date_to}")
        page = 0
        rows: list[dict[str, Any]] = []
        max_retries = 8
        retries = 0

        while True:
            # Be polite with the API and reduce rate-limit pressure.
            time.sleep(self.request_wait_seconds)
            url = f"{self.BASE}?page={page}"
            payload = {
                "cpvItems": self.cpvs,
                "dateFrom": date_from.isoformat(),
                "dateTo": date_to.isoformat(),
            }
            try:
                response = requests.post(
                    url,
                    json=payload,
                    headers=self.HEADERS,
                    timeout=self.request_timeout,
                )
            except requests.RequestException as exc:
                retries += 1
                if retries >= max_retries:
                    raise RuntimeError(
                        f"Request failed after {max_retries} retries on page={page}: {exc}"
                    ) from exc
                backoff = min(self.retry_sleep_seconds * (2 ** (retries - 1)), 120)
                print(
                    f"Request error ({type(exc).__name__}), waiting {backoff}s "
                    f"(retry {retries}/{max_retries})"
                )
                time.sleep(backoff)
                continue

            if response.status_code == 429:
                retries += 1
                if retries >= max_retries:
                    raise RuntimeError(
                        f"Max retries ({max_retries}) exceeded while fetching page={page}"
                    )
                retry_after_raw = response.headers.get("Retry-After", "").strip()
                retry_after = None
                if retry_after_raw.isdigit():
                    retry_after = int(retry_after_raw)
                backoff = min(self.retry_sleep_seconds * (2 ** (retries - 1)), 120)
                wait_seconds = retry_after if retry_after is not None else backoff
                print(
                    f"429 Too Many Requests, waiting {wait_seconds}s "
                    f"(retry {retries}/{max_retries})"
                )
                time.sleep(wait_seconds)
                continue

            if 500 <= response.status_code < 600:
                retries += 1
                if retries >= max_retries:
                    response.raise_for_status()
                backoff = min(self.retry_sleep_seconds * (2 ** (retries - 1)), 120)
                print(
                    f"{response.status_code} server error, waiting {backoff}s "
                    f"(retry {retries}/{max_retries})"
                )
                time.sleep(backoff)
                continue

            retries = 0
            response.raise_for_status()
            payload_json = response.json()
            total_pages = payload_json.get("totalPages", 1)
            rows.extend(payload_json.get("content", []))
            print(f"Page {page + 1}/{total_pages}")

            if page >= total_pages - 1:
                break
            page += 1

        print(f"Rows in window: {len(rows)}")
        return rows

    def fetch_all(self, backup_path: Path) -> list[dict[str, Any]]:
        self.items = []
        backup_path.parent.mkdir(parents=True, exist_ok=True)

        for date_from, date_to in self.iter_windows():
            self.items.extend(self.fetch_window(date_from, date_to))
            backup_path.write_text(
                json.dumps(self.items, ensure_ascii=False),
                encoding="utf-8",
            )
            print(f"Backup saved: {len(self.items)} rows -> {backup_path}")

        print(f"\nTotal fetched rows: {len(self.items)}")
        return self.items

    def load_from_backup(self, backup_path: Path) -> list[dict[str, Any]]:
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup not found: {backup_path}")
        self.items = json.loads(backup_path.read_text(encoding="utf-8"))
        print(f"Loaded {len(self.items)} rows from backup: {backup_path}")
        return self.items

    def is_excluded(self, item: dict[str, Any]) -> bool:
        org_value = normalize_string((item.get("organization") or {}).get("value", ""))
        short_descriptions = normalize_string(
            " | ".join((obj or {}).get("shortDescription", "") for obj in (item.get("objectDetailsList") or []))
        )
        normalized_keywords = [normalize_string(k) for k in self.exclude_keywords]

        return (
            bool(item.get("cancelled"))
            or any(k in org_value for k in normalized_keywords)
            or any(k in short_descriptions for k in normalized_keywords)
        )

    def parse_item(self, item: dict[str, Any]) -> dict[str, Any]:
        ada = item.get("contractRelatedADA") or {}
        ada_str = " | ".join(str(v) for v in ada.values() if v)

        nuts = item.get("nutsCodes") or []
        centralized_markets = item.get("centralizedMarkets") or []
        award_procedure = item.get("awardProcedure")
        duration_uom = item.get("contractDurationUnitOfMeasure")

        objects = item.get("objectDetailsList") or []
        short_descriptions = " | ".join((obj or {}).get("shortDescription", "") for obj in objects if obj)
        cpv_keys = " | ".join(
            (cpv or {}).get("key", "")
            for obj in objects
            for cpv in ((obj or {}).get("cpvs") or [])
        )
        cpv_values = " | ".join(
            (cpv or {}).get("value", "")
            for obj in objects
            for cpv in ((obj or {}).get("cpvs") or [])
        )
        green_contracts = " | ".join(
            ((obj or {}).get("greenContracts") or {}).get("value", "")
            for obj in objects
        )

        funding = item.get("fundingDetails") or {}
        contracting = item.get("contractingDataDetails") or {}
        units_operator = (contracting.get("unitsOperator") or {}).get("value", "")
        signers = (contracting.get("signers") or {}).get("value", "")
        members = contracting.get("contractingMembersDataList") or []
        first_member = members[0] if members else {}

        return {
            "title": item.get("title"),
            "referenceNumber": item.get("referenceNumber"),
            "submissionDate": item.get("submissionDate"),
            "contractSignedDate": item.get("contractSignedDate"),
            "startDate": item.get("startDate"),
            "noEndDate": item.get("noEndDate"),
            "endDate": item.get("endDate"),
            "cancelled": item.get("cancelled"),
            "cancellationDate": item.get("cancellationDate"),
            "cancellationType": item.get("cancellationType"),
            "cancellationReason": item.get("cancellationReason"),
            "decisionRelatedAda": item.get("decisionRelatedAda"),
            "contractNumber": item.get("contractNumber"),
            "organizationVatNumber": item.get("organizationVatNumber"),
            "greekOrganizationVatNumber": item.get("greekOrganizationVatNumber"),
            "diavgeiaADA": item.get("diavgeiaADA"),
            "budget": item.get("budget"),
            "contractBudget": item.get("contractBudget"),
            "bidsSubmitted": item.get("bidsSubmitted"),
            "maxBidsSubmitted": item.get("maxBidsSubmitted"),
            "numberOfSections": item.get("numberOfSections"),
            "centralGovernmentAuthority": (item.get("centralGovernmentAuthority") or {}).get("value"),
            "nutsCode_key": " | ".join((n.get("nutsCode", {}) or {}).get("key", "") for n in nuts),
            "nutsCode_value": " | ".join((n.get("nutsCode", {}) or {}).get("value", "") for n in nuts),
            "organization_key": (item.get("organization") or {}).get("key"),
            "organization_value": (item.get("organization") or {}).get("value"),
            "procedureType_key": (item.get("procedureType") or {}).get("key"),
            "procedureType_value": (item.get("procedureType") or {}).get("value"),
            "awardProcedure": award_procedure.get("value") if isinstance(award_procedure, dict) else award_procedure,
            "nutsCity": item.get("nutsCity"),
            "nutsPostalCode": item.get("nutsPostalCode"),
            "centralizedMarkets": " | ".join(
                (m.get("centralizedMarket", {}) or {}).get("value", "")
                for m in centralized_markets
            ),
            "contractType": (item.get("contractType") or {}).get("value"),
            "assignCriteria": (item.get("assignCriteria") or {}).get("value"),
            "classificationOfPublicLawOrganization": (
                item.get("classificationOfPublicLawOrganization") or {}
            ).get("value"),
            "typeOfContractingAuthority": (item.get("typeOfContractingAuthority") or {}).get("value"),
            "contractingAuthorityActivity": (item.get("contractingAuthorityActivity") or {}).get("value"),
            "contractDuration": item.get("contractDuration"),
            "contractDurationUnitOfMeasure": duration_uom.get("value") if isinstance(duration_uom, dict) else duration_uom,
            "contractRelatedADA": ada_str,
            "fundingDetails_cofund": funding.get("cofundProgramRef"),
            "fundingDetails_selfFund": funding.get("selfFundProgramRef"),
            "fundingDetails_espa": funding.get("espaFundProgramRef"),
            "fundingDetails_regularBudget": funding.get("regularBudgetFundedProgramRef"),
            "unitsOperator": units_operator,
            "signers": signers,
            "firstMember_vatNumber": first_member.get("vatNumber"),
            "firstMember_name": first_member.get("name"),
            "totalCostWithVAT": item.get("totalCostWithVAT"),
            "totalCostWithoutVAT": item.get("totalCostWithoutVAT"),
            "shortDescriptions": short_descriptions,
            "cpv_keys": cpv_keys,
            "cpv_values": cpv_values,
            "greenContracts": green_contracts,
            "auctionRefNo": item.get("auctionRefNo"),
            "paymentRefNo": item.get("paymentRefNo"),
        }

    def build_dataset(self) -> pd.DataFrame:
        if not self.items:
            return pd.DataFrame()
        rows = [self.parse_item(item) for item in self.items if not self.is_excluded(item)]
        rows = [{k: normalize_cell_value(v) for k, v in row.items()} for row in rows]
        df = pd.DataFrame(rows)
        if "submissionDate" in df.columns:
            dt = pd.to_datetime(df["submissionDate"], errors="coerce")
            df = df.assign(submissionDate=dt).sort_values("submissionDate").reset_index(drop=True)
            df["submissionDate"] = df["submissionDate"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str.rstrip("0").str.rstrip(".")
        print(f"Rows after filtering: {len(df)} / {len(self.items)}")
        return df


def append_run_log(
    log_csv: Path,
    *,
    fetched_records: int,
    output_rows: int,
    output_csv: Path,
    from_backup: bool,
    success: bool,
    error_message: str,
    state_file: Path,
    full_refresh: bool,
) -> None:
    log_csv.parent.mkdir(parents=True, exist_ok=True)
    write_header = not log_csv.exists()
    fields = [
        "run_started_at_utc",
        "fetched_records",
        "output_rows",
        "output_csv",
        "from_backup",
        "full_refresh",
        "state_file",
        "success",
        "error",
        "error_message",
    ]
    row = {
        "run_started_at_utc": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "fetched_records": fetched_records,
        "output_rows": output_rows,
        "output_csv": str(output_csv),
        "from_backup": from_backup,
        "full_refresh": full_refresh,
        "state_file": str(state_file),
        "success": success,
        "error": not success,
        "error_message": error_message,
    }
    with log_csv.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def load_last_fetch_from_state(state_file: Path) -> datetime | None:
    if not state_file.exists():
        return None
    data = json.loads(state_file.read_text(encoding="utf-8"))
    raw = str(data.get("last_fetch") or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def derive_last_fetch_from_csv(output_csv: Path) -> datetime | None:
    if not output_csv.exists():
        return None
    try:
        df = pd.read_csv(output_csv, usecols=["submissionDate"], dtype=str)
    except Exception:
        return None
    dt = pd.to_datetime(df["submissionDate"], errors="coerce", utc=True)
    if dt.isna().all():
        return None
    return dt.max().to_pydatetime()


def save_last_fetch_to_state(state_file: Path, last_fetch: datetime) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(
        json.dumps({"last_fetch": last_fetch.isoformat()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def merge_with_existing_csv(output_csv: Path, new_df: pd.DataFrame) -> pd.DataFrame:
    if not output_csv.exists():
        return new_df
    existing_df = pd.read_csv(output_csv, dtype=str, keep_default_na=False, na_values=[""])
    merged = pd.concat([existing_df, new_df], ignore_index=True)
    dedupe_df = merged.copy()
    for col in dedupe_df.columns:
        dedupe_df[col] = dedupe_df[col].map(normalize_cell_value)
    merged = merged.loc[~dedupe_df.duplicated()].reset_index(drop=True)
    if "submissionDate" in merged.columns:
        dt = pd.to_datetime(merged["submissionDate"], errors="coerce")
        merged = merged.assign(submissionDate=dt).sort_values("submissionDate").reset_index(drop=True)
        merged["submissionDate"] = (
            merged["submissionDate"]
            .dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
            .str.rstrip("0")
            .str.rstrip(".")
        )
    return merged


def merge_raw_items(existing_items: list[dict[str, Any]], new_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged = []
    seen: set[str] = set()
    for item in [*existing_items, *new_items]:
        key = json.dumps(item, ensure_ascii=False, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)
    return merged


def parse_args() -> CollectorConfig:
    parser = argparse.ArgumentParser(description="Fetch KIMDIS procurement contracts into raw_procurements.csv")
    parser.add_argument("--start-date", default="2024-01-01", help="Fetch start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=date.today().isoformat(), help="Fetch end date (YYYY-MM-DD)")
    parser.add_argument("--max-window-days", type=int, default=180, help="Max date span per API window")
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV), help="Output CSV path")
    parser.add_argument("--backup-json", default=str(DEFAULT_BACKUP_JSON), help="Raw API backup JSON path")
    parser.add_argument("--log-csv", default=str(DEFAULT_LOG_CSV), help="Run log CSV path")
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE), help="Incremental state file path")
    parser.add_argument("--from-backup", action="store_true", help="Skip API and rebuild CSV from backup JSON")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore incremental state and fetch from --start-date to --end-date",
    )
    parser.add_argument("--request-timeout", type=int, default=60, help="HTTP request timeout in seconds")
    parser.add_argument("--retry-sleep-seconds", type=int, default=5, help="Wait before retry after 429")
    parser.add_argument(
        "--request-wait-seconds",
        type=float,
        default=1.0,
        help="Wait between API requests (seconds)",
    )

    args = parser.parse_args()

    try:
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)
    except ValueError as exc:
        raise SystemExit(f"Invalid date format: {exc}") from exc

    if end_date < start_date:
        raise SystemExit("--end-date cannot be before --start-date")
    if args.max_window_days < 1:
        raise SystemExit("--max-window-days must be >= 1")

    return CollectorConfig(
        start_date=start_date,
        end_date=end_date,
        max_window_days=args.max_window_days,
        output_csv=Path(args.output_csv).resolve(),
        backup_json=Path(args.backup_json).resolve(),
        log_csv=Path(args.log_csv).resolve(),
        from_backup=args.from_backup,
        full_refresh=args.full_refresh,
        state_file=Path(args.state_file).resolve(),
        request_timeout=args.request_timeout,
        retry_sleep_seconds=args.retry_sleep_seconds,
        request_wait_seconds=args.request_wait_seconds,
    )


def main() -> None:
    cfg = parse_args()

    fetched_records = 0
    output_rows = 0
    error_message = "NONE"

    try:
        if cfg.from_backup:
            collector = ProcurementCollector(
                cpvs=DEFAULT_CPVS,
                start_date=cfg.start_date,
                end_date=cfg.end_date,
                max_window_days=cfg.max_window_days,
                exclude_keywords=DEFAULT_EXCLUDE_KEYWORDS,
                request_timeout=cfg.request_timeout,
                retry_sleep_seconds=cfg.retry_sleep_seconds,
                request_wait_seconds=cfg.request_wait_seconds,
            )
            collector.load_from_backup(cfg.backup_json)
        else:
            existing_backup_items: list[dict[str, Any]] = []
            if cfg.full_refresh:
                last_fetch = None
            else:
                last_fetch = load_last_fetch_from_state(cfg.state_file)
                if last_fetch is None:
                    last_fetch = derive_last_fetch_from_csv(cfg.output_csv)
                if cfg.backup_json.exists():
                    try:
                        existing_backup_items = json.loads(cfg.backup_json.read_text(encoding="utf-8"))
                    except Exception:
                        existing_backup_items = []

            effective_start = cfg.start_date
            if last_fetch is not None and last_fetch.date() > effective_start:
                effective_start = last_fetch.date()

            collector = ProcurementCollector(
                cpvs=DEFAULT_CPVS,
                start_date=effective_start,
                end_date=cfg.end_date,
                max_window_days=cfg.max_window_days,
                exclude_keywords=DEFAULT_EXCLUDE_KEYWORDS,
                request_timeout=cfg.request_timeout,
                retry_sleep_seconds=cfg.retry_sleep_seconds,
                request_wait_seconds=cfg.request_wait_seconds,
            )
            collector.fetch_all(cfg.backup_json)
            if not cfg.full_refresh:
                collector.items = merge_raw_items(existing_backup_items, collector.items)
                cfg.backup_json.write_text(
                    json.dumps(collector.items, ensure_ascii=False),
                    encoding="utf-8",
                )

        fetched_records = len(collector.items)
        new_df = collector.build_dataset()

        if cfg.from_backup or cfg.full_refresh:
            output_df = new_df
        else:
            output_df = merge_with_existing_csv(cfg.output_csv, new_df)

        output_rows = len(output_df)

        cfg.output_csv.parent.mkdir(parents=True, exist_ok=True)
        output_df.to_csv(cfg.output_csv, index=False)
        print(f"Saved {output_rows} rows -> {cfg.output_csv}")

        if not cfg.from_backup and "submissionDate" in output_df.columns and not output_df.empty:
            dt = pd.to_datetime(output_df["submissionDate"], errors="coerce", utc=True).dropna()
            if not dt.empty:
                save_last_fetch_to_state(cfg.state_file, dt.max().to_pydatetime())

        append_run_log(
            cfg.log_csv,
            fetched_records=fetched_records,
            output_rows=output_rows,
            output_csv=cfg.output_csv,
            from_backup=cfg.from_backup,
            full_refresh=cfg.full_refresh,
            state_file=cfg.state_file,
            success=True,
            error_message=error_message,
        )
    except Exception as exc:
        error_message = str(exc)
        append_run_log(
            cfg.log_csv,
            fetched_records=fetched_records,
            output_rows=output_rows,
            output_csv=cfg.output_csv,
            from_backup=cfg.from_backup,
            full_refresh=cfg.full_refresh,
            state_file=cfg.state_file,
            success=False,
            error_message=error_message,
        )
        raise


if __name__ == "__main__":
    main()
