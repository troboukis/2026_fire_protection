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
    request_timeout: int
    retry_sleep_seconds: int


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
    ) -> None:
        self.cpvs = list((cpvs or DEFAULT_CPVS).keys())
        self.start_date = start_date
        self.end_date = end_date
        self.max_window_days = max_window_days
        self.exclude_keywords = exclude_keywords or DEFAULT_EXCLUDE_KEYWORDS
        self.request_timeout = request_timeout
        self.retry_sleep_seconds = retry_sleep_seconds
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
        max_retries = 3
        retries = 0

        while True:
            url = f"{self.BASE}?page={page}"
            payload = {
                "cpvItems": self.cpvs,
                "dateFrom": date_from.isoformat(),
                "dateTo": date_to.isoformat(),
            }
            response = requests.post(
                url,
                json=payload,
                headers=self.HEADERS,
                timeout=self.request_timeout,
            )

            if response.status_code == 429:
                retries += 1
                if retries >= max_retries:
                    raise RuntimeError(
                        f"Max retries ({max_retries}) exceeded while fetching page={page}"
                    )
                print(
                    f"429 Too Many Requests, waiting {self.retry_sleep_seconds}s "
                    f"(retry {retries}/{max_retries})"
                )
                time.sleep(self.retry_sleep_seconds)
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
) -> None:
    log_csv.parent.mkdir(parents=True, exist_ok=True)
    write_header = not log_csv.exists()
    fields = [
        "run_started_at_utc",
        "fetched_records",
        "output_rows",
        "output_csv",
        "from_backup",
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
        "success": success,
        "error": not success,
        "error_message": error_message,
    }
    with log_csv.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def parse_args() -> CollectorConfig:
    parser = argparse.ArgumentParser(description="Fetch KIMDIS procurement contracts into raw_procurements.csv")
    parser.add_argument("--start-date", default="2024-01-01", help="Fetch start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=date.today().isoformat(), help="Fetch end date (YYYY-MM-DD)")
    parser.add_argument("--max-window-days", type=int, default=180, help="Max date span per API window")
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT_CSV), help="Output CSV path")
    parser.add_argument("--backup-json", default=str(DEFAULT_BACKUP_JSON), help="Raw API backup JSON path")
    parser.add_argument("--log-csv", default=str(DEFAULT_LOG_CSV), help="Run log CSV path")
    parser.add_argument("--from-backup", action="store_true", help="Skip API and rebuild CSV from backup JSON")
    parser.add_argument("--request-timeout", type=int, default=60, help="HTTP request timeout in seconds")
    parser.add_argument("--retry-sleep-seconds", type=int, default=5, help="Wait before retry after 429")

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
        request_timeout=args.request_timeout,
        retry_sleep_seconds=args.retry_sleep_seconds,
    )


def main() -> None:
    cfg = parse_args()
    collector = ProcurementCollector(
        cpvs=DEFAULT_CPVS,
        start_date=cfg.start_date,
        end_date=cfg.end_date,
        max_window_days=cfg.max_window_days,
        exclude_keywords=DEFAULT_EXCLUDE_KEYWORDS,
        request_timeout=cfg.request_timeout,
        retry_sleep_seconds=cfg.retry_sleep_seconds,
    )

    fetched_records = 0
    output_rows = 0
    error_message = "NONE"

    try:
        if cfg.from_backup:
            collector.load_from_backup(cfg.backup_json)
        else:
            collector.fetch_all(cfg.backup_json)

        fetched_records = len(collector.items)
        df = collector.build_dataset()
        output_rows = len(df)

        cfg.output_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(cfg.output_csv, index=False)
        print(f"Saved {output_rows} rows -> {cfg.output_csv}")

        append_run_log(
            cfg.log_csv,
            fetched_records=fetched_records,
            output_rows=output_rows,
            output_csv=cfg.output_csv,
            from_backup=cfg.from_backup,
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
            success=False,
            error_message=error_message,
        )
        raise


if __name__ == "__main__":
    main()
