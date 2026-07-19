#!/usr/bin/env python3
"""Export the public FireWatch datasets as stable, analysis-ready CSV files."""

from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data" / "open_data"


EXPORTS: dict[str, str] = {
    "procurements.csv": """
        WITH cpv_agg AS (
          SELECT
            procurement_id,
            jsonb_agg(
              jsonb_build_object('code', cpv_key, 'description', cpv_value)
              ORDER BY cpv_key
            ) AS cpv_codes
          FROM public.cpv
          GROUP BY procurement_id
        ),
        works_agg AS (
          SELECT
            reference_number,
            jsonb_agg(
              jsonb_build_object(
                'id', id,
                'point_name_raw', point_name_raw,
                'point_name_canonical', point_name_canonical,
                'work', work,
                'lat', lat,
                'lon', lon,
                'page', page,
                'pages', pages,
                'excerpt', excerpt,
                'formatted_address', formatted_address,
                'place_id', place_id
              )
              ORDER BY id
            ) AS works
          FROM public.works
          GROUP BY reference_number
        )
        SELECT
          p.*,
          org.organization_value,
          org.organization_normalized_value,
          org.authority_scope AS organization_authority_scope,
          muni.municipality_value,
          muni.municipality_normalized_value,
          reg.region_value,
          reg.region_normalized_value,
          pay.diavgeia_document_type_decision_uid AS payment_document_type_uid,
          pay.beneficiaries_count AS payment_beneficiaries_count,
          pay.signers AS payment_signers,
          pay.beneficiary_name AS payment_primary_beneficiary_name,
          pay.beneficiary_vat_number AS payment_primary_beneficiary_vat_number,
          pay.amount_with_vat AS payment_amount_with_vat,
          pay.amount_without_vat AS payment_amount_without_vat,
          pay.kae_ale AS payment_kae_ale,
          pay.fiscal_year AS payment_fiscal_year,
          pay.budget_category AS payment_budget_category,
          pay.counter_party AS payment_counter_party,
          pay.payment_ref_no,
          COALESCE(ca.cpv_codes, '[]'::jsonb) AS cpv_codes_json,
          COALESCE(wa.works, '[]'::jsonb) AS works_json
        FROM public.procurement p
        LEFT JOIN public.payment pay ON pay.procurement_id = p.id
        LEFT JOIN LATERAL (
          SELECT
            o.organization_value,
            o.organization_normalized_value,
            o.authority_scope
          FROM public.organization o
          WHERE o.organization_key = p.organization_key
          ORDER BY o.updated_at DESC, o.id DESC
          LIMIT 1
        ) org ON true
        LEFT JOIN public.municipality_normalized_name muni
          ON muni.municipality_key = p.municipality_key
        LEFT JOIN public.region reg ON reg.region_key = p.region_key
        LEFT JOIN cpv_agg ca ON ca.procurement_id = p.id
        LEFT JOIN works_agg wa ON wa.reference_number = p.reference_number
        ORDER BY p.id
    """,
    "diavgeia.csv": """
        SELECT
          d.id,
          d.region_key,
          d.organization_key,
          d.municipality_key,
          d.ada,
          d.protocol_number,
          d.submission_timestamp,
          d.publish_timestamp,
          d.status,
          d.non_revokable,
          d.document_url,
          d.subject,
          d.document_type,
          d.version_comment,
          d.thematic_categories,
          d.cooperating_organizations,
          d.unit_ids,
          d.org,
          d.org_type,
          d.org_name_clean,
          d.spending_signers,
          d.spending_contractors_afm,
          d.spending_contractors_name,
          d.spending_contractors_value,
          d.diavgeia_document_type_decision_uid,
          d.created_at,
          d.updated_at,
          dt.decision_type
        FROM public.diavgeia d
        LEFT JOIN public.diavgeia_document_type dt
          ON dt.decision_uid = d.diavgeia_document_type_decision_uid
        ORDER BY d.id
    """,
    "current_fires.csv": """
        WITH notice_agg AS (
          SELECT
            current_fire_incident_key,
            jsonb_agg(
              jsonb_build_object(
                'notice_id', notice_id,
                'post_url', post_url,
                'posted_at', posted_at,
                'notice_type', notice_type,
                'notice_text', notice_text,
                'instructions_geocoded', instructions_geocoded,
                'municipality_keys', municipality_keys,
                'matched_municipality_key', matched_municipality_key
              )
              ORDER BY posted_at, notice_id
            ) AS notices
          FROM public."112_notice"
          WHERE current_fire_incident_key IS NOT NULL
          GROUP BY current_fire_incident_key
        )
        SELECT
          cf.*,
          COALESCE(na.notices, '[]'::jsonb) AS notices_112_json
        FROM public.current_fires cf
        LEFT JOIN notice_agg na
          ON na.current_fire_incident_key = cf.incident_key
        ORDER BY cf.first_seen_at, cf.incident_key
    """,
    "copernicus_fires.csv": """
        WITH municipality_region AS (
          SELECT
            municipality_key,
            min(region_key) AS region_key
          FROM public.municipality
          WHERE municipality_key IS NOT NULL
          GROUP BY municipality_key
        )
        SELECT
          c.*,
          mr.region_key,
          r.region_normalized_value
        FROM public.copernicus c
        LEFT JOIN municipality_region mr ON mr.municipality_key = c.municipality_key
        LEFT JOIN public.region r ON r.region_key = mr.region_key
        ORDER BY c.copernicus_id
    """,
}


EXPECTED_TABLES = {
    "procurements.csv": "procurement",
    "diavgeia.csv": "diavgeia",
    "current_fires.csv": "current_fires",
    "copernicus_fires.csv": "copernicus",
}


EXCLUDED_COLUMNS = {
    "procurements.csv": {"payment_id"},
}


def load_database_url() -> str:
    value = os.environ.get("DATABASE_URL", "").strip()
    if value:
        return value

    env_path = REPO_ROOT / ".env"
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, candidate = line.split("=", 1)
            if key.strip() == "DATABASE_URL" and candidate.strip():
                return candidate.strip()

    raise RuntimeError("DATABASE_URL is required")


def csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return value


def export_query(
    conn, filename: str, query: str, output_dir: Path, expected_count: int
) -> int:
    final_path = output_dir / filename
    temp_path = final_path.with_suffix(".csv.tmp")

    with conn.cursor() as cur:
        cur.execute(query)
        columns = [column.name for column in cur.description]
        included_indexes = [
            index
            for index, column in enumerate(columns)
            if column not in EXCLUDED_COLUMNS.get(filename, set())
        ]
        public_columns = [columns[index] for index in included_indexes]
        if len(public_columns) != len(set(public_columns)):
            raise RuntimeError(f"Duplicate columns in {filename}")

        row_count = 0
        with temp_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(public_columns)
            while rows := cur.fetchmany(1_000):
                for row in rows:
                    writer.writerow([csv_value(row[index]) for index in included_indexes])
                    row_count += 1

    if row_count != expected_count:
        temp_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"{filename}: exported {row_count} rows; expected {expected_count}"
        )
    temp_path.replace(final_path)
    return row_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = psycopg2.connect(load_database_url())
    conn.set_session(readonly=True, autocommit=False)
    try:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '5min'")
            expected_counts: dict[str, int] = {}
            for filename, table_name in EXPECTED_TABLES.items():
                cur.execute(f'SELECT count(*) FROM public."{table_name}"')
                expected_counts[filename] = int(cur.fetchone()[0])

        for filename, query in EXPORTS.items():
            expected = expected_counts[filename]
            row_count = export_query(conn, filename, query, output_dir, expected)
            print(f"Exported {filename}: {row_count} rows")
        conn.rollback()
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
