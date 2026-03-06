#!/usr/bin/env python3
"""
Reset only procurement-related fact tables before reingest.

Truncates:
  - payment_beneficiary
  - payment
  - cpv
  - procurement
  - diavgeia_procurement
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import psycopg2


REPO_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = REPO_ROOT / ".env"


def load_database_url() -> str:
  if "DATABASE_URL" in os.environ and os.environ["DATABASE_URL"].strip():
    return os.environ["DATABASE_URL"].strip()

  if not ENV_PATH.exists():
    raise RuntimeError("Missing DATABASE_URL and .env file not found.")

  for raw in ENV_PATH.read_text(encoding="utf-8").splitlines():
    line = raw.strip()
    if not line or line.startswith("#") or "=" not in line:
      continue
    key, value = line.split("=", 1)
    if key.strip() == "DATABASE_URL" and value.strip():
      return value.strip()

  raise RuntimeError("DATABASE_URL not found in environment or .env")


def main() -> int:
  db_url = load_database_url()
  conn = psycopg2.connect(db_url)
  conn.autocommit = False
  try:
    with conn.cursor() as cur:
      cur.execute(
        """
        TRUNCATE TABLE
          public.payment_beneficiary,
          public.payment,
          public.cpv,
          public.diavgeia_procurement,
          public.procurement
        RESTART IDENTITY;
        """
      )
    conn.commit()
    print("Reset completed: procurement stack truncated.")
    return 0
  except Exception as exc:
    conn.rollback()
    print(f"Reset failed: {exc}", file=sys.stderr)
    return 1
  finally:
    conn.close()


if __name__ == "__main__":
  raise SystemExit(main())
