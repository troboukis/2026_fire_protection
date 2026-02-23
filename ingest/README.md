# Ingestion Scripts

Scripts that load processed data files into Supabase.
One script per dataset, run in the order listed below.

## Execution order

```
1. ingest_municipalities.py   — load geo data + municipality table
2. ingest_fires.py            — load fire_incidents.csv
3. ingest_funding.py          — load municipal_funding.csv
4. ingest_procurement.py      — load 2026_diavgeia_filtered.csv
                                (depends on org_to_municipality.csv mapping)
```

## Prerequisites

- Supabase project created
- `sql/001_init_schema.sql` applied
- `sql/002_webapp_schema.sql` applied (Sprint 1 — extends schema for web app tables)
- `.env` file with `SUPABASE_URL` and `SUPABASE_SERVICE_KEY`
- Data files in place under `data/`

## Scripts (to be created per sprint)

| Script | Sprint | Status |
|---|---|---|
| `ingest_municipalities.py` | Sprint 1 | [ ] |
| `ingest_fires.py` | Sprint 2 | [ ] |
| `ingest_funding.py` | Sprint 4 | [ ] |
| `ingest_procurement.py` | Sprint 3 | [ ] |
