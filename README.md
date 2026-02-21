# fire_protection_2026

Project structure:

```text
.
├── data/
│   └── 2026_diavgeia.csv
├── src/
│   └── fetch_diavgeia.py
├── state/
│   └── state.json
├── logs/
│   └── fetch_runs.csv
└── fetch_diavgeia.py
```

- `src/fetch_diavgeia.py`: core fetch + enrich + persist logic
- `data/2026_diavgeia.csv`: dataset output
- `state/state.json`: incremental fetch checkpoint
- `logs/fetch_runs.csv`: run history (timestamp, fetched count, CSV update flag, success/error)
- `fetch_diavgeia.py`: root launcher for backward-compatible execution

Run:

```bash
python fetch_diavgeia.py
```

## Daily automated collection (GitHub Actions)

A workflow is included at `.github/workflows/daily-fetch.yml` and runs:

- every day at `03:00` UTC
- on manual trigger (`workflow_dispatch`)

To enable automation:

```bash
git add .
git commit -m "chore: setup daily Diavgeia automation"
git branch -M main
git remote add origin <your-github-repo-url>
git push -u origin main
```

Then in GitHub:

1. Open the repository `Settings` -> `Actions` -> `General`
2. Ensure actions are allowed and workflow permissions allow read/write
3. Open the `Actions` tab and run `Daily Diavgeia Fetch` once manually

## Database schema (Supabase/Postgres)

The initial relational schema is in `sql/001_init_schema.sql`.
It creates:
- `organization` (one organization to many records)
- `record` (main records, each linked to one organization)
- `file` (one-to-one with `record` via `ada`)

To create tables:

1. Open your Supabase project
2. Go to `SQL Editor`
3. Paste and run the contents of `sql/001_init_schema.sql`
