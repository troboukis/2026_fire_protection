# Analysis feature

The `/analysis` section is organized by published analysis so page copy, UI, data and tests stay close together.

## Catalog

- `catalog/AnalysisPage.tsx`: cards shown at `/analysis`.
- `catalog/AnalysisCard.tsx`: shared catalog card, including the image slot.
- `catalog/AnalysisPromo.tsx` and `catalog/AnalysisPromo.css`: animated homepage banner, alternating statistics and AntiNERO links with pause/manual controls and reduced-motion support.
- `catalog/AnalysisCatalog.css` and `catalog/AnalysisCard.css`: catalog layout and card styling.
- `app/analysis/index.html`: prerendered shell for `/analysis` with SEO meta tags and a crawlable summary (search engines and LLM crawlers) listing every published analysis, including AntiNERO. Registered as a Vite build input (`vite.config.ts`) and a Vercel rewrite (`vercel.json`), same as the statistics and AntiNERO pages below. Update it whenever an analysis is added to or removed from the catalog.

## Statistical analysis

- `statistics/StatisticalAnalysisPage.tsx`: route shell and back navigation.
- `statistics/StatisticalAnalysis.tsx`: data loading and the main analysis layout.
- `statistics/analysisData.ts`: types, normalization and aggregate calculations.
- `statistics/AnalysisCharts.tsx`: monthly bar chart, breakdown bars and the interactive sunburst.
- `statistics/DirectAwardHistogram.tsx`: direct-award distribution.
- `statistics/TopAuthoritiesSection.tsx`: contracting-authority ranking.
- `statistics/StatisticalAnalysis.css`: statistical-analysis charts, tables and responsive styling.
- The database RPC is defined in `sql/044_contract_analysis_rpc.sql` and mirrored in `sql/900_frontend_api_setup.sql`.

## Yearly report 2026

- Route: `/analysis/yearly_report` (prerendered shell `app/analysis/yearly_report/index.html`, Vite input and Vercel rewrites in both `vercel.json` files).
- `yearly-report/YearlyReportPage.tsx`: route shell and back navigation.
- `yearly-report/YearlyReport.tsx`: the report text. Every number is read from the generated snapshot, and every sentence whose wording depends on the data (rises, falls, rankings, empty categories) is conditional, because the snapshot changes every day.
- `yearly-report/ReportCharts.tsx`: SVG charts (columns, grouped and stacked columns, horizontal stacks, log histogram, bar list) with hover/focus tooltips and a data table under every chart.
- `yearly-report/reportData.ts`: the explicit type of the snapshot (so a new day's values, empty lists or nulls never break the build), lookups and Greek number formatting. `reportData.test.ts` checks the snapshot arithmetic; `YearlyReport.test.ts` renders the page with the current snapshot.
- `yearly-report/data/yearlyReport2026.generated.json`: audience-facing aggregates. Do not edit it by hand.

The report covers 1/1/2026 up to the day of the build, capped at 31/12/2026. Amounts are without VAT (Diavgeia amounts are divided by 1.24, except payroll and other VAT-free payments), the total is the 2026 contracts plus the 2026 payments that are not linked to any contract, and records about the fire safety of buildings and facilities are left out of every figure (never deleted). The rules are in the builder and in `data/reports/yearly-report-2026/README.md`.

`.github/workflows/yearly-report-refresh.yml` rebuilds the report every evening and after every "Daily Data Refresh" run, checks that the page type-checks and renders, and commits the regenerated files. It stops after 1/1/2027 unless run by hand with `force`. To rebuild by hand from the repository root (read-only database access):

```bash
.fireprotection/bin/python scripts/reports/yearly-report-2026/build_yearly_report.py
```

The builder also writes `data/reports/yearly-report-2026/` (full results, per-contract and per-payment CSV files with the scope status of every record, and the fact-check guide `README.md`). `--snapshot` freezes the rows that are counted and `--out-root` writes a trial build elsewhere.

## AntiNERO West Attica

- `antinero/AntineroPage.tsx`: published report page.
- `antinero/AntineroNetwork.tsx`: interactive network.
- `antinero/Antinero.css`: report and network styling.
- `antinero/data/`: frontend graph logic, physics and generated publication snapshot.
- `data/reports/antinero-west-attica/`: reviewed research and editorial publication inputs.
- `scripts/reports/antinero-west-attica/`: dataset, classification and publication builders.

Do not edit `antinero/data/antineroReport.generated.json` directly. Edit the reviewed source material under `data/reports/antinero-west-attica/`, then run `python scripts/reports/antinero-west-attica/build_publication.py`.

Place page preview images under `app/public/social/`. The analysis cards currently use `statistical-analysis.png` and `antinero-west-attica.png`.
