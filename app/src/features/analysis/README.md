# Analysis feature

The `/analysis` section is organized by published analysis so page copy, UI, data and tests stay close together.

## Catalog

- `catalog/AnalysisPage.tsx`: cards shown at `/analysis`.
- `catalog/AnalysisCard.tsx`: shared catalog card, including the image slot.
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

## AntiNERO West Attica

- `antinero/AntineroPage.tsx`: published report page.
- `antinero/AntineroNetwork.tsx`: interactive network.
- `antinero/Antinero.css`: report and network styling.
- `antinero/data/`: frontend graph logic, physics and generated publication snapshot.
- `data/reports/antinero-west-attica/`: reviewed research and editorial publication inputs.
- `scripts/reports/antinero-west-attica/`: dataset, classification and publication builders.

Do not edit `antinero/data/antineroReport.generated.json` directly. Edit the reviewed source material under `data/reports/antinero-west-attica/`, then run `python scripts/reports/antinero-west-attica/build_publication.py`.

Place page preview images under `app/public/social/`. The analysis cards currently use `statistical-analysis.png` and `antinero-west-attica.png`.
