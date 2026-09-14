# Web App

React + TypeScript frontend — scaffolded in Sprint 1.

See `WEBAPP_ROADMAP.md` in the project root for the full sprint plan.

## Standalone West Attica fire map

The current `mega_fire_2026` West Attica–Boeotia report is published at
`/analysis/west-attica-fire-2026/`. From the `fire_protection_2026` directory,
refresh it with one command:

```bash
./scripts/update_west_attica_fire_map.sh
```

The script performs these steps:

1. Copies the latest West Attica data into the `mega_fire_2026` frontend.
2. Builds that frontend with `/analysis/west-attica-fire-2026/` as its URL.
3. Copies the finished standalone map into FireWatch.
4. Builds FireWatch and stops if the two projects do not fit together.

The sync command replaces `public/analysis/west-attica-fire-2026`, then adds
the FireWatch footer, opt-in Google Analytics, cookie consent and page metadata.
The update remains local until the resulting FireWatch files are committed and
deployed.

## Planned stack

- React + TypeScript + Vite
- D3 (map + charts)
- Supabase JS client
- Deployed to Vercel

## Deployment

Vercel is configured to build the `app/` project from pushes to `main`.

## Directory structure (to be created in Sprint 1)

```
app/
├── src/
│   ├── components/
│   │   ├── Map/
│   │   ├── MunicipalityPanel/
│   │   ├── FireHistory/
│   │   ├── ProcurementActivity/
│   │   ├── FundingMetrics/
│   │   └── CompareView/
│   ├── hooks/
│   ├── lib/
│   │   └── supabase.ts
│   ├── pages/
│   └── types/
├── public/
├── package.json
└── vite.config.ts
```
