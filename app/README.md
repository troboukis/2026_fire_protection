# Web App

React + TypeScript frontend — scaffolded in Sprint 1.

See `WEBAPP_ROADMAP.md` in the project root for the full sprint plan.

## Standalone West Attica fire map

The page at `/west-attica-fire-2026/` is published from the generated
`mega_fire_2026/app/dist` directory. To refresh it:

1. Run `npm run build` in `mega_fire_2026/app`.
2. Run `npm run sync:west-attica-fire` in this `app` directory.

The sync command replaces `public/west-attica-fire-2026`, then adds the same
opt-in Google Analytics and cookie-consent behavior used by FireWatch. The map
build must keep its Vite base set to `/west-attica-fire-2026/`.

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
