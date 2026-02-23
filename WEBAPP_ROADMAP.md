# Forest Fire Protection — Public Interest Web App

## Concept

A civic transparency tool that lets any citizen select a municipality (or regional/national authority area) on a map of Greece and see:

1. The area's **20-year forest fire history** (frequency, severity, hectares burned)
2. **What all levels of government did** to protect it — public procurement decisions from Diavgeia, filterable by authority level (municipality, region, national)
3. **How much money was allocated** by the central government for fire protection vs. how much was actually spent
4. A plain-language **accountability summary** tying all three together

The implicit civic question the app answers: *Was this area protected — and by whom?*

---

## Design Principles

- **Place-first, not institution-first.** Users think in terms of location, not authority hierarchy. The map is always the primary entry point.
- **Absence of records is also data.** Municipalities with high fire risk and low procurement activity are a story worth surfacing, not hiding.
- **Numbers serve the narrative.** Every visualization traces back to the question a citizen actually asks, not the question an analyst asks.
- **Honest about gaps.** Absence of Diavgeia records ≠ inactivity. Disclaimers are part of the product, not an afterthought.

---

## Data Sources

| Dataset | Description | Status |
|---|---|---|
| `data/2026_diavgeia_filtered.csv` | Relevant public procurement decisions (fire protection) from Diavgeia | Pending — run `filter_relevance.py` |
| Historical fire incidents | 20+ years of forest fire events in Greece (location, year, hectares burned, cause) | To be sourced (EFFIS / Πυροσβεστική / WWF) |
| Municipal fire protection funding | Yearly central government allocations per municipality for fire protection | To be sourced |
| Municipality GeoJSON | Kallikratis/Kleisthenis boundary files for all ~332 Greek municipalities | Available from geodata.gov.gr |
| `org_name_clean → municipality_id` mapping | Lookup table connecting Diavgeia org names to municipality geometries | To be built — critical path item |

---

## Tech Stack

| Layer | Choice |
|---|---|
| Frontend | React + TypeScript + Vite |
| Map | D3 (choropleth) or MapLibre GL |
| Charts | D3 |
| Backend / DB | Supabase (Postgres + PostGIS) |
| Hosting | Vercel |
| Analytics | Plausible (privacy-respecting) |

---

## Database Schema (planned extensions to `sql/001_init_schema.sql`)

Beyond the existing `organization`, `record`, `file` tables:

- `municipality` — id, name, region, population, forest_coverage_ha, geometry (PostGIS)
- `authority` — id, name, type (municipality/region/national), parent_id (hierarchy)
- `fire_incident` — id, municipality_id, year, hectares_burned, cause, severity
- `procurement_decision` — id, ada, municipality_id, authority_id, decision_type, amount, date, subject
- `municipal_funding` — id, municipality_id, year, amount_allocated

---

## Authority Levels in the Data

| Level | Examples | Typical role |
|---|---|---|
| Municipality (Δήμος) | Δήμος Αρταίων | Local clearance, road access, local equipment |
| Region (Περιφέρεια) | Περιφέρεια Αττικής | Regional firebreaks, inter-municipal programs |
| Decentralized Admin | Αποκεντρωμένη Διοίκηση | Forest service coordination, forestry roads |
| National | Υπουργείο Περιβάλλοντος, Πολιτική Προστασία | Strategy, large programs, EU funds |

---

## Accountability Triangle

```
Central Gov Allocation  →  What municipalities received  (funding dataset)
       ↓                              ↓
  Did they spend it?    ←   Procurement activity          (filtered Diavgeia)
       ↓
  Did it matter?        ←   Historical fire damage        (fire incidents dataset)
```

Key metric: **utilization rate** = funds received vs. funds procured (per municipality, per year).

---

## Sprint Backlog

### Pre-Sprint — Data Acquisition (parallel track)

> Not a code sprint. These unblock Sprints 2–4 and should run in parallel with Sprint 1.

- [ ] Run `filter_relevance.py` → finalize `data/2026_diavgeia_filtered.csv`
- [ ] Source historical fire incidents dataset (2000–2025)
- [ ] Source central government municipal fire protection funding dataset
- [ ] Acquire Greek municipality GeoJSON (geodata.gov.gr)
- [ ] Build `org_name_clean → municipality_id` normalization/mapping table
- [ ] Define which regional/national authorities map to which municipality groups

---

### Sprint 1 — Interactive Map Shell

**Goal:** A user can open the app, explore a map of Greece, click any municipality, and see a basic profile panel.

| Story | Detail | Done |
|---|---|---|
| Project scaffolding | React + TypeScript + Vite, Supabase client, deploy to Vercel | [ ] |
| Supabase geography table | `municipality` table with PostGIS geometry | [ ] |
| Greece choropleth map | D3 or MapLibre, rendered from municipality GeoJSON, colored by region | [ ] |
| Click → info panel | Municipality name, region, population (static stub) | [ ] |
| Map zoom & pan | Smooth interaction, mobile-tolerant | [ ] |
| URL routing | Each municipality gets a shareable URL (`/municipality/athens`) | [ ] |

**End state:** Deployed app, interactive map, clickable municipalities, shareable links. Stub data only.

---

### Sprint 2 — Fire History Layer

**Goal:** A user can click a municipality and see its 20-year fire history.

| Story | Detail | Done |
|---|---|---|
| Fire incidents table | Supabase: `fire_incident` schema + PostGIS index | [ ] |
| Data ingestion script | Load historical fire CSV into Supabase | [ ] |
| Fire timeline chart | D3 bar/area — incidents per year, sized by hectares burned | [ ] |
| Severity map overlay | Choropleth shade = cumulative burn area (20yr) | [ ] |
| Key stats panel | Worst year, total hectares burned, fire count, last incident | [ ] |
| "No data" state | Graceful handling for municipalities with no recorded fires | [ ] |

**End state:** The map communicates fire risk before any click. Full fire history per municipality.

---

### Sprint 3 — Protection Activity (Procurement)

**Goal:** A user can see what authorities have done for fire protection in their area, overlaid on fire history.

> Requires: `data/2026_diavgeia_filtered.csv` finalized + `org_name_clean → municipality_id` mapping

| Story | Detail | Done |
|---|---|---|
| Procurement schema | `procurement_decision` + `authority` tables in Supabase | [ ] |
| Data ingestion | Load filtered CSV, resolve org names to municipality ids | [ ] |
| Unified timeline | Fire incidents + procurement decisions on one shared time axis | [ ] |
| Decision type breakdown | Bar/donut: ΑΝΑΘΕΣΗ / ΕΓΚΡΙΣΗ ΔΑΠΑΝΗΣ / ΑΝΑΛΗΨΗ / ΠΛΗΡΩΜΗ | [ ] |
| Decision list | Filterable table: date, subject, type, amount, ADA link to Diavgeia | [ ] |
| Authority level tag | Visual badge on each decision: Δήμος / Περιφέρεια / Υπουργείο | [ ] |

**End state:** Core product value live. Citizens can see the temporal relationship between fire events and government procurement.

---

### Sprint 4 — Funding & Accountability Metrics

**Goal:** A user can see how much money was allocated vs. spent, and get a plain-language accountability summary.

> Requires: municipal fire protection funding dataset

| Story | Detail | Done |
|---|---|---|
| Funding table | `municipal_funding` schema + ingestion script | [ ] |
| Allocation vs. spending chart | D3 grouped bar: allocated vs. procured per year | [ ] |
| Utilization rate metric | Large prominent number: "spent X% of allocated funds" | [ ] |
| Multi-year selector | Year range filter affecting all panels | [ ] |
| Accountability summary card | 3 numbers: total allocated, total procured, utilization rate + plain-language note | [ ] |
| Edge case: overspend | Municipalities that exceeded allocation treated as positive signal | [ ] |

**End state:** "Follow the money" story complete. App answers not just *what was done* but *how much of available funding was used*.

---

### Sprint 5 — Multi-Authority View

**Goal:** A user can see everything all levels of government did in their area, filterable by authority level.

| Story | Detail | Done |
|---|---|---|
| Authority hierarchy | `authority` table with parent/child relationships | [ ] |
| Regional attribution | Map regional/national procurement to covered municipalities | [ ] |
| Authority toggle | Filter panel: show/hide by authority level | [ ] |
| "Who did what" breakdown | Stacked bar: procurement by authority level per year | [ ] |
| National programs panel | Major national/EU programs affecting the area | [ ] |
| Coverage note | "This area is also covered by [Περιφέρεια X] and [Αποκεντρωμένη Διοίκηση Y]" | [ ] |

**End state:** Full institutional picture per place. Citizens see what the municipality didn't do and whether higher authorities compensated.

---

### Sprint 6 — Discovery & Comparison

**Goal:** A user can search by municipality, compare multiple areas, and see national rankings.

| Story | Detail | Done |
|---|---|---|
| Search | Municipality autocomplete (name, region) | [ ] |
| Compare mode | Select up to 3 municipalities → side-by-side panels | [ ] |
| National rankings | Top municipalities by utilization rate, procurement volume, fire risk | [ ] |
| "Municipalities like mine" | Contextual comparison by forest coverage, region, population band | [ ] |
| Shareable comparison URL | Encoded in URL | [ ] |
| National overview page | Greece-wide totals, most active authorities, largest fires | [ ] |

**End state:** App becomes discoverable and shareable. Useful for journalists and researchers, not just individual citizens.

---

### Sprint 7 — Production Hardening

**Goal:** App is trustworthy, accessible, fast, and ready for public release.

| Story | Detail | Done |
|---|---|---|
| Mobile responsiveness | Full map + panel on mobile (tap-friendly, readable) | [ ] |
| Loading & error states | Skeleton screens, error boundaries, graceful API failures | [ ] |
| Methodology page | Data sources, limitations, what "procurement" means, attribution gaps | [ ] |
| Disclaimers | "Absence of records ≠ inactivity", data freshness timestamp | [ ] |
| Performance | Query optimization, Supabase indexes, lazy-loaded map tiles | [ ] |
| SEO & social sharing | Open Graph tags, municipality-level meta descriptions | [ ] |
| Accessibility | Keyboard navigation, screen reader compatibility, color contrast | [ ] |
| Analytics | Plausible or equivalent (privacy-respecting) | [ ] |

**End state:** Public launch ready.

---

## Sprint Dependency Map

```
Pre-Sprint (data acquisition) ──────────────────────────────┐
                                                             │
Sprint 1 (map shell)                                         │
    └── Sprint 2 (fire data)          ←── fire dataset ──────┤
            └── Sprint 3 (procurement) ←── filtered CSV ─────┤
                    └── Sprint 4 (funding) ←── funding data ──┘
                            └── Sprint 5 (multi-authority)
                                    └── Sprint 6 (comparison)
                                            └── Sprint 7 (hardening)
```

**Critical path bottleneck:** `org_name_clean → municipality_id` mapping.
This must be resolved before Sprint 3 data ingestion. Start it during Sprint 1.

---

## Key Risks

| Risk | Impact | Mitigation |
|---|---|---|
| `org_name_clean` doesn't match municipality names cleanly | Blocks Sprint 3 data ingestion | Build mapping table early; accept ~80% auto-match + manual corrections |
| Fire incidents dataset not publicly available in clean form | Delays Sprint 2 | Use EFFIS (EU public data) as primary source; supplement with local data |
| Funding dataset scope unclear (fire-specific vs. general transfers) | Undermines accountability metrics | Define scope explicitly in methodology page; be conservative |
| Regional procurement attribution is ambiguous | Sprint 5 data quality | Flag as "regional-level" without over-attributing to specific municipalities |
| Municipality boundary changes (Kallikratis → Kleisthenis reform) | Historic fire data may use old boundaries | Normalize everything to current Kleisthenis boundaries with a crosswalk table |
