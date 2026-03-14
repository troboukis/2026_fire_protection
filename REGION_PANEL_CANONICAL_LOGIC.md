# Region Panel Canonical Logic

This note records the agreed canonical model for geography-aware contract display in the app.

## Canonical Sources

- `municipality`: source of truth for municipalities
- `region`: source of truth for regions
- `organization`: source of truth for all non-municipality / non-region entities

## Display Rules

### Municipality panel

Show:
- canonical municipality contracts
- optionally organization contracts that explicitly cover the selected municipality

Do not infer municipality ownership from unrelated organization heuristics.

### Region panel

Show:
- canonical region contracts
- organization contracts only when the organization belongs to or covers the selected region

Exclude:
- `national` organizations

Do not rely on:
- `municipality_key IN (...)` alone
- frontend string heuristics

## Why This Exists

Regional organizations such as:
- development organizations
- decentralized administrations
- other curated region-scoped entities

may legitimately have `region_key` without a single `municipality_key`.

Example:
- `ΙΟΝΙΑ ΑΝΑΠΤΥΞΗ ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ ΟΤΑ ΠΕΡΙΦΕΡΕΙΑΣ ΙΟΝΙΩΝ ΝΗΣΩΝ (ΑΟΠΙΝ) Α.Ε.`

So a region panel that filters only by municipality-linked procurements will miss valid regional organizations.

## Recommended Backend Contract

Implement a DB-side API such as `region_contracts_rpc(region_key, year)` that:

1. includes canonical `region` contracts for the selected region
2. includes `organization` contracts whose coverage includes the selected region
3. excludes `national` organizations
4. deduplicates by `procurement.id`

The frontend should consume that RPC directly instead of reconstructing geography rules client-side.

## Practical Interpretation

- `Υπουργείο Περιβάλλοντος και Ενέργειας` is `national` and should not appear in region panels
- `ΑΟΠΙΝ` is a regional organization and should appear in the `ΙΟΝΙΩΝ ΝΗΣΩΝ` region panel

