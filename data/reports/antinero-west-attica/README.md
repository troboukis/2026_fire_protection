# AntiNERO West Attica research dataset

`dataset.json` is an offline, versioned research dataset for the interactive report.
It covers the 1 January 2022–12 August 2026 research window. It does not refresh or
write to Supabase. An audience-facing export now feeds the report page.

## Contents

- `contracts`: 21 records, including 4 amendments/supplements (17 originals).
- `decisions`: 426 unique ADA nodes, ordered by issue date and ID.
- `relationships`: 429 decision-to-contract associations and 4 amendment-to-parent links.
- `findings`: 47 verbatim research paragraphs from the actual Word document,
  including Part C. A paragraph can relate to several contracts.
- `sources`: repository-relative source paths and SHA-256 hashes.

The research source folder is intentionally gitignored. This dataset and its builder
live outside that folder so they can be versioned. Rebuilding requires the original
local archive; source PDFs are not copied into the web application's public assets.

## Provenance and evidence

Contract metadata comes from `khmdhs_2022_plus/relevant_contracts.json`.
The complete researched profile is preserved in `report_profile.fields`, including
signatories, contractor identifiers, procurement references and amendment notes.
These fields are extracted directly from Word tables rather than the generator script.

`amount` transcribes the Word report's amounts as decimal EUR strings. `amount_metadata`
preserves the separate KIMDIS metadata values. In particular, the researched value for
24SYMV015170089 is 799483.29 EUR excluding VAT and 991359.28 EUR including VAT.
The larger metadata value is not silently substituted for the researched contract price.

Findings retain exact Word body-block locators and the original report text.
All 47 paragraphs now have a manual review in `finding_reviews.json`, embedded in
`dataset.json` under each finding's `review`. Reviews distinguish supported,
qualified, corrected and unresolved findings. The three corrections include Greek
replacement drafts. Read `FINDINGS_AUDIT.md` for the paragraph-by-paragraph results.

Evidence references identify whole archived extracted documents with source URLs
and SHA-256 hashes. These are not page-specific citations or visual PDF validation.
The builder rejects changes to either the original paragraph or its evidence until
reviews are updated. Review notes must accompany interpretation of each status.
Original paragraphs are preserved for provenance, not approved publication copy.

Decision subjects, issue dates, document URLs, associations and database-presence flags
come from `diavgeia_inventory.tsv`, with 1-based file row numbers including the header.
Database presence is a boolean **at the August snapshot**, not current live status.
Research decision summaries remain null. Classifications are automatic, with matched
action, rule version and evidence provenance; they do not establish a decision's substantive effect. The dataset contains source links for all
nodes, but it does not claim full primary-source verification of every narrative finding.

## Consumer rules

- IDs are strings. Preserve Greek ADA characters and leading zeros in VAT numbers.
- Join `decision_ids` / `finding_ids` to the corresponding top-level arrays.
- `references_contract` points from decision to contract; `amends_contract` from
  amendment to parent. Shared decisions must remain one node with multiple edges.
- Use `amount.basis`: original amount, additional amount, or unchanged by amendment.
  Unchanged amendments have null amounts; do not count their parent price again.
- Keep `signed_on_metadata`, report signature/publication text and decision issue dates
  distinct. They are not actual work commencement/completion dates.
- `matched_research_offices` is a search-selection aid, not exhaustive geographic scope.
  Multi-region prices are not West Attica allocations. The Chalkida supplement is
  retained for its contract relationship, not as West Attica expenditure.
- The typo alias 24SYMV0142117833 belongs to 24SYMV014217833; never create another node.
- Zero matching decisions means zero in this search snapshot, not zero activity.
- The Word report's acceptance shorthand for 24SYMV014192335 must retain the
  clarification from ΡΩ5Π4653Π8-Ο4Χ: partial acceptance excluding planting.
- The JSON includes research/developer provenance. Do not display raw paths, schema
  field names or database diagnostics as audience-facing panel copy.

## Rebuild and validation

From the repository root:

```sh
python scripts/reports/antinero-west-attica/build_dataset.py
```

The standard-library builder reads the DOCX ZIP/XML, KIMDIS metadata and inventory;
it does not execute the Word-generation script or make network requests. It validates
unique IDs, all edge endpoints, 21/426/429/4 cardinalities, source-file existence,
research coverage for every contract, the corrected amount, unchanged amendments and
the typo alias. Source hashes make the exact research inputs identifiable.

The fixed counts deliberately make a future archive refresh fail for review rather
than silently change this publication snapshot. Editorial integration, page-level citation refinement and event extraction remain
separate work; no inferred event timeline is supplied.

## Frontend publication

`publication.json` contains the manually prepared Aigaleo summaries (all 27 decisions)
and four Greek findings incorporating the review. Other decisions retain their exact
published subjects and automatic category, without an inferred narrative summary. The broader research review
is preserved; publication readiness is an explicit, separate selection.

Run `python scripts/reports/antinero-west-attica/build_publication.py` after rebuilding the research dataset.
It exports `app/src/features/analysis/antinero/data/antineroReport.generated.json`, containing 21 contract records,
426 unique decisions and the four prepared findings. The export omits internal notes,
local paths and contractor tax identifiers. Source hashes and finding-review hashes
prevent editorial text silently surviving changes to its evidence.

The page offers all 17 originals, linked amendments, shareable contract/decision URLs,
all related decisions in a continuous pan-and-zoom network or timeline and the full related-contract list for shared
decisions. Decision dates are issue dates, not inferred execution dates. Supplemental
approval documents and original versus additional amounts remain explicitly labelled.

## Automatic classification and category graph

`python scripts/reports/antinero-west-attica/build_dataset.py` applies
`scripts/reports/antinero-west-attica/classify_decisions.py` to every unique decision. Re-run
`python scripts/reports/antinero-west-attica/build_publication.py` to refresh the app export.

The five fixed categories and shared graph colours are:

- Μελέτες — blue `#2864a0`: studies, schedules, start orders, supervision, committees.
- Τροποποιήσεις — amber `#ad661b`: extensions, suspensions, amendments, APE/new prices.
- Παραλαβή — green `#277b64`: measurements, recorded-work checks, completion, acceptance.
- Πληρωμές — purple `#8555a3`: accounts, special accounts, payment orders, compensation.
- Λοιπά — grey `#777777`: no clear action match or conflicting actions.

The classifier normalizes Greek accents/case and reads the administrative action in
its title, excluding the quoted project description. If no action matches, it checks
only the operative section after ΑΠΟΦΑΣΙΖΟΥΜΕ/ΑΠΟΦΑΣΙΖΕΙ, not the recital of earlier
acts. It keeps the matched rules, evidence and relevant hashes in `classification`.
Unknown or ambiguous text is retained under Λοιπά, never assigned arbitrarily.
Guarantee reductions alone remain Λοιπά; accounts that also reduce guarantees remain
Πληρωμές. Classifying a payment order does not verify a bank payment.

Current automatic results: 89 studies, 83 changes, 98 acceptance, 137 payments,
19 other. 405 title matches and two operative-text matches; 19 abstentions. These
are automatic classification results, separate from manual findings review.

Each contract expands to exactly five category nodes, including empty categories.
Categories toggle independently and expose all their documents on the same graph.
Opening a direct decision URL expands its category. Shared decisions keep one
classification across all their contract links. Category colours are also labelled
in text and in the legend.

Checks: `python scripts/reports/antinero-west-attica/test_classify_decisions.py` and, from `app/`,
`npm test -- src/features/analysis/antinero/AntineroNetwork.test.ts src/features/analysis/antinero/data/antineroReport.test.ts`.

### Node movement

The network uses the existing D3 dependency for link springs, repulsion, collision
spacing and gentle centering. Drag any contract, category or document node; dragging
the background pans the camera. Nodes are held at the pointer during a drag and
released back into the simulation afterwards. Positions survive category toggles
within the mounted graph. Pause/resume stops/restarts settling while still allowing
manual movement. Fit-all uses current node positions. Simulation timers stop on
unmount and topology changes, including React StrictMode cleanup.

Physics tests: `npm test -- src/features/analysis/antinero/data/antineroPhysics.test.ts` from `app/`.


### Contract overview and connection criteria

The default route displays all 21 contract records (17 originals and four amendments)
without an automatic contract selection. A native dropdown filters to one contract;
returning to “Όλες οι συμβάσεις” restores the overview. In the overview, multiple
contracts and their category branches may be expanded concurrently. The same ADA is
rendered once with every applicable expanded branch connection.

Contract top rules use programme colours separately from document-category markers:
I ochre, II blue, III green, IV purple, unspecified grey. The current archive has no
I records, so the legend displays II, III, IV and unspecified. Programme labels come
from registry titles with attachment fallback, not inferred from dates. Existing
funding-programme discrepancies remain visible in contract notes.

`contractConnections()` derives five distinct evidenced pairs: four parent/amendment
links and one pair of management-study contracts sharing two decisions. One supplement
also shares its approval decision with its parent; that evidence is merged into the
amendment edge, avoiding overlapping duplicate pair links. Solid lines mean amendment;
dashed lines mean shared decisions. Both link types can be toggled and selected to
open source documents. Missing links do not establish that contracts are unrelated.

Potential additional criteria should be explicit optional layers: exact contractor
VAT (not fuzzy name matching), or common procurement/award IDs. The public export
currently omits contractor VAT. Common programme, office or area is suitable for
colour/filter/grouping; it does not by itself imply contractual dependence.
