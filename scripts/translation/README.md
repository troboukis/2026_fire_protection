# Local DeepL preparation

Read the ignored root `ENGLISH_VERSION_IMPLEMENTATION.md` before and after
each English-version change and record its before/after journal entries.

Run from the repository root with the existing Python environment:

```sh
.fireprotection/bin/python scripts/translation/deepl_ui.py usage
.fireprotection/bin/python scripts/translation/deepl_ui.py plan --catalogue procurement
.fireprotection/bin/python scripts/translation/deepl_ui.py translate --catalogue procurement --max-characters 12000
.fireprotection/bin/python scripts/translation/deepl_ui.py review --catalogue procurement
.fireprotection/bin/python scripts/translation/deepl_ui.py glossary
.fireprotection/bin/python -m unittest discover -s scripts/translation -p 'test_*.py'
```

`usage` and `plan` never submit translation text. `translate` creates/reuses a
native EL → EN glossary and translates into EN-GB. The per-run ceiling cannot
raise the fixed 1,000,000-character project budget or the account/key limits.
Only UI sources explicitly listed in `CATALOGUES` can be sent. **Contract and
document titles are excluded, including from context.** No database or document
extraction is performed. Add catalogues only after reviewing their full contents.

Publication and programme titles on the Media page are explicitly in scope:
use the `mediaTitles` catalogue. The title exclusion applies to contracts and
administrative/source documents, not media headlines. The separate `publicInfo`
context retains its original wording to reuse already paid policy/UI text.

The key stays in the root `.env`. The ignored `state/translations/` directory
contains a durable ledger, cached machine results and generated output. Keep it
between runs; never delete it to reset a limit. Each batch records usage before
and after, reserves Unicode code points before its POST and keeps the API's
actual billed count. The estimate includes XML protection overhead conservatively.

Remote usage can lag. All local committed/reserved characters remain an
additional conservative allowance deduction, even after they appear remotely.
This intentionally underestimates available capacity; there is no automatic
release on a timer, remote period reset or changed key. Account/key usage includes
other callers, but this local lock only coordinates this tool's own processes.
Do not raise server-side limits to bypass a refusal.

There are no automatic POST retries. After a timeout, malformed output or billing
discrepancy, the request remains unresolved and blocks further translation. Inspect
the preserved response, usage and reservation before making an explicit correction;
do not replay the request or delete its ledger entry. A successful result is cached
before the final usage check, so a failed usage check does not discard paid work.

Machine output is **not** imported automatically by the app. Review it, verify
financial terms/placeholders/numbers in context and then copy the edited strings
into the matching `app/src/i18n/catalogs/*.en.json` resource. Future translation
runs write only the ignored machine result; they never overwrite editorial edits.

After editing the app's English catalogue, `review` records its approved strings
in the cache. It requires an existing paid translation with the exact same source,
context and options and checks every placeholder. It sends no translation POST.
This permits an explicitly reviewed terminology update to reuse earlier paid work
under the new glossary fingerprint, while retaining the original machine output.
Cache exports prefer the approved wording. Review again after further editorial
changes; do not use approval to bypass the initial DeepL pass for new messages.

`glossary` verifies or synchronises the current terminology. When updating, it
reuses only the FireWatch glossary already identified in the local ledger, saves
its previous dictionary and verifies the new entries. This works within a single
glossary slot and does not raise account limits. Unknown glossaries are never
overwritten or deleted. Any failed update stops before text is submitted.

The controlled `taxonomy`, `cpvSuppliesWorks` and `cpvServices` catalogues contain
category labels, not record titles or free-form document content. Their initial
DeepL output is reviewed against the [official CPV 2008 vocabulary](https://ted.europa.eu/en/simap/cpv).
The app bundles only verified code labels (`cpvCodes.json`), with the download
URL, verification date and XML hash in `cpvProvenance.json`. Code-based labels use
the official English entry. A shortened Greek description shown without a code
keeps its narrower source meaning. Unknown values remain unchanged. Rendering
helpers never rewrite database fields, filter values, identifiers or URLs.

Full public-authority names are explicitly in scope in `municipalityNames`,
`regionNames` and `organizationNames1`–`organizationNames5`. They are prepared
from name-only source fields and reviewed before any submission. Contract and
administrative-document titles remain excluded. Known aliases share a reviewed
display name; source records, IDs and filter/query values are never rewritten.
Historical or differently worded names are retained separately: sharing a
municipality key is insufficient to merge names. Unknown future names retain
their original spelling. English authority names are searchable alongside the
Greek originals; search operators and contract-chain inclusion stay unchanged.
