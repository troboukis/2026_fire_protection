import { readFileSync, mkdirSync } from 'node:fs'
import { createRequire } from 'node:module'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../', import.meta.url))
const require = createRequire(new URL('../app/package.json', import.meta.url))
const readJson = file => JSON.parse(readFileSync(path.join(root, file), 'utf8'))

// Resolve the shared, reviewed translations at build time. Only the small
// result is shipped to the archived application, never the full UI runtime.
export function westAtticaMessages() {
  const directory = path.join(root, 'state/translations/standalone-build')
  mkdirSync(directory, { recursive: true })
  const output = path.join(directory, 'shared-ui.cjs')
  require('esbuild').buildSync({
    stdin: {
      contents: `export { getOverviewUi } from './src/i18n/overview'; export { cookieConsentTranslation } from './src/i18n/cookieConsentTranslation'; export { createAppI18n } from './src/i18n/instance';`,
      resolveDir: path.join(root, 'app'), loader: 'ts',
    },
    bundle: true, platform: 'node', format: 'cjs', outfile: output,
    define: { 'import.meta.env.BASE_URL': JSON.stringify('/'), 'import.meta.env.DEV': 'false', 'import.meta.env.PROD': 'false' },
  })
  delete require.cache[output]
  const { getOverviewUi, cookieConsentTranslation, createAppI18n } = require(output)
  const texts = {}
  for (const name of ['analysisWestAtticaUi', 'analysisWestAtticaText', 'analysisWestAtticaPlaces']) {
    const sources = readJson(`app/src/i18n/catalogs/${name}.el.json`)
    const targets = readJson(`app/src/i18n/catalogs/${name}.en.json`)
    if (Object.keys(sources).sort().join() !== Object.keys(targets).sort().join()) throw new Error('Standalone translation keys differ.')
    for (const [key, source] of Object.entries(sources)) {
      if (!targets[key]?.trim()) throw new Error('Standalone translation is missing.')
      texts[source] = targets[key]
    }
  }
  const works = readJson('app/public/analysis/west-attica-fire-2026/data/attica-boeotia-2026/works.json')
  const rows = Array.isArray(works) ? works : works.works ?? works.rows
  const organizations = {}
  const ui = getOverviewUi('en')
  for (const row of rows) if (row.authority_name) organizations[row.authority_name] = ui.organization(row.authority_name)
  const languages = {}
  for (const language of ['el', 'en']) {
    const instance = createAppI18n(language)
    const cookie = cookieConsentTranslation(language)
    cookie.preferencesModal.sections[0].description = language === 'el'
      ? 'Η επιλογή σας ισχύει και για την κύρια εφαρμογή του FireWatch.'
      : texts['Η επιλογή σας ισχύει και για την κύρια εφαρμογή του FireWatch.']
    // The existing standalone consent links open a new tab.
    const links = value => typeof value === 'string' ? value.replace(/<a href=/g, '<a target="_blank" rel="noopener" href=')
      : Array.isArray(value) ? value.map(links)
      : value && typeof value === 'object' ? Object.fromEntries(Object.entries(value).map(([key, item]) => [key, links(item)])) : value
    languages[language] = {
      cookie: links(cookie),
      footer: {
        terms: instance.t('navigation:terms'), privacy: instance.t('navigation:privacy'),
        preferences: instance.t('navigation:cookiePreferences'),
      },
    }
  }
  return { texts, organizations, languages }
}
