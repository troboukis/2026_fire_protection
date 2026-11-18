import analysisYearly3 from './catalogs/analysisYearly3.el.json'
import analysisYearly2 from './catalogs/analysisYearly2.el.json'
import analysisYearly1 from './catalogs/analysisYearly1.el.json'
import analysisAntinero from './catalogs/analysisAntinero.el.json'
import analysisStatistics from './catalogs/analysisStatistics.el.json'
import analysisCatalog from './catalogs/analysisCatalog.el.json'
import { taxonomyUi } from './taxonomy'
import { createAppI18n } from './instance'
import { createFormatters } from './format'
import { LOCALES, type AppLanguage } from './languages'
import { useAppI18n } from './useAppI18n'
import greek from './catalogs/overview.el.json'
import places from './catalogs/places.el.json'
import ministry from './catalogs/ministry.el.json'
import publicInfo from './catalogs/publicInfo.el.json'
import mediaTitles from './catalogs/mediaTitles.el.json'

type MessageKey = `analysisYearly3:${keyof typeof analysisYearly3}` | `analysisYearly2:${keyof typeof analysisYearly2}` | `analysisYearly1:${keyof typeof analysisYearly1}` | `analysisAntinero:${keyof typeof analysisAntinero}` | `analysisStatistics:${keyof typeof analysisStatistics}` | `analysisCatalog:${keyof typeof analysisCatalog}` | `overview:${keyof typeof greek}` | `places:${keyof typeof places}` | `ministry:${keyof typeof ministry}` | `publicInfo:${keyof typeof publicInfo}` | `mediaTitles:${keyof typeof mediaTitles}`
type Values = Record<string, string | number | null | undefined>
export type OverviewUi = ((source: string, values?: Values) => string) & ReturnType<typeof taxonomyUi> & {
  language: AppLanguage
  locale: typeof LOCALES[AppLanguage]
  format: ReturnType<typeof createFormatters>
}

// These are bundled UI messages, never an API request or automatic translation
// of record fields. Unknown source text is returned exactly as supplied.
const sourceKeys = new Map<string, MessageKey>([
  ...Object.entries(analysisYearly3).map(([key, source]) => [source, `analysisYearly3:${key}`] as [string, MessageKey]),
  ...Object.entries(analysisYearly2).map(([key, source]) => [source, `analysisYearly2:${key}`] as [string, MessageKey]),
  ...Object.entries(analysisYearly1).map(([key, source]) => [source, `analysisYearly1:${key}`] as [string, MessageKey]),
  ...Object.entries(analysisAntinero).map(([key, source]) => [source, `analysisAntinero:${key}`] as [string, MessageKey]),
  ...Object.entries(analysisStatistics).map(([key, source]) => [source, `analysisStatistics:${key}`] as [string, MessageKey]),
  ...Object.entries(analysisCatalog).map(([key, source]) => [source, `analysisCatalog:${key}`] as [string, MessageKey]),
  ...Object.entries(mediaTitles).map(([key, source]) => [source, `mediaTitles:${key}`] as [string, MessageKey]),
  ...Object.entries(publicInfo).map(([key, source]) => [source, `publicInfo:${key}`] as [string, MessageKey]),
  ...Object.entries(ministry).map(([key, source]) => [source, `ministry:${key}`] as [string, MessageKey]),
  ...Object.entries(places).map(([key, source]) => [source, `places:${key}`] as [string, MessageKey]),
  ...Object.entries(greek).map(([key, source]) => [source, `overview:${key}`] as [string, MessageKey]),
])
const contexts = new Map<AppLanguage, OverviewUi>()

export function getOverviewUi(language: AppLanguage = 'el'): OverviewUi {
  const existing = contexts.get(language)
  if (existing) return existing
  const instance = createAppI18n(language)
  const taxonomy = taxonomyUi(language)
  const text = (source: string, values: Values = {}) => {
    const match = source.match(/^(\s*)([\s\S]*?)(\s*)$/)!
    const key = sourceKeys.get(match[2])
    if (!key) return taxonomy.label(source)
    return match[1] + instance.t(key, values) + match[3]
  }
  const ui = Object.assign(text, { ...taxonomy, language, locale: LOCALES[language], format: createFormatters(language) })
  contexts.set(language, ui)
  return ui
}

export const greekOverviewUi = getOverviewUi('el')

export function useOverviewUi(): OverviewUi {
  return getOverviewUi(useAppI18n().language)
}
