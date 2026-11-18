import { translatedOrganizationName } from './organizationNames'
import type { AppLanguage } from './languages'
import procurementEl from './catalogs/procurement.el.json'
import procurementEn from './catalogs/procurement.en.json'
import overviewEl from './catalogs/overview.el.json'
import overviewEn from './catalogs/overview.en.json'
import placesEl from './catalogs/places.el.json'
import placesEn from './catalogs/places.en.json'
import ministryEl from './catalogs/ministry.el.json'
import ministryEn from './catalogs/ministry.en.json'
import taxonomyEl from './catalogs/taxonomy.el.json'
import taxonomyEn from './catalogs/taxonomy.en.json'
import suppliesEl from './catalogs/cpvSuppliesWorks.el.json'
import suppliesEn from './catalogs/cpvSuppliesWorks.en.json'
import servicesEl from './catalogs/cpvServices.el.json'
import servicesEn from './catalogs/cpvServices.en.json'
import cpvCodes from './cpvCodes.json'

// Lookup normalization never changes source records, filter values or URLs.
const normalize = (value: string) => value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLocaleUpperCase('el-GR').replace(/\s+/g, ' ').trim()
const pairs = (el: Record<string, string>, en: Record<string, string>) => Object.entries(el).map(([key, source]) => [normalize(source), en[key]] as const)
const labels = new Map([
  ...pairs(overviewEl, overviewEn), ...pairs(placesEl, placesEn),
  ...pairs(ministryEl, ministryEn), ...pairs(taxonomyEl, taxonomyEn),
  ...pairs(procurementEl, procurementEn),
])
const descriptions = new Map([...pairs(suppliesEl, suppliesEn), ...pairs(servicesEl, servicesEn)])
const codeLabels = new Map(Object.entries(cpvCodes))
const prefixes = [
  'ΑΛΛΟΣ ΦΟΡΕΑΣ', 'ΑΝΑΠΤΥΞΙΑΚΟΣ ΟΡΓΑΝΙΣΜΟΣ', 'ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ',
  'ΔΗΜΟΣ', 'ΔΗΜΟΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ', 'ΔΗΜΟΤΙΚΟ ΒΡΕΦΟΚΟΜΕΙΟ',
  'ΔΗΜΟΤΙΚΟ ΛΙΜΕΝΙΚΟ ΤΑΜΕΙΟ', 'ΔΗΜΟΤΙΚΟ ΠΕΡΙΦΕΡΕΙΑΚΟ ΘΕΑΤΡΟ',
  'ΚΕΝΤΡΟ ΚΟΙΝΩΝΙΚΗΣ ΠΡΟΝΟΙΑΣ ΠΕΡΙΦΕΡΕΙΑΣ', 'ΠΕΡΙΦΕΡΕΙΑ',
  'ΠΕΡΙΦΕΡΕΙΑΚΟ ΤΑΜΕΙΟ ΑΝΑΠΤΥΞΗΣ', 'ΣΥΝΔΕΣΜΟΣ ΔΗΜΩΝ', 'ΥΠΟΥΡΓΕΙΟ',
]

export function getTaxonomyUi(language: AppLanguage) {
  const label = (value: string) => language === 'el' ? value : labels.get(normalize(value)) ?? value
  const description = (value: string): string => {
    if (language === 'el') return value
    return value.split(/(\|)/).map(part => {
      const match = part.match(/^(\s*)([\s\S]*?)(\s*)$/)!
      const translated = descriptions.get(normalize(match[2]))
      return translated ? match[1] + translated + match[3] : part
    }).join('')
  }
  const cpv = (code: string, value: string) => language === 'el' ? value : codeLabels.get(code.trim()) ?? description(value)
  const organization = (value: string) => {
    if (language === 'el') return value
    const known = translatedOrganizationName(value)
    if (known) return known
    const token = normalize(value)
    // Retain the user-approved name for otherwise unseen ministry spellings.
    if (/^ΥΠΟΥΡΓΕΙΟ ΠΕΡΙΒΑΛΛΟΝΤΟΣ (?:ΚΑΙ|&) ΕΝΕΡΓΕΙΑΣ$/.test(token)) return 'Ministry of Environment and Energy'
    for (const prefix of prefixes) {
      if (token === prefix) return label(value)
      if (!token.startsWith(prefix + ' ')) continue
      const words = prefix.split(' ').length
      const match = value.match(new RegExp(`^(\\s*)(\\S+(?:\\s+\\S+){${words - 1}})(\\s+)([\\s\\S]*)$`))
      if (match) {
        const suffix = match[4]
        const joined = suffix.match(/^(·\s+)([\s\S]*)$/)
        const name = joined?.[2] ?? suffix
        return match[1] + label(prefix) + match[3] + (joined?.[1] ?? '') + (translatedOrganizationName(name) ?? name)
      }
    }
    return value
  }
  const thematic = (value: string) => {
    if (language === 'el') return value
    // The API exposes both JSON arrays and Python-style quoted list strings.
    // Parse only that constrained format, never evaluate source text or split
    // commas within an individual category such as agriculture/forestry.
    const trimmed = value.trim()
    if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
      const inner = trimmed.slice(1, -1)
      const members = [...inner.matchAll(/'([^']*)'|"([^"\\]*(?:\\.[^"\\]*)*)"/g)]
      const remainder = inner.replace(/'[^']*'|"[^"\\]*(?:\\.[^"\\]*)*"/g, '').replace(/[\s,]/g, '')
      if (!remainder && members.length) return members.map(match => label(match[1] ?? match[2])).join(' · ')
    }
    return value.split(/(\|)/).map(part => label(part)).join('')
  }
  return { label, description, cpv, organization, thematic }
}

const contexts = { el: getTaxonomyUi('el'), en: getTaxonomyUi('en') }
export const taxonomyUi = (language: AppLanguage) => contexts[language]
