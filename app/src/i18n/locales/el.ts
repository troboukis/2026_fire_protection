import analysisYearly3 from '../catalogs/analysisYearly3.el.json'
import analysisYearly2 from '../catalogs/analysisYearly2.el.json'
import analysisYearly1 from '../catalogs/analysisYearly1.el.json'
import analysisAntinero from '../catalogs/analysisAntinero.el.json'
import analysisStatistics from '../catalogs/analysisStatistics.el.json'
import analysisCatalog from '../catalogs/analysisCatalog.el.json'
import organizationNames5 from '../catalogs/organizationNames5.el.json'
import organizationNames4 from '../catalogs/organizationNames4.el.json'
import organizationNames3 from '../catalogs/organizationNames3.el.json'
import organizationNames2 from '../catalogs/organizationNames2.el.json'
import organizationNames1 from '../catalogs/organizationNames1.el.json'
import regionNames from '../catalogs/regionNames.el.json'
import municipalityNames from '../catalogs/municipalityNames.el.json'
import cpvServices from '../catalogs/cpvServices.el.json'
import cpvSuppliesWorks from '../catalogs/cpvSuppliesWorks.el.json'
import taxonomy from '../catalogs/taxonomy.el.json'
import procurement from '../catalogs/procurement.el.json'
import overview from '../catalogs/overview.el.json'
import places from '../catalogs/places.el.json'
import ministry from '../catalogs/ministry.el.json'
import publicInfo from '../catalogs/publicInfo.el.json'
import mediaTitles from '../catalogs/mediaTitles.el.json'

export const el = {
  analysisYearly3,
  analysisYearly2,
  analysisYearly1,
  analysisAntinero,
  analysisStatistics,
  analysisCatalog,
  municipalityNames,
  regionNames,
  organizationNames1,
  organizationNames2,
  organizationNames3,
  organizationNames4,
  organizationNames5,

  taxonomy,
  cpvSuppliesWorks,
  cpvServices,
  mediaTitles,
  publicInfo,
  ministry,
  places,
  overview,
  procurement,
  common: {
    loading: 'Φόρτωση…',
    loadingPage: 'Φόρτωση σελίδας…',
    loadingData: 'Φόρτωση δεδομένων',
    retrievingLatest: 'Ανακτώνται τα πιο πρόσφατα στοιχεία.',
    totalUnavailable: 'Το σύνολο δεν είναι διαθέσιμο.',
    documentsWithoutAmount_one: '{{count, number}} έγγραφο δεν έχει ποσό',
    documentsWithoutAmount_other: '{{count, number}} έγγραφα δεν έχουν ποσό',
  },
  navigation: {
    language: 'Γλώσσα',
    home: 'Αρχική',
    maps: 'Χάρτης',
    municipalities: 'Δήμοι',
    environmentMinistry: 'Υπ. Περιβάλλοντος',
    contracts: 'Συμβάσεις',
    diavgeia: 'Διαύγεια',
    analysis: 'Ανάλυση',
    new: 'New',
    about: 'Σχετικά',
    terms: 'Όροι χρήσης',
    privacy: 'Απόρρητο & cookies',
    cookiePreferences: 'Ρυθμίσεις cookies',
  },
  layout: {
    brandAlt: 'FireWatch — παρατηρητήριο για τις δασικές πυρκαγιές',
    lastUpdated: 'Τελευταία ενημέρωση',
    mainNavigation: 'Κύρια πλοήγηση',
  },
  source: {
    originalGreek: 'Πρωτότυπο στα ελληνικά',
    translatedFromGreek: 'Μετάφραση από τα ελληνικά',
  },
} as const

// The English dictionary must contain the same namespaces and keys, while
// allowing different text. New Greek entries require an English counterpart.
export type TranslationMessages = {
  [Namespace in keyof typeof el]: {
    [Key in keyof typeof el[Namespace]]: string
  }
}
