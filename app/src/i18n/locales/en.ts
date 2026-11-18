import analysisYearly3 from '../catalogs/analysisYearly3.en.json'
import analysisYearly2 from '../catalogs/analysisYearly2.en.json'
import analysisYearly1 from '../catalogs/analysisYearly1.en.json'
import analysisAntinero from '../catalogs/analysisAntinero.en.json'
import analysisStatistics from '../catalogs/analysisStatistics.en.json'
import analysisCatalog from '../catalogs/analysisCatalog.en.json'
import organizationNames5 from '../catalogs/organizationNames5.en.json'
import organizationNames4 from '../catalogs/organizationNames4.en.json'
import organizationNames3 from '../catalogs/organizationNames3.en.json'
import organizationNames2 from '../catalogs/organizationNames2.en.json'
import organizationNames1 from '../catalogs/organizationNames1.en.json'
import regionNames from '../catalogs/regionNames.en.json'
import municipalityNames from '../catalogs/municipalityNames.en.json'
import cpvServices from '../catalogs/cpvServices.en.json'
import cpvSuppliesWorks from '../catalogs/cpvSuppliesWorks.en.json'
import taxonomy from '../catalogs/taxonomy.en.json'
import type { TranslationMessages } from './el'
import procurement from '../catalogs/procurement.en.json'
import overview from '../catalogs/overview.en.json'
import places from '../catalogs/places.en.json'
import ministry from '../catalogs/ministry.en.json'
import publicInfo from '../catalogs/publicInfo.en.json'
import mediaTitles from '../catalogs/mediaTitles.en.json'

export const en = {
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
    loading: 'Loading…',
    loadingPage: 'Loading page…',
    loadingData: 'Loading data',
    retrievingLatest: 'Retrieving the latest data.',
    totalUnavailable: 'The total is unavailable.',
    documentsWithoutAmount_one: '{{count, number}} document has no amount',
    documentsWithoutAmount_other: '{{count, number}} documents have no amount',
  },
  navigation: {
    language: 'Language',
    home: 'Home',
    maps: 'Map',
    municipalities: 'Municipalities',
    environmentMinistry: 'Environment ministry',
    contracts: 'Contracts',
    diavgeia: 'Diavgeia',
    analysis: 'Analysis',
    new: 'New',
    about: 'About',
    terms: 'Terms of use',
    privacy: 'Privacy & cookies',
    cookiePreferences: 'Cookie settings',
  },
  layout: {
    brandAlt: 'FireWatch — forest fire observatory',
    lastUpdated: 'Last updated',
    mainNavigation: 'Main navigation',
  },
  source: {
    originalGreek: 'Original in Greek',
    translatedFromGreek: 'Translated from Greek',
  },
} as const satisfies TranslationMessages
