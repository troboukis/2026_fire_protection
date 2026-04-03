const SEARCH_URL = 'https://publicity.businessportal.gr/api/search'

const DEFAULT_HEADERS = {
  Accept: 'application/json, text/plain, */*',
  'Accept-Language': 'en-US,en;q=0.9,el;q=0.8',
  'Content-Type': 'application/json',
  Origin: 'https://publicity.businessportal.gr',
  Referer: 'https://publicity.businessportal.gr/',
  'User-Agent': 'Mozilla/5.0',
}

export function normalizeAfm(value) {
  const match = String(value ?? '').match(/\b\d{9}\b/)
  return match?.[0] ?? null
}

function buildSearchPayload(afm, language = 'el') {
  return {
    dataToBeSent: {
      inputField: afm,
      city: null,
      postcode: null,
      legalType: [],
      status: [],
      suspension: [],
      category: [],
      specialCharacteristics: [],
      employeeNumber: [],
      armodiaGEMI: [],
      kad: [],
      recommendationDateFrom: null,
      recommendationDateTo: null,
      closingDateFrom: null,
      closingDateTo: null,
      alterationDateFrom: null,
      alterationDateTo: null,
      person: [],
      personrecommendationDateFrom: null,
      personrecommendationDateTo: null,
      radioValue: 'all',
      places: [],
    },
    token: null,
    language,
  }
}

export async function getCompanyUrlByAfm(afm) {
  const response = await fetch(SEARCH_URL, {
    method: 'POST',
    headers: DEFAULT_HEADERS,
    body: JSON.stringify(buildSearchPayload(afm)),
  })

  if (!response.ok) {
    throw new Error(`GEMI search failed with status ${response.status}`)
  }

  const data = await response.json()
  const hits = Array.isArray(data?.company?.hits) ? data.company.hits : []
  const exactHit = hits.find((hit) => hit?.afm === afm)

  if (!exactHit) {
    throw new Error(`No exact AFM match found for ${afm}`)
  }

  const gemiNumber = String(exactHit.gemiNumber ?? '').trim()
  if (!gemiNumber) {
    throw new Error(`Search hit for AFM ${afm} did not include gemiNumber`)
  }

  return {
    gemiNumber,
    url: `https://publicity.businessportal.gr/company/${gemiNumber}`,
  }
}
