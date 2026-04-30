import { supabase } from './supabase'

const GEMI_COMPANY_URL_CACHE = new Map<string, Promise<string>>()
const BENEFICIARY_GEMI_CACHE = new Map<string, Promise<string | null>>()
const GEMI_NOT_FOUND = '__GEMI_NOT_FOUND__'
const GEMI_PENDING = '__GEMI_PENDING__'

type GemiCompanyUrlResponse = {
  gemiNumber?: string
  error?: string
}

export function normalizeAfm(value: unknown): string | null {
  if (value == null) return null
  const text = String(value)
  for (const part of text.split(/[|,;]/)) {
    const digits = part.replace(/\D/g, '')
    if (digits.length >= 8) return digits
  }
  const digits = text.replace(/\D/g, '')
  return digits.length >= 8 ? digits : null
}

function normalizeGemi(value: unknown): string | null {
  if (value == null) return null
  const gemi = String(value).trim()
  if (!gemi) return null
  if (gemi === '-1') return GEMI_NOT_FOUND
  return gemi
}

function buildGemiCompanyUrl(gemiNumber: string): string {
  return `https://publicity.businessportal.gr/company/${encodeURIComponent(gemiNumber)}`
}

async function resolveGemiNumberByAfm(afm: string): Promise<string> {
  const response = await fetch(`/api/gemi/company-url?afm=${encodeURIComponent(afm)}`, {
    method: 'GET',
    headers: { Accept: 'application/json' },
  })

  const rawPayload = await response.text()
  let payload: GemiCompanyUrlResponse

  try {
    payload = JSON.parse(rawPayload) as GemiCompanyUrlResponse
  } catch {
    throw new Error(`GEMI lookup returned a non-JSON response (${response.status})`)
  }

  if (!response.ok || !payload.gemiNumber) {
    throw new Error(payload.error ?? `GEMI lookup failed with status ${response.status}`)
  }

  return payload.gemiNumber
}

export function getGemiNumberByAfm(afm: string): Promise<string> {
  const normalizedAfm = normalizeAfm(afm)
  if (!normalizedAfm) {
    return Promise.reject(new Error('Missing beneficiary AFM'))
  }

  if (!GEMI_COMPANY_URL_CACHE.has(normalizedAfm)) {
    GEMI_COMPANY_URL_CACHE.set(
      normalizedAfm,
      resolveGemiNumberByAfm(normalizedAfm).catch((error) => {
        GEMI_COMPANY_URL_CACHE.delete(normalizedAfm)
        throw error
      }),
    )
  }

  return GEMI_COMPANY_URL_CACHE.get(normalizedAfm) as Promise<string>
}

async function resolveStoredGemiNumberByAfm(afm: string): Promise<string | null> {
  const { data, error } = await supabase
    .from('beneficiary')
    .select('gemi')
    .eq('beneficiary_vat_number', afm)
    .maybeSingle()

  if (error) {
    console.error('[gemi] stored GEMI lookup failed', error)
    return GEMI_PENDING
  }

  return data ? normalizeGemi(data.gemi) ?? GEMI_PENDING : GEMI_PENDING
}

export function getStoredGemiNumberByAfm(afm: string): Promise<string | null> {
  const normalizedAfm = normalizeAfm(afm)
  if (!normalizedAfm) {
    return Promise.resolve(null)
  }

  if (!BENEFICIARY_GEMI_CACHE.has(normalizedAfm)) {
    BENEFICIARY_GEMI_CACHE.set(
      normalizedAfm,
      resolveStoredGemiNumberByAfm(normalizedAfm).catch((error) => {
        BENEFICIARY_GEMI_CACHE.delete(normalizedAfm)
        throw error
      }),
    )
  }

  return BENEFICIARY_GEMI_CACHE.get(normalizedAfm) as Promise<string | null>
}

export async function openGemiCompanyPageByAfm(afm: string, companyName?: string): Promise<void> {
  const normalizedAfm = normalizeAfm(afm)
  if (!normalizedAfm) return

  const popup = typeof window !== 'undefined' ? window.open('', '_blank') : null

  if (popup) {
    popup.document.title = 'Αναζήτηση ΓΕΜΗ'
    popup.document.body.innerHTML = `
      <main style="font-family: system-ui, sans-serif; padding: 24px; line-height: 1.5;">
        <h1 style="margin: 0 0 12px; font-size: 20px;">Αναζήτηση στο ΓΕΜΗ…</h1>
        <p style="margin: 0;">${companyName ? `Αναζητείται η εταιρεία ${companyName}.` : 'Αναζητείται η εταιρεία με βάση το ΑΦΜ.'}</p>
      </main>
    `
  }

  try {
    const storedGemiNumber = await getStoredGemiNumberByAfm(normalizedAfm)
    if (storedGemiNumber === GEMI_PENDING) {
      throw new Error(`Δεν έχει εντοπιστεί η εταιρεία με ΑΦΜ ${normalizedAfm} στο ΓΕΜΙ. Δοκιμάστε λίγο αργότερα.`)
    }
    if (storedGemiNumber === GEMI_NOT_FOUND) {
      throw new Error(`No GEMI company found for AFM ${normalizedAfm}`)
    }
    const gemiNumber = storedGemiNumber ?? await getGemiNumberByAfm(normalizedAfm)
    const companyUrl = buildGemiCompanyUrl(gemiNumber)
    if (popup && !popup.closed) popup.location.replace(companyUrl)
    else window.open(companyUrl, '_blank', 'noopener,noreferrer')
  } catch (error) {
    console.error('[gemi] lookup failed', error)
    if (popup && !popup.closed) {
      popup.document.title = 'Αδυναμία φόρτωσης ΓΕΜΗ'
      const message = error instanceof Error && error.message.startsWith('Δεν έχει εντοπιστεί')
        ? error.message
        : `Δεν κατέστη δυνατή η εύρεση εταιρείας${companyName ? ` για ${companyName}` : ''} με ΑΦΜ ${normalizedAfm}.`
      popup.document.body.innerHTML = `
        <main style="font-family: system-ui, sans-serif; padding: 24px; line-height: 1.5;">
          <h1 style="margin: 0 0 12px; font-size: 20px;">Δεν βρέθηκε εταιρεία στο ΓΕΜΗ</h1>
          <p style="margin: 0;">${message}</p>
        </main>
      `
      return
    }
    throw error
  }
}
