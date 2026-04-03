const GEMI_COMPANY_URL_CACHE = new Map<string, Promise<string>>()

type GemiCompanyUrlResponse = {
  gemiNumber?: string
  error?: string
}

export function normalizeAfm(value: unknown): string | null {
  if (value == null) return null
  const match = String(value).match(/\b\d{9}\b/)
  return match?.[0] ?? null
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
    const gemiNumber = await getGemiNumberByAfm(normalizedAfm)
    const companyUrl = `https://publicity.businessportal.gr/company/${gemiNumber}`
    console.log(companyUrl)
    if (popup && !popup.closed) popup.location.replace(companyUrl)
    else window.open(companyUrl, '_blank', 'noopener,noreferrer')
  } catch (error) {
    console.error('[gemi] lookup failed', error)
    if (popup && !popup.closed) {
      popup.document.title = 'Αδυναμία φόρτωσης ΓΕΜΗ'
      popup.document.body.innerHTML = `
        <main style="font-family: system-ui, sans-serif; padding: 24px; line-height: 1.5;">
          <h1 style="margin: 0 0 12px; font-size: 20px;">Δεν βρέθηκε εταιρεία στο ΓΕΜΗ</h1>
          <p style="margin: 0;">Δεν κατέστη δυνατή η εύρεση εταιρείας${companyName ? ` για ${companyName}` : ''} με ΑΦΜ ${normalizedAfm}.</p>
        </main>
      `
      return
    }
    throw error
  }
}
