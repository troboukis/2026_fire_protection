import { supabase } from './supabase'

type GemiCarrier = {
  beneficiary_vat_number?: string | null
  beneficiary_gemi?: string | null
}

function cleanText(value: unknown): string | null {
  if (value == null) return null
  const text = String(value).trim()
  if (!text || text.toLowerCase() === 'nan' || text.toLowerCase() === 'none') return null
  return text
}

function splitBeneficiaryVat(value: unknown): string[] {
  const text = cleanText(value)
  if (!text) return []
  return text
    .split(/[|,;]/)
    .map((part) => cleanText(part))
    .filter((part): part is string => Boolean(part))
}

export async function loadBeneficiaryGemiByVat(vatNumbers: string[], signal?: AbortSignal): Promise<Map<string, string>> {
  const normalizedVatNumbers = Array.from(new Set(vatNumbers.flatMap(splitBeneficiaryVat)))
  const result = new Map<string, string>()

  for (let index = 0; index < normalizedVatNumbers.length; index += 200) {
    const vats = normalizedVatNumbers.slice(index, index + 200)
    const query = supabase
      .from('beneficiary')
      .select('beneficiary_vat_number, gemi')
      .in('beneficiary_vat_number', vats)
    const { data } = await (signal ? query.abortSignal(signal) : query)

    for (const row of ((data ?? []) as Array<{ beneficiary_vat_number: string | null; gemi: string | null }>)) {
      const vat = cleanText(row.beneficiary_vat_number)
      const gemi = cleanText(row.gemi)
      if (vat && gemi && gemi !== '-1') result.set(vat, gemi)
    }
  }

  return result
}

export async function attachBeneficiaryGemi<T extends GemiCarrier>(rows: T[], signal?: AbortSignal): Promise<T[]> {
  const gemiByVat = await loadBeneficiaryGemiByVat(
    rows.flatMap((row) => splitBeneficiaryVat(row.beneficiary_vat_number)),
    signal,
  )

  return rows.map((row) => {
    const gemis = splitBeneficiaryVat(row.beneficiary_vat_number)
      .map((vat) => gemiByVat.get(vat))
      .filter((gemi): gemi is string => Boolean(gemi))
    return {
      ...row,
      beneficiary_gemi: Array.from(new Set(gemis)).join(' | ') || row.beneficiary_gemi || null,
    }
  })
}
