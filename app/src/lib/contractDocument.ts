import type { ContractModalContract } from '../components/ContractModal'

const KIMDIS_ATTACHMENT_BASE_URL = 'https://cerpp.eprocurement.gov.gr/khmdhs-opendata/contract/attachment'

export function cleanValue(value: string | null | undefined): string | null {
  const trimmed = String(value ?? '').trim()
  if (!trimmed || trimmed === '—') return null
  return trimmed
}

export function extractKimdisRefNumber(contract: Pick<ContractModalContract, 'referenceNumber' | 'contractNumber'>): string | null {
  const candidates = [cleanValue(contract.referenceNumber), cleanValue(contract.contractNumber)]
  for (const candidate of candidates) {
    if (!candidate) continue
    const match = candidate.match(/\b\d{2}SYMV\d+\b/i)
    if (match) return match[0].toUpperCase()
    if (/^\d{2}SYMV\d+$/i.test(candidate)) return candidate.toUpperCase()
  }
  return null
}

export function buildAttachmentUrl(refNumber: string): string {
  return `${KIMDIS_ATTACHMENT_BASE_URL}/${encodeURIComponent(refNumber)}`
}

export function buildDiavgeiaDocumentUrl(...candidates: Array<string | null | undefined>): string | null {
  const ada = candidates.map(cleanValue).find((value): value is string => Boolean(value))
  if (!ada) return null
  return `https://diavgeia.gov.gr/doc/${encodeURIComponent(ada)}`
}

export async function downloadContractDocument(contract: Pick<ContractModalContract, 'referenceNumber' | 'contractNumber'>): Promise<void> {
  const refNumber = extractKimdisRefNumber(contract)
  if (!refNumber) throw new Error('Δεν βρέθηκε κωδικός ΚΗΜΔΗΣ για το έγγραφο της σύμβασης.')

  const url = buildAttachmentUrl(refNumber)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.target = '_blank'
  anchor.rel = 'noopener noreferrer'
  document.body.appendChild(anchor)
  anchor.click()
  anchor.remove()
}
