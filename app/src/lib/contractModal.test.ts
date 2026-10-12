import { beforeEach, describe, expect, it, vi } from 'vitest'
import { loadContractModal } from './contractModal'

const { from, results, filters } = vi.hoisted(() => ({
  from: vi.fn(),
  results: {} as Record<string, { data: unknown; error: unknown }>,
  filters: [] as Array<{ table: string; key: string; value: unknown }>,
}))

vi.mock('./supabase', () => ({ supabase: { from } }))

beforeEach(() => {
  filters.length = 0
  from.mockReset()
  for (const table of Object.keys(results)) delete results[table]
  results.procurement = {
    data: {
      id: 42,
      title: 'Καθαρισμός βλάστησης',
      reference_number: '26SYMV018123456',
      short_descriptions: 'Εργασίες πυροπροστασίας | Δεύτερη περιγραφή',
      submission_at: '2026-05-10',
      contract_signed_date: '2026-05-09',
      organization_key: 'org-1',
      contract_budget: 900,
      budget: 800,
      contract_related_ada: 'ΑΒΓΔ-123',
      prev_reference_no: '25SYMV017123456',
    },
    error: null,
  }
  results.payment = {
    data: [
      { beneficiary_name: 'Ανάδοχος Α', beneficiary_vat_number: '123456789', amount_without_vat: 100, amount_with_vat: 124 },
      { beneficiary_name: 'Ανάδοχος Α', beneficiary_vat_number: '123456789', amount_without_vat: 200, amount_with_vat: 248 },
    ],
    error: null,
  }
  results.cpv = {
    data: [
      { cpv_key: '77312000', cpv_value: 'Απομάκρυνση ζιζανίων' },
      { cpv_key: '77312000', cpv_value: 'Απομάκρυνση ζιζανίων' },
    ],
    error: null,
  }
  results.organization = { data: [{ organization_normalized_value: 'Φορέας Α' }], error: null }
  from.mockImplementation((table: string) => {
    const query = {
      select: () => query,
      eq: (key: string, value: unknown) => { filters.push({ table, key, value }); return query },
      single: () => Promise.resolve(results[table]),
      limit: () => Promise.resolve(results[table]),
      then: (resolve: (value: unknown) => unknown) => Promise.resolve(results[table]).then(resolve),
    }
    return query
  })
})

describe('loadContractModal', () => {
  it('loads the selected contract by id with full amounts, deduplicated CPVs and references', async () => {
    const contract = await loadContractModal({ id: 42 })
    expect(filters).toContainEqual({ table: 'procurement', key: 'id', value: 42 })
    expect(filters).toContainEqual({ table: 'payment', key: 'procurement_id', value: 42 })
    expect(contract).toMatchObject({
      id: '42',
      what: 'Καθαρισμός βλάστησης',
      referenceNumber: '26SYMV018123456',
      previousReferenceNumber: '25SYMV017123456',
      why: 'Εργασίες πυροπροστασίας',
      beneficiary: 'Ανάδοχος Α',
      beneficiaryVat: '123456789',
      cpvItems: [{ code: '77312000', label: 'Απομάκρυνση ζιζανίων' }],
    })
    expect(contract.withoutVatAmount).toBe((300).toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }))
    expect(contract.withVatAmount).toBe((372).toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }))
  })

  it('uses the contract budget when there are no payment amounts', async () => {
    results.payment.data = []
    const contract = await loadContractModal({ id: 42 })
    expect(contract.withoutVatAmount).toBe((900).toLocaleString('el-GR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }))
    expect(contract.withVatAmount).toBe('—')
  })

  it('rejects a missing contract and failed related-data requests', async () => {
    const savedProcurement = results.procurement.data
    results.procurement.data = null
    await expect(loadContractModal({ id: 42 })).rejects.toThrow('Contract not found')
    results.procurement.data = savedProcurement
    results.payment.error = new Error('Payment lookup failed')
    await expect(loadContractModal({ id: 42 })).rejects.toThrow('Payment lookup failed')
  })
})
