import { describe, expect, it } from 'vitest'
import { buildCpvFilterIndex, matchesContractCpv } from './cpvFilter'
import { normalizeSearchText } from './searchQuery'
import { summarizeContractAmounts } from './contractAmount'

const index = buildCpvFilterIndex([
  { cpv_key: '77312000-0', cpv_value: 'Απομάκρυνση ζιζανίων', procurement_id: 1 },
  { cpv_key: '77312000-0', cpv_value: 'Απομάκρυνση ζιζανίων', procurement_id: 2 },
  { cpv_key: ' 75251100-1 ', cpv_value: 'Υπηρεσίες πυρόσβεσης', procurement_id: 1 },
  { cpv_key: '75251100-1', cpv_value: null, procurement_id: 3 },
])

describe('CPV contract filter', () => {
  it('groups repeated codes and matches every CPV of a contract by exact code', () => {
    expect(index.size).toBe(2)
    expect(matchesContractCpv(1, '77312000-0', index)).toBe(true)
    expect(matchesContractCpv(1, '75251100-1', index)).toBe(true)
    expect(matchesContractCpv(2, '75251100-1', index)).toBe(false)
    expect(matchesContractCpv(3, '75251100-1', index)).toBe(true)
    expect(matchesContractCpv(1, '77312000', index)).toBe(false)
    expect(matchesContractCpv(999, '', index)).toBe(true)
  })

  it('offers code and description together for normalized searching', () => {
    const label = index.get('77312000-0')!.label
    expect(label).toBe('77312000-0 · Απομάκρυνση ζιζανίων')
    for (const query of ['773120', 'ζιζανίων', 'ΖΙΖΑΝΙΩΝ', 'απομακρυνση']) {
      expect(normalizeSearchText(label).includes(normalizeSearchText(query))).toBe(true)
    }
  })

  it('updates the amount summary from all matching contracts without double counting multi-CPV contracts', () => {
    const contracts = [
      { id: 1, amount_without_vat: 100 },
      { id: 2, amount_without_vat: 200 },
      { id: 3, amount_without_vat: 400 },
    ]
    const filtered = contracts.filter(row => matchesContractCpv(row.id, '75251100-1', index))
    expect(filtered.map(row => row.id)).toEqual([1, 3])
    expect(summarizeContractAmounts(filtered)).toEqual({ totalCents: 50_000, missingCount: 0 })
  })
})
