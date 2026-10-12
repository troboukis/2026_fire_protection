import { describe, expect, it } from 'vitest'
import { summarizeContractAmounts } from './contractAmount'

describe('summarizeContractAmounts', () => {
  it('adds decimal amounts as integer cents without floating-point drift', () => {
    expect(summarizeContractAmounts([
      { amount_without_vat: 0.1 },
      { amount_without_vat: 0.2 },
      { amount_without_vat: 12_345.67 },
    ])).toEqual({ totalCents: 1_234_597, missingCount: 0 })
  })

  it('preserves zero and negative amounts and rounds each contract to cents', () => {
    expect(summarizeContractAmounts([
      { amount_without_vat: 0 },
      { amount_without_vat: 100 },
      { amount_without_vat: -25.5 },
      { amount_without_vat: 1.005 },
      { amount_without_vat: -1.005 },
    ])).toEqual({ totalCents: 7_450, missingCount: 0 })
    expect(summarizeContractAmounts([
      { amount_without_vat: 1.005 },
      { amount_without_vat: 1.005 },
    ])).toEqual({ totalCents: 202, missingCount: 0 })
  })

  it('counts absent or unusable amounts once per contract', () => {
    expect(summarizeContractAmounts([
      { amount_without_vat: null },
      { amount_without_vat: Number.NaN },
      { amount_without_vat: Number.POSITIVE_INFINITY },
      { amount_without_vat: Number.NEGATIVE_INFINITY },
      { amount_without_vat: Number.MAX_SAFE_INTEGER },
      { amount_without_vat: 0 },
      { amount_without_vat: 42.5 },
    ])).toEqual({ totalCents: 4_250, missingCount: 5 })
  })

  it('includes contracts on every page and all supplied statuses', () => {
    const rows = Array.from({ length: 51 }, (_, index) => ({
      amount_without_vat: 0.01,
      cancelled: index === 0,
      is_modified: index === 1,
    }))
    expect(summarizeContractAmounts(rows)).toEqual({ totalCents: 51, missingCount: 0 })
  })

  it('returns zero totals when there are no matching contracts', () => {
    expect(summarizeContractAmounts([])).toEqual({ totalCents: 0, missingCount: 0 })
  })
})
