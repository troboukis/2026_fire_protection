import { describe, expect, it } from 'vitest'
import { REPORT_YEAR, amount, contracts, count, mil, milText, minimumMillions, pct, pctText, report, signedPct, ytd } from './reportData'

const close = (a: number, b: number, tolerance = 0.05) => expect(Math.abs(a - b)).toBeLessThanOrEqual(tolerance)
const sum = (values: number[]) => values.reduce((total, value) => total + value, 0)

describe('yearly report snapshot arithmetic', () => {
  it('builds the total from contracts and unlinked payments', () => {
    const t = report.total
    close(t.amount, t.contracts_amount + t.payments_amount)
    close(t.contracts_amount, contracts.amount)
    expect(t.contracts).toBe(contracts.contracts)
    close(t.payments_amount, report.payments.amount)
    expect(t.payments).toBe(report.payments.counted)
  })

  it('keeps linked payments out of the total', () => {
    const p = report.payments
    expect(p.counted + p.linked).toBe(p.valid)
    close(p.amount + p.linked_amount, p.valid_amount, 1)
    expect(sum(Object.values(p.linked_by_method).map(row => row.payments))).toBe(p.linked)
    expect(p.vat_removed + p.vat_free).toBe(p.counted)
    expect(sum(p.municipal.by_month.map(row => row.payments))).toBe(p.municipal.payments)
    close(sum(p.municipal.by_month.map(row => row.amount)), p.municipal.amount, 1)
  })

  it('reconciles payments by category', () => {
    const p = report.payments
    close(sum(p.by_category.map(row => row.amount)), p.amount, 1)
    expect(sum(p.by_category.map(row => row.payments))).toBe(p.counted)
    expect(p.by_category.some(row => row.category === 'cancellation')).toBe(false)
  })

  it('keeps the contract breakdowns consistent with the total', () => {
    for (const rows of [contracts.procedures, contracts.contract_types]) {
      close(sum(rows.map(row => row.amount)), contracts.amount, 1)
      expect(sum(rows.map(row => row.count))).toBe(contracts.contracts)
      close(sum(rows.map(row => row.share_amount_pct ?? 0)), 100, 0.1)
    }
    close(sum(report.relevance.map(row => row.amount)), contracts.amount, 1)
    close(sum(contracts.by_group.map(row => row.amount)), contracts.amount, 1)
    expect(contracts.top_cpvs.length).toBeLessThanOrEqual(5)
    const top10 = report.contractors.top10
    expect(top10.length).toBeLessThanOrEqual(10)
    close(sum(top10.map(row => row.share_pct ?? 0)), report.contractors.top10_share_pct ?? 0, 0.1)
    expect(top10.every((row, i) => i === 0 || row.amount <= top10[i - 1].amount)).toBe(true)
    const bins = contracts.direct_awards.bins
    expect(sum(bins.map(bin => bin.count))).toBe(contracts.direct_awards.count)
    expect(bins.find(bin => bin.lo === 25_000)?.count).toBe(contracts.direct_awards.from_25k_to_30k)
    expect(bins.find(bin => bin.lo === 20_000)?.count).toBe(contracts.direct_awards.from_20k_to_25k)
    expect(bins.every(bin => bin.hi === null || bin.hi - bin.lo === 5_000)).toBe(true)
    expect(ytd(REPORT_YEAR).contracts).toBe(contracts.contracts)
    close(ytd(REPORT_YEAR).amount, contracts.amount)
  })

  it('covers 1 January up to the snapshot date, capped at 31 December', () => {
    const { period_start, period_end, snapshot_date, year_complete } = report.meta
    expect(period_start).toBe(`${REPORT_YEAR}-01-01`)
    expect(period_end <= `${REPORT_YEAR}-12-31`).toBe(true)
    expect(period_end).toBe(year_complete ? `${REPORT_YEAR}-12-31` : snapshot_date)
    expect(report.meta.latest_contract_signed <= period_end).toBe(true)
  })
})

describe('Greek number formatting', () => {
  it('formats amounts, shares and small counts', () => {
    expect(mil(173_140_768.42)).toBe('173,1 εκατ. €')
    expect(minimumMillions(149_755_526.49)).toBe('149,7')
    expect(amount(738_427)).toBe('738,4 χιλ. €')
    expect(amount(1_761_904.76)).toBe('1,8 εκατ. €')
    expect(pct(49.51)).toBe('49,5%')
    expect(pct(-41.47)).toBe('−41,5%')
    expect(pct(-0.01)).toBe('0,0%')
    expect(signedPct(12)).toBe('+12,0%')
    expect(signedPct(-53.42)).toBe('−53,4%')
    expect(milText(21_578_093.51)).toBe('21,6 εκατ. ευρώ')
    expect(milText(3_004_000)).toBe('3,0 εκατ. ευρώ')
    expect(milText(47_500_000)).toBe('47,5 εκατ. ευρώ')
    expect(pctText(11.98)).toBe('12,0%')
    expect(pctText(12.735)).toBe('12,7%')
    expect(count(2, true)).toBe('δύο')
    expect(count(12)).toBe('12')
  })
})
