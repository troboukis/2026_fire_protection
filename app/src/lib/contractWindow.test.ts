import { describe, expect, it } from 'vitest'

import { getContractYearAnchorDate, isContractActiveInYear, isContractActiveOnDate } from './contractWindow'

describe('contractWindow', () => {
  it('includes contracts signed in the selected year regardless of start date', () => {
    expect(isContractActiveInYear({
      contract_signed_date: '2026-04-01',
      start_date: '2026-05-01',
      end_date: '2026-10-31',
      no_end_date: false,
    }, 2026)).toBe(true)
  })

  it('includes contracts signed in the selected year even without an explicit end date', () => {
    expect(isContractActiveInYear({
      contract_signed_date: '2026-04-01',
      no_end_date: false,
    }, 2026)).toBe(true)
  })

  it('includes contracts signed in a previous year when their explicit end date falls inside the selected year', () => {
    expect(isContractActiveInYear({
      contract_signed_date: '2025-10-14',
      end_date: '2026-04-30',
      no_end_date: false,
    }, 2026)).toBe(true)
  })

  it('excludes open-ended contracts signed in a previous year', () => {
    expect(isContractActiveInYear({
      contract_signed_date: '2025-06-01',
      no_end_date: true,
    }, 2026)).toBe(false)
  })

  it('excludes previous-year contracts without an explicit end date', () => {
    expect(isContractActiveInYear({
      contract_signed_date: '2025-06-01',
      no_end_date: false,
    }, 2026)).toBe(false)
  })

  it('includes old contracts whose explicit end date is after the selected year has started', () => {
    expect(isContractActiveInYear({
      contract_signed_date: '2025-06-01',
      end_date: '2027-08-31',
      no_end_date: false,
    }, 2026)).toBe(true)
  })

  it('excludes old contracts whose explicit end date is before the selected year', () => {
    expect(isContractActiveInYear({
      contract_signed_date: '2025-06-01',
      end_date: '2025-12-31',
      no_end_date: false,
    }, 2026)).toBe(false)
  })

  it('anchors already-active carryover contracts to the start of the year', () => {
    expect(getContractYearAnchorDate({
      contract_signed_date: '2025-10-14',
      end_date: '2026-04-30',
      no_end_date: false,
    }, 2026)).toBe('2026-01-01')
  })

  it('anchors previously signed carryover contracts to the start of the year', () => {
    expect(getContractYearAnchorDate({
      contract_signed_date: '2025-12-20',
      start_date: '2026-02-15',
      end_date: '2026-05-01',
      no_end_date: false,
    }, 2026)).toBe('2026-01-01')
  })

  it('treats specific-day checks as the selected year definition', () => {
    expect(isContractActiveOnDate({
      contract_signed_date: '2025-10-14',
      end_date: '2026-02-15',
      no_end_date: false,
    }, '2026-03-01')).toBe(true)
  })
})
