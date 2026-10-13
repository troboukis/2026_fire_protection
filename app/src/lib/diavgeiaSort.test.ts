import { describe, expect, it } from 'vitest'
import { sortDiavgeiaRows } from './diavgeiaSort'

const rows = [
  { id: 1, decision_date: '2026-10-10T12:00:00+03:00', spending_contractors_value: '1.000,00' },
  { id: 2, decision_date: '2026-10-09', spending_contractors_value: "['900,00', '900,00']" },
  { id: 3, decision_date: '2026-10-11', spending_contractors_value: null },
  { id: 4, decision_date: '2026-10-08', spending_contractors_value: '-10,00' },
  { id: 5, decision_date: null, spending_contractors_value: '0,00' },
]

describe('sortDiavgeiaRows', () => {
  it('sorts dates in both directions and keeps missing dates last', () => {
    expect(sortDiavgeiaRows(rows, 'date_desc').map(row => row.id)).toEqual([3, 1, 2, 4, 5])
    expect(sortDiavgeiaRows(rows, 'date_asc').map(row => row.id)).toEqual([4, 2, 1, 3, 5])
  })

  it('sorts numeric document totals, including beneficiary lists, negatives and zero, with missing amounts last', () => {
    expect(sortDiavgeiaRows(rows, 'amount_desc').map(row => row.id)).toEqual([2, 1, 5, 4, 3])
    expect(sortDiavgeiaRows(rows, 'amount_asc').map(row => row.id)).toEqual([4, 5, 1, 2, 3])
  })

  it('compares timestamps across offsets and breaks ties by id without mutating the source', () => {
    const tied = [
      { id: 1, decision_date: '2026-10-10T12:00:00+03:00', spending_contractors_value: '10,00' },
      { id: 2, decision_date: '2026-10-10T09:00:00Z', spending_contractors_value: '10,00' },
      { id: 3, decision_date: 'invalid', spending_contractors_value: 'invalid' },
    ]
    expect(sortDiavgeiaRows(tied, 'date_desc').map(row => row.id)).toEqual([2, 1, 3])
    expect(sortDiavgeiaRows(tied, 'date_asc').map(row => row.id)).toEqual([1, 2, 3])
    expect(sortDiavgeiaRows(tied, 'amount_asc').map(row => row.id)).toEqual([2, 1, 3])
    expect(tied.map(row => row.id)).toEqual([1, 2, 3])
  })

  it('sorts all results before pagination', () => {
    const documents = Array.from({ length: 51 }, (_, id) => ({
      id, decision_date: '2026-10-10', spending_contractors_value: String(id),
    }))
    expect(sortDiavgeiaRows(documents, 'amount_desc').slice(0, 50)[0].id).toBe(50)
  })
})
