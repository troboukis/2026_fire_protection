import { parseDiavgeiaAmountCents } from './diavgeiaAmount'

export type DiavgeiaSort = 'date_desc' | 'date_asc' | 'amount_desc' | 'amount_asc'

type SortableDecision = {
  id: number
  decision_date: string | null
  spending_contractors_value: string | null
}

function compareNullable(a: number | null, b: number | null, ascending: boolean): number {
  if (a == null) return b == null ? 0 : 1
  if (b == null) return -1
  return ascending ? a - b : b - a
}

export function sortDiavgeiaRows<T extends SortableDecision>(rows: readonly T[], sort: DiavgeiaSort): T[] {
  return rows.map((row) => {
    const date = row.decision_date ? Date.parse(row.decision_date) : Number.NaN
    return {
      row,
      date: Number.isFinite(date) ? date : null,
      amount: sort.startsWith('amount_') ? parseDiavgeiaAmountCents(row.spending_contractors_value) : null,
    }
  }).sort((a, b) => {
    if (sort.startsWith('amount_')) {
      const byAmount = compareNullable(a.amount, b.amount, sort === 'amount_asc')
      if (byAmount) return byAmount
    }
    const byDate = compareNullable(a.date, b.date, sort === 'date_asc')
    if (byDate) return byDate
    return sort === 'date_asc' ? a.row.id - b.row.id : b.row.id - a.row.id
  }).map(({ row }) => row)
}
