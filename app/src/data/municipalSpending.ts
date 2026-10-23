import snapshot from '../features/analysis/yearly-report/data/yearlyReport2026.generated.json'

// Both pages use the same reporting period and the same amounts without VAT.
// payments.municipal contains only valid municipal finalizations with no contract link,
// including no links to contracts signed in an earlier year.
const contractsAmount = snapshot.contracts.by_group.find(row => row.group === 'Δήμοι')?.amount ?? 0
const unlinkedPaymentsAmount = snapshot.payments.municipal.amount

export const municipalSpending = {
  year: snapshot.meta.report_year,
  periodEnd: snapshot.meta.period_end,
  contractsAmount,
  unlinkedPaymentsAmount,
  unlinkedPaymentsCount: snapshot.payments.municipal.payments,
  totalAmount: Math.round((contractsAmount + unlinkedPaymentsAmount) * 100) / 100,
}
