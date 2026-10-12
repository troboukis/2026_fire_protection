export function summarizeContractAmounts(
  rows: readonly { amount_without_vat: number | null }[],
): { totalCents: number; missingCount: number } {
  let totalCents = 0
  let missingCount = 0

  for (const row of rows) {
    const amount = row.amount_without_vat
    if (amount == null || !Number.isFinite(amount)) {
      missingCount += 1
      continue
    }

    const cents = Math.sign(amount) * Math.round((Math.abs(amount) + Number.EPSILON) * 100)
    if (!Number.isSafeInteger(cents)) {
      missingCount += 1
      continue
    }

    totalCents += cents
  }

  return { totalCents, missingCount }
}
