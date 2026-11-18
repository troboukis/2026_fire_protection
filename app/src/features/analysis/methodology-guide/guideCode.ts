import type { OverviewUi } from '../../../i18n/overview'

// Only explanatory comments, the example key placeholder and print label are
// localized. Python identifiers, API URLs, parameters and numeric values stay exact.
export const CODE_TEXTS = [
  'κωδικός χώρας:', 'εγγραφές ανά σελίδα', 'επόμενη σελίδα ή', 'πυρκαγιές',
  'ΤΟ_ΚΛΕΙΔΙ_ΣΑΣ', 'δωρεάν:', 'ή', 'δυτικά, νότια, ανατολικά, βόρεια (εδώ: Ελλάδα)',
  'τελευταίες 1 έως 10 ημέρες', 'τα αποτελέσματα του EFFIS από τον πρώτο κώδικα',
  'όρια δήμων της χώρας σας', 'καμένα εκτάρια μέσα σε κάθε δήμο',
]
const pattern = new RegExp([...CODE_TEXTS].sort((a, b) => b.length - a.length)
  .map(text => text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|'), 'g')

export function guideCode(code: string, ui: Pick<OverviewUi, 'language'> & ((source: string) => string)): string {
  return ui.language === 'el' ? code : code.replace(pattern, source => ui(source))
}
