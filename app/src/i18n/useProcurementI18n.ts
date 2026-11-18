import { useAppI18n } from './useAppI18n'
import messages from './catalogs/procurement.el.json'
import { taxonomyUi } from './taxonomy'

type MessageKey = keyof typeof messages

export function useProcurementI18n() {
  const app = useAppI18n()
  const p = (key: MessageKey, values: Record<string, string | number> = {}) => app.t(`procurement:${key}`, values)
  const taxonomy = taxonomyUi(app.language)
  const category = taxonomy.label
  const periodLabel = (from: string, to: string) => {
    if (from && to) return `${app.format.date(from)} - ${app.format.date(to)}`
    if (from) return p('periodFrom', { date: app.format.date(from) })
    if (to) return p('periodTo', { date: app.format.date(to) })
    return p('allDates')
  }
  return { ...app, ...taxonomy, p, category, periodLabel }
}
