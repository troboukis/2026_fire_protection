import { greekOverviewUi, type OverviewUi } from './overview'

// The monthly key/Greek source label still belongs to the aggregate data.
// Only its displayed month is localized; the original year stays unchanged.
export function analysisMonthLabel(value: string, ui: OverviewUi = greekOverviewUi): string {
  if (ui.language === 'el') return value
  return value.replace(/^([^ ]+)(?=\s+['’]\d{2})/, month => ui(month))
}
