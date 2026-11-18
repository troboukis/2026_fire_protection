import { greekOverviewUi, type OverviewUi } from '../../../i18n/overview'
import * as greek from './reportData'

const original = {
  num: greek.num, decimal: greek.decimal, mil: greek.mil, milText: greek.milText,
  amount: greek.amount, minimumMillions: greek.minimumMillions, eur: greek.eur,
  pct: greek.pct, pctText: greek.pctText, signedPct: greek.signedPct,
  count: greek.count, longDate: greek.longDate, shortDate: greek.shortDate,
  dayMonth: greek.dayMonth, listJoin: greek.listJoin,
}

// Display only: the snapshot, arithmetic, thresholds and precision stay unchanged.
export function createReportFormatters(ui: OverviewUi = greekOverviewUi): typeof original {
  if (ui.language === 'el') return original
  const numbers = new Intl.NumberFormat(ui.locale)
  const decimals = new Intl.NumberFormat(ui.locale, { minimumFractionDigits: 1, maximumFractionDigits: 1 })
  const num = (value: number) => numbers.format(value)
  const decimal = (value: number) => decimals.format(value)
  const mil = (value: number) => ui('{{v0}} εκατ. €', { v0: decimal(value / 1_000_000) })
  const milText = (value: number) => ui('{{v0}} εκατ. ευρώ', { v0: decimal(value / 1_000_000) })
  const eur = (value: number) => `${decimal(value)} €`
  const amount = (value: number) => Math.abs(value) >= 1_000_000 ? mil(value)
    : Math.abs(value) >= 1_000 ? ui('{{v0}} χιλ. €', { v0: decimal(value / 1_000) }) : eur(value)
  const pct = (value: number | null | undefined) => {
    if (value === null || value === undefined || !Number.isFinite(value)) return '—'
    const text = decimal(Math.abs(value))
    return `${value < 0 && text.replace(/[0,.]/g, '') !== '' ? '−' : ''}${text}%`
  }
  return {
    num, decimal, mil, milText, amount, eur, pct, pctText: pct,
    minimumMillions: value => decimal(Math.floor(value / 100_000) / 10),
    signedPct: value => {
      const text = pct(value)
      return value > 0 && text.replace(/[0,.%]/g, '') !== '' ? `+${text}` : text
    },
    count: (value, feminine = false) => value >= 1 && value <= 10 ? ui(greek.count(value, feminine)) : num(value),
    longDate: iso => new Date(`${iso}T12:00:00`).toLocaleDateString(ui.locale, { day: 'numeric', month: 'long', year: 'numeric' }),
    shortDate: iso => new Date(`${iso}T12:00:00`).toLocaleDateString(ui.locale),
    dayMonth: greek.dayMonth,
    listJoin: items => items.length <= 1 ? items.join('') : `${items.slice(0, -1).join(', ')} and ${items[items.length - 1]}`,
  }
}
