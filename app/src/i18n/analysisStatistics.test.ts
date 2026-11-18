import { createElement } from 'react'
import { act, create, type ReactTestRenderer, type ReactTestRendererJSON } from 'react-test-renderer'
import { afterEach, describe, expect, it, vi } from 'vitest'
import I18nProvider from './I18nProvider'
import type { AppLanguage } from './languages'
import { getOverviewUi } from './overview'
import { analysisMonthLabel } from './analysisFormatting'
import StatisticalAnalysis from '../features/analysis/statistics/StatisticalAnalysis'
import DirectAwardHistogram from '../features/analysis/statistics/DirectAwardHistogram'
import { BarChart } from '../features/analysis/statistics/AnalysisCharts'
import { buildAnalysisDataFromRows, CURRENT_YEAR, type SectionRow } from '../features/analysis/statistics/analysisData'

const { rpc, logError } = vi.hoisted(() => ({ rpc: vi.fn(), logError: vi.fn() }))
vi.mock('../lib/supabase', () => ({ supabase: { rpc } }))
vi.mock('../lib/logger', () => ({ logError, logWarn: vi.fn() }))
vi.mock('../lib/devView', () => ({ DEV_VIEW_AVAILABLE: false, useDevViewEnabled: () => [false], useDevView: () => ({ enabled: false }) }))

const fixture: SectionRow[] = [
  { signedDate: `${CURRENT_YEAR}-05-01`, effectiveStart: `${CURRENT_YEAR}-05-01`, effectiveEnd: `${CURRENT_YEAR}-06-01`, orgName: 'ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ', authorityLabel: 'ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ', contractType: 'Υπηρεσίες', procedure: 'Απευθείας Ανάθεση', amount: 25000, cpvs: ['Εργαλεία'] },
  { signedDate: `${CURRENT_YEAR}-07-01`, effectiveStart: `${CURRENT_YEAR}-07-01`, effectiveEnd: `${CURRENT_YEAR}-09-01`, orgName: 'ΔΗΜΟΣ ΔΟΞΑΤΟΥ', authorityLabel: 'ΔΗΜΟΣ ΔΟΞΑΤΟΥ', contractType: 'Προμήθειες', procedure: 'Ανοιχτή Διαδικασία', amount: 1500000, cpvs: ['Εξοπλισμός πυρόσβεσης'] },
  { signedDate: `${CURRENT_YEAR - 1}-02-01`, effectiveStart: `${CURRENT_YEAR - 1}-02-01`, effectiveEnd: `${CURRENT_YEAR - 1}-05-01`, orgName: 'ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ', authorityLabel: 'ΠΡΑΣΙΝΟ ΤΑΜΕΙΟ', contractType: 'Έργα', procedure: 'Διαπραγμάτευση', amount: 2000000, cpvs: ['Εργαλεία'] },
]
function content(value: ReactTestRendererJSON | ReactTestRendererJSON[] | string | null): string {
  if (typeof value === 'string') return value
  if (!value) return ''
  if (Array.isArray(value)) return value.map(content).join('')
  return (value.children ?? []).map(content).join('')
}
function view(language: AppLanguage) {
  return createElement(I18nProvider, { language, children: createElement(StatisticalAnalysis) })
}

describe('localized statistical analysis using unchanged procurement data', () => {
  let tree: ReactTestRenderer | undefined
  afterEach(() => {
    if (tree) act(() => tree!.unmount())
    tree = undefined
    vi.unstubAllGlobals()
    vi.clearAllMocks()
  })
  function prepare() {
    vi.stubGlobal('ResizeObserver', class { observe() {} disconnect() {} })
  }

  it('retains amounts, monthly totals, selected period and metric across a language change without refetching', async () => {
    prepare()
    const rows = structuredClone(fixture)
    rpc.mockReturnValue({ abortSignal: () => Promise.resolve({ data: { sectionRows: rows }, error: null }) })
    await act(async () => { tree = create(view('en')) })
    const english = content(tree!.toJSON())
    expect(english).toContain('3 published contracts')
    expect(english).toContain('Green Fund')
    expect(english).toContain('Municipality of Doxato')
    expect(english).toContain('Tools')
    expect(english).toContain('Contract value (excl. VAT)')
    expect(english).not.toMatch(/[Α-Ωα-ω]/)
    expect(tree!.root.findByType(DirectAwardHistogram).props.amounts).toEqual([25000])
    expect(tree!.root.findByType(DirectAwardHistogram).props.totalCount).toBe(1)
    const monthly = structuredClone(tree!.root.findByType(BarChart).props.monthly)
    expect(monthly.reduce((sum: number, point: { total_k: number }) => sum + point.total_k * 1000, 0)).toBe(3525000)
    act(() => tree!.root.find(node => node.type === 'button' && node.children.join('') === 'Number of Contracts').props.onClick())
    act(() => tree!.root.find(node => node.type === 'button' && node.children.join('') === String(CURRENT_YEAR - 1)).props.onClick())
    await act(async () => tree!.update(view('el')))
    expect(tree!.root.findByType(BarChart).props.metric).toBe('count')
    expect(tree!.root.findByType(BarChart).props.monthly).toEqual(monthly)
    expect(tree!.root.findByType(DirectAwardHistogram).props.periodLabel).toBe(String(CURRENT_YEAR - 1))
    expect(tree!.root.findByType(DirectAwardHistogram).props.totalCount).toBe(0)
    expect(content(tree!.toJSON())).toContain('Στατιστική Ανάλυση')
    expect(rpc).toHaveBeenCalledExactlyOnceWith('get_contract_analysis', { p_year_start: 2024 })
    expect(rows).toEqual(fixture)
    const aggregates = buildAnalysisDataFromRows(rows)
    expect(aggregates.totalAmount).toBe(3525000)
    expect(aggregates.totalContracts).toBe(3)
  })

  it('keeps technical errors in logs and shows only product copy', async () => {
    prepare()
    const error = new Error('internal RPC get_contract_analysis / private_schema')
    rpc.mockReturnValue({ abortSignal: () => Promise.reject(error) })
    await act(async () => { tree = create(view('en')) })
    const rendered = content(tree!.toJSON())
    expect(rendered).toContain('The analysis section is temporarily unavailable.')
    expect(rendered).not.toContain('private_schema')
    expect(rendered).not.toContain('get_contract_analysis')
    expect(logError).toHaveBeenCalledWith('Failed to load contract statistics', error)
  })

  it('localizes only the month display while retaining raw month/year labels and unknown values', () => {
    const raw = "Ιαν '26"
    expect(analysisMonthLabel(raw, getOverviewUi('en'))).toBe("Jan '26")
    expect(analysisMonthLabel(raw, getOverviewUi('el'))).toBe(raw)
    expect(analysisMonthLabel('2026-01', getOverviewUi('en'))).toBe('2026-01')
    expect(analysisMonthLabel("Unknown '26", getOverviewUi('en'))).toBe("Unknown '26")
  })
})
