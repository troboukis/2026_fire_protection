import { createElement } from 'react'
import { act, create, type ReactTestRenderer, type ReactTestInstance } from 'react-test-renderer'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import I18nProvider from './I18nProvider'
import { getOverviewUi } from './overview'
import type { AppLanguage } from './languages'
import source from './catalogs/ministry.el.json'
import target from './catalogs/ministry.en.json'
import EnvironmentMinistryPage from '../pages/EnvironmentMinistryPage'
import LatestContractCard from '../components/LatestContractCard'
import DiavgeiaDecisionCard from '../components/DiavgeiaDecisionCard'
import ContractModal from '../components/ContractModal'
import MapLegendToggle from '../components/MapLegendToggle'
import ProfileMetricCard from '../components/ProfileMetricCard'

const mocks = vi.hoisted(() => ({ rpc: vi.fn() }))
vi.mock('../lib/supabase', () => ({ supabase: { rpc: mocks.rpc } }))
vi.mock('../lib/devView', () => ({ DEV_VIEW_AVAILABLE: false, useDevViewEnabled: () => [false] }))
vi.mock('../lib/municipalitiesGeojson', () => ({ loadMunicipalitiesGeojson: async () => ({
  type: 'FeatureCollection', features: [{ type: 'Feature', properties: { municipality_code: '9186' },
    geometry: { type: 'Polygon', coordinates: [[[23, 38], [24, 38], [24, 39], [23, 39], [23, 38]]] } }],
}) }))

const today = new Date().toISOString().slice(0, 10)
const year = new Date().getFullYear()
const contractTitle = 'Πρωτότυπος τίτλος σύμβασης 26SYMV012345678'
const documentTitle = 'Πρωτότυπος τίτλος απόφασης Ψ123ΩΑ-ΒΓΔ'
const contract = {
  id: 51, who: 'Υπουργείο Περιβάλλοντος και Ενέργειας', what: contractTitle,
  when: `${year}-05-01`, contract_signed_date: today,
  start_date: `${year}-06-01`, end_date: `${year}-12-31`, no_end_date: false,
  amount_without_vat: 1234, amount_with_vat: 1530.16,
  beneficiary: 'ΕΛΛΗΝΙΚΟΣ ΑΝΑΔΟΧΟΣ', beneficiary_vat_number: '123456789',
  primary_beneficiary: 'ΕΛΛΗΝΙΚΟΣ ΑΝΑΔΟΧΟΣ', primary_beneficiary_vat_number: '123456789',
  primary_signer: 'ΕΛΛΗΝΑΣ ΥΠΟΓΡΑΦΩΝ', signers: 'ΕΛΛΗΝΑΣ ΥΠΟΓΡΑΦΩΝ',
  contract_type: 'Απευθείας ανάθεση', contract_kind: 'Υπηρεσίες', award_procedure: 'Απευθείας ανάθεση',
  reference_number: '26SYMV012345678', contract_related_ada: 'Ψ123ΩΑ-ΒΓΔ',
}
const payload = {
  ministry_name: 'Υπουργείο Περιβάλλοντος και Ενέργειας',
  identification: { organization_keys: ['ΥΠΕΝ'] },
  total_spend: 100000, signed_current_amount: 40000, signed_2026_count: 12,
  active_carryover_count: 7, current_year_beneficiary_count: 3,
  direct_award_amount: 20000, direct_award_with_auction_amount: 20000, direct_award_without_auction_amount: 0,
  current_year_direct_award_amount: 10000, current_year_direct_award_with_auction_amount: 10000,
  current_year_direct_award_without_auction_amount: 0,
  recent_active_contracts: [contract], featured_contracts: [contract],
  recent_diavgeia_decisions: [{ id: 7, subject: documentTitle, ada: 'Ψ123ΩΑ-ΒΓΔ',
    decision_date: today, spending_contractors_value: '2.345,00 EUR',
    diavgeia_document_type_decision_uid: 'ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ' }],
  work_points: [{ id: 'point-1', procurement_id: 51, lat: 38.5, lon: 23.5,
    contract_title: contractTitle, amount_without_vat: 1234, beneficiary: 'ΕΛΛΗΝΙΚΟΣ ΑΝΑΔΟΧΟΣ',
    assignment_type: 'Απευθείας ανάθεση' }],
}

function page(language: AppLanguage) {
  return createElement(I18nProvider, { language, children: createElement(MemoryRouter, {
    initialEntries: ['/en/environment-ministry?tag=a%20b&tag=a+b#sources'],
  }, createElement(EnvironmentMinistryPage)) })
}
function text(node: unknown): string {
  if (node == null || typeof node === 'boolean') return ''
  if (typeof node === 'string' || typeof node === 'number') return String(node)
  if (Array.isArray(node)) return node.map(text).join(' ')
  if (typeof node === 'object' && 'children' in node) return text(node.children)
  return ''
}
function nodeText(node: ReactTestInstance): string { return text(node.children) }

let tree: ReactTestRenderer
beforeEach(() => { mocks.rpc.mockReset(); mocks.rpc.mockResolvedValue({ data: structuredClone(payload), error: null }) })
afterEach(() => { if (tree) act(() => tree.unmount()) })
async function mount(language: AppLanguage = 'en') {
  await act(async () => { tree = create(page(language)); await Promise.resolve() })
  return tree.root
}

describe('reviewed Ministry of Environment and Energy UI', () => {
  it('covers every message, placeholder and numerical reference without original titles', () => {
    expect(Object.keys(target).sort()).toEqual(Object.keys(source).sort())
    for (const key of Object.keys(source) as Array<keyof typeof source>) {
      expect(target[key].trim(), key).not.toBe('')
      const placeholders = (value: string) => value.match(/\{\{[^{}]+\}\}/g)?.sort() ?? []
      const numbers = (value: string) => value.replace(/\{\{[^{}]+\}\}/g, '').match(/\d+/g)?.sort() ?? []
      expect(placeholders(target[key]), key).toEqual(placeholders(source[key]))
      expect(numbers(target[key]), key).toEqual(numbers(source[key]))
      expect(target[key], key).not.toMatch(/[Α-Ωα-ω]/)
    }
    expect(JSON.stringify(source)).not.toContain(contractTitle)
    expect(JSON.stringify(source)).not.toContain(documentTitle)
    expect(getOverviewUi('en')(contractTitle)).toBe(contractTitle)
  })

  it('keeps new-contract spending, active older contracts and bid invitations distinct', async () => {
    const original = structuredClone(payload)
    const root = await mount()
    const copy = text(tree.toJSON())
    expect(copy).toContain(`In ${year}, it spent €40,000 on fire protection works`)
    expect(copy).toContain('€10,000 (25%) is recorded in KIMDIS as direct awards')
    expect(copy).toContain('specific companies were invited to submit bids')
    expect(copy).toContain('excluding older contracts that remain active*')
    expect(copy).toContain('Law 998/1979')
    expect(copy).toContain(`contracts, we mean contracts either signed in  ${year} , or signed before  ${year}`)
    expect(root.findAllByType(ProfileMetricCard).map(node => node.props.value)).toEqual(['100K €', '12', '7', '3'])
    expect(payload).toEqual(original)
    expect(root.findByType(LatestContractCard).props.item.sourceValues).toMatchObject({
      submittedAt: `${year}-05-01`, signedAt: today, withoutVat: 1234, withVat: 1530.16,
    })
    expect(root.findByType(DiavgeiaDecisionCard).props.item).toMatchObject({ subject: documentTitle, amountEur: 2345 })
    const link = root.findAllByType('a').find(node => nodeText(node) === 'All Diavgeia decisions')
    expect(link?.props.href).toBe('/en/diavgeia?q=' + encodeURIComponent(payload.ministry_name) + '&allDates=1')
  })

  it.each([
    [0, 100, 'without a call for proposals', 'Article 16(5)'],
    [50, 50, 'these include contracts using Law 998/1979', '25%'],
    [0, 0, '€0 (0%) was awarded through direct awards', 'new contracts'],
  ])('retains the other direct-award narrative branches (%i/%i)', async (withAuction, withoutAuction, expected, detail) => {
    const value = structuredClone(payload)
    Object.assign(value, { current_year_direct_award_with_auction_amount: withAuction,
      current_year_direct_award_without_auction_amount: withoutAuction,
      current_year_direct_award_amount: withAuction + withoutAuction > 0 ? 10000 : 0 })
    mocks.rpc.mockResolvedValue({ data: value, error: null })
    await mount()
    expect(text(tree.toJSON())).toContain(expected)
    expect(text(tree.toJSON())).toContain(detail)
  })

  it('updates a visible tooltip from raw values without changing its title, geometry or requests', async () => {
    const root = await mount('el')
    const svg = root.findByType('svg')
    const paths = root.findByProps({ className: 'environment-map__base' }).findAllByType('path').map(node => node.props.d)
    act(() => root.findByProps({ className: 'environment-map__point-hitbox' }).props.onMouseEnter({ clientX: 0, clientY: 0 }))
    expect(nodeText(root.findByProps({ className: 'environment-map__tooltip app-tooltip' }))).toContain('Κόστος χωρίς ΦΠΑ')
    act(() => tree.update(page('en')))
    const tooltip = root.findByProps({ className: 'environment-map__tooltip app-tooltip' })
    expect(nodeText(tooltip)).toContain('Cost excluding VAT: €1,234')
    expect(nodeText(tooltip)).toContain('Award procedure: Direct award')
    expect(tooltip.findByType('strong').props).toMatchObject({ lang: 'el', children: contractTitle })
    expect(root.findByType('svg')).toBe(svg)
    expect(root.findByProps({ className: 'environment-map__base' }).findAllByType('path').map(node => node.props.d)).toEqual(paths)
    expect(mocks.rpc).toHaveBeenCalledTimes(1)
  })

  it.each([false, true])('keeps an open original contract, source dates and hidden layer across language changes (no end: %s)', async noEnd => {
    const value = structuredClone(payload)
    value.recent_active_contracts[0].no_end_date = noEnd
    if (noEnd) value.recent_active_contracts[0].end_date = ''
    mocks.rpc.mockResolvedValue({ data: value, error: null })
    const root = await mount('el')
    act(() => root.findByType(MapLegendToggle).props.onToggle())
    act(() => root.findByType(LatestContractCard).props.onOpen())
    const modal = root.findByType(ContractModal)
    act(() => tree.update(page('en')))
    expect(root.findByType(ContractModal)).toBe(modal)
    expect(root.findByType(MapLegendToggle).props.visible).toBe(false)
    const heading = modal.findAllByType('h2').find(node => node.props.lang === 'el')
    expect(heading?.props.children).toBe(contractTitle)
    expect(nodeText(modal)).toContain('€1,234')
    expect(nodeText(modal)).toContain('€1,530')
    expect(nodeText(modal)).toContain('01 May ' + year)
    expect(nodeText(modal)).toContain(noEnd ? 'No end date' : '31 Dec ' + year)
    act(() => tree.update(page('el')))
    if (noEnd) expect(nodeText(modal)).toContain('Χωρίς λήξη')
    expect(mocks.rpc).toHaveBeenCalledTimes(1)
  })

  it('uses translated product copy for a failed request without exposing technical diagnostics', async () => {
    mocks.rpc.mockResolvedValue({ data: null, error: { message: 'private_table RPC stack trace', code: '57014' } })
    await mount('el')
    act(() => tree.update(page('en')))
    expect(text(tree.toJSON())).toContain('The Ministry of Environment and Energy data could not be loaded.')
    expect(text(tree.toJSON())).not.toMatch(/private_table|RPC|stack trace|57014/)
    expect(mocks.rpc).toHaveBeenCalledTimes(1)
  })
})
