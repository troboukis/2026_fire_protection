import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { MemoryRouter } from 'react-router-dom'
import { describe, expect, it } from 'vitest'
import AntineroPage from './AntineroPage'

function renderRoute(route: string) {
  return renderToStaticMarkup(createElement(MemoryRouter, { initialEntries: [route] }, createElement(AntineroPage)))
}
function overviewSvg(html: string) {
  return html.match(/<svg\b[\s\S]*?<\/svg>/)?.[0]
}
describe('report initial view', () => {
  it('opens directly on the complete network without the removed controls', () => {
    const html = renderRoute('/analysis/antinero-west-attica')
    expect(html.match(/class="report-graph-contract /g)).toHaveLength(21)
    expect(html).not.toContain('report-controls')
    expect(html).not.toContain('report-toolbar')
    expect(html).not.toContain('report-selection-bar')
    expect(html).not.toContain('Χρονολόγιο')
    expect(html).not.toContain('<aside')
    expect(html).not.toContain('<dialog')
  })
  it('opens the selected contract over the complete network in a closable fullscreen dialog', () => {
    const html = renderRoute('/analysis/antinero-west-attica?scope=24SYMV014192335&node=24SYMV014192335')
    expect(overviewSvg(html)?.match(/class="report-graph-contract /g)).toHaveLength(21)
    expect(html).toContain('<dialog')
    expect(html).toContain('Κλείσιμο')
    expect(html).toContain('aria-label="Πληροφορίες επιλεγμένου εγγράφου"')
    expect(html).not.toContain('Στοιχεία επιλογής ↓')
  })
  it('keeps the overview positions and counts unchanged when a contract is selected', () => {
    const overview = renderRoute('/analysis/antinero-west-attica')
    const selected = renderRoute('/analysis/antinero-west-attica?node=24SYMV014192335&contract=24SYMV014192335')

    expect(overviewSvg(selected)).toBe(overviewSvg(overview))
    expect(selected).toContain('<dialog')
  })
  it('ignores the removed scope control without opening document details', () => {
    const html = renderRoute('/analysis/antinero-west-attica?scope=24SYMV014192335')

    expect(overviewSvg(html)?.match(/class="report-graph-contract /g)).toHaveLength(21)
    expect(html).not.toContain('<dialog')
  })
  it('labels the direction of an amendment relationship explicitly', () => {
    const html = renderRoute('/analysis/antinero-west-attica?connection=amendment%3A23SYMV013156865')
    const originalLabel = html.indexOf('Αρχική σύμβαση')
    const originalId = html.indexOf('23SYMV012964096', originalLabel)
    const direction = html.indexOf('τροποποιείται από', originalId)
    const amendmentLabel = html.indexOf('Τροποποιητική πράξη', direction)
    const amendmentId = html.indexOf('23SYMV013156865', amendmentLabel)

    expect(originalLabel).toBeGreaterThan(-1)
    expect(originalId).toBeGreaterThan(originalLabel)
    expect(direction).toBeGreaterThan(originalId)
    expect(amendmentLabel).toBeGreaterThan(direction)
    expect(amendmentId).toBeGreaterThan(amendmentLabel)
    expect(html).toContain('Διορθωτική τροποποίηση')
    expect(html.match(/Κατέβασε το έγγραφο/g)).toHaveLength(2)
    expect(html).not.toContain('Έγγραφα της σχέσης')
    expect(html).not.toContain('Πρόσθετα έγγραφα τεκμηρίωσης')
  })
  it('shows and explains a connection between contracts with the same beneficiary', () => {
    const html = renderRoute('/analysis/antinero-west-attica?connection=beneficiary%3A23SYMV012992150%3A25SYMV016570021')

    expect(html).toContain('Κοινός ανάδοχος')
    expect(html).toContain('23SYMV012992150')
    expect(html).toContain('25SYMV016570021')
    expect(html).toContain('Τ &amp; Τ ΚΑΤΑΣΚΕΥΕΣ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ')
  })
  it('shows document-backed contract context without internal research notes', () => {
    const html = renderRoute('/analysis/antinero-west-attica?scope=23SYMV012992150&node=23SYMV012992150')

    expect(html).toContain('<div class="eyebrow"><time dateTime="2023-06-30">30/6/2023</time></div>')
    expect(html).not.toContain('Ημερομηνία ·')
    expect(html).toContain('Δασαρχεία Αιγάλεω, Καπανδριτίου και Λαυρίου')
    expect(html).not.toContain('<dt>Περιοχή</dt>')
    expect(html).toContain('Συμβατικό τίμημα χωρίς ΦΠΑ')
    expect(html).not.toContain('Αρχικό συμβατικό τίμημα')
    expect(html).not.toContain('Δυτική Αττική · περιοχή έρευνας')
    expect(html).not.toContain('με ΦΠΑ')
    expect(html).not.toContain('στοιχεία ΚΗΜΔΗΣ')
    expect(html).not.toContain('Η ονομασία προέρχεται')
    expect(html).not.toContain('Τα ποσά αφορούν το σύνολο')
    expect(html).not.toContain('Τι προκύπτει από τα έγγραφα')
    expect(html).not.toContain('Δεν έχει προστεθεί ξεχωριστό εύρημα')
  })
  it('uses the amendment document date instead of its parent contract date', () => {
    const html = renderRoute('/analysis/antinero-west-attica?scope=23SYMV013156865&node=23SYMV013156865')

    expect(html).toContain('<div class="eyebrow"><time dateTime="2023-07-24">24/7/2023</time></div>')
    expect(html).not.toContain('Υπογραφή αρχικής σύμβασης')
  })
  it('labels supplement approval dates as approvals', () => {
    const html = renderRoute('/analysis/antinero-west-attica?scope=26SYMV018936694&node=26SYMV018936694')

    expect(html).toContain('<div class="eyebrow"><time dateTime="2026-04-30">30/4/2026</time></div>')
    expect(html).toContain('Πρόσθετο τίμημα χωρίς ΦΠΑ')
    expect(html).toContain('607.719,34 €')
  })
})
