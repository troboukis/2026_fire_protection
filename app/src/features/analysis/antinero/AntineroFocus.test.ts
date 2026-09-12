import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import AntineroFocus from './AntineroFocus'
import { connections } from './data/antineroGraph'
import { contractDecisions, report } from './data/antineroReport'

const noop = () => {}
const props = { relationKinds: ['amendment', 'shared', 'beneficiary'], onSelect: noop, onConnection: noop, onClose: noop, children: null }
function focus(id: string, relationKinds = props.relationKinds) {
  return renderToStaticMarkup(createElement(AntineroFocus, { ...props, relationKinds, selectedId: id, selectedContractId: id }))
}
function focusNodes(html: string) {
  return [...html.matchAll(/<button\b[^>]*data-focus-node-id="([^"]+)"[^>]*>/g)].map(match => ({
    id: match[1],
    classes: match[0].match(/class="([^"]+)"/)![1].split(' '),
  }))
}
function categoryIds(id: string) {
  const populated = new Set(contractDecisions(id).map(item => item.categoryId))
  return report.categories.filter(item => populated.has(item.id)).map(item => `${id}:${item.id}`)
}

describe('focused AntiNERO neighborhood', () => {
  it('shows direct contracts on the left and only populated document categories on the right', () => {
    const id = '23SYMV012992150'
    const html = focus(id)
    const neighbors = connections.filter(edge => edge.source === id || edge.target === id).map(edge => edge.source === id ? edge.target : edge.source)
    const visible = focusNodes(html)
    expect(new Set(visible.map(node => node.id))).toEqual(new Set([...neighbors, ...categoryIds(id)]))
    for (const node of visible) {
      expect(node.classes).toContain(`report-focus-neighbor--${neighbors.includes(node.id) ? 'left' : 'right'}`)
    }
    expect(html).not.toContain(`data-focus-node-id="${id}:other"`)
    expect(html).toContain('Συνδεόμενα έγγραφα στη Διαύγεια')
    expect(html).not.toContain('report-focus-origin-title')
    expect(html).not.toMatch(/\sd="[^"]*[QC]\s/)
    expect(html).toContain('report-focus-line--beneficiary')
  })
  it('keeps every document category on the right for a contract without contract connections', () => {
    const isolated = report.contracts.find(contract => contractDecisions(contract.id).length > 0
      && !connections.some(edge => edge.source === contract.id || edge.target === contract.id))!
    const html = focus(isolated.id)
    const visible = focusNodes(html)

    expect(new Set(visible.map(node => node.id))).toEqual(new Set(categoryIds(isolated.id)))
    expect(visible.length).toBeGreaterThan(0)
    for (const node of visible) {
      expect(node.classes).toContain('report-focus-neighbor--category')
      expect(node.classes).toContain('report-focus-neighbor--right')
      expect(node.classes).not.toContain('report-focus-neighbor--left')
    }
    expect(html).toContain('Συνδεόμενα έγγραφα στη Διαύγεια')
  })
  it('omits all document categories when a contract has no documents', () => {
    const id = '23SYMV013156865'
    expect(contractDecisions(id)).toHaveLength(0)
    const visible = focusNodes(focus(id))

    expect(visible.length).toBeGreaterThan(0)
    expect(visible.every(node => node.classes.includes('report-focus-neighbor--contract'))).toBe(true)
    expect(visible.every(node => node.classes.includes('report-focus-neighbor--left'))).toBe(true)
  })
  it('shows a single populated category without empty neighboring categories', () => {
    const id = '26SYMV018936694'
    const visible = focusNodes(focus(id)).filter(node => node.classes.includes('report-focus-neighbor--category'))

    expect(visible.map(node => node.id)).toEqual([`${id}:changes`])
    expect(visible[0].classes).toContain('report-focus-neighbor--right')
  })
  it('labels the original contract correctly when focusing its amendment', () => {
    const html = focus('23SYMV013156865')
    expect(html).toContain('Αρχική σύμβαση / Κοινός ανάδοχος · Αιγάλεω')
    expect(html).toContain('data-focus-node-id="23SYMV012964096"')
    expect(html).toContain('report-focus-line--amendment')
    expect(html).toContain('marker-end="url(#report-focus-amendment-arrow)"')
    expect(focus('23SYMV012964096')).toContain('marker-start="url(#report-focus-amendment-arrow)"')
  })
  it('applies relation filters without losing access to document categories', () => {
    const id = '23SYMV012992150'
    const html = focus(id, [])
    expect(html).not.toContain('data-focus-node-id="25SYMV016570021"')
    const visible = focusNodes(html)
    expect(new Set(visible.map(node => node.id))).toEqual(new Set(categoryIds(id)))
    expect(visible.every(node => node.classes.includes('report-focus-neighbor--right'))).toBe(true)
  })
  it('preserves the active relationship kind when a stronger relationship is filtered out', () => {
    const html = focus('23SYMV013156865', ['beneficiary'])

    expect(html).toContain('data-focus-node-id="23SYMV012964096"')
    expect(html).toContain('Κοινός ανάδοχος · Αιγάλεω')
    expect(html).toContain('report-focus-line--beneficiary')
    expect(html).not.toContain('report-focus-line--amendment')
    expect(html).not.toContain('Αρχική σύμβαση /')
  })
  it('retains the source category on the left and every associated contract for a shared decision', () => {
    const decision = report.decisions.find(item => item.contractIds.length > 1)!
    for (const contextId of decision.contractIds) {
      const html = renderToStaticMarkup(createElement(AntineroFocus, { ...props, selectedId: decision.id, selectedContractId: contextId }))
      const visible = focusNodes(html)
      const contracts = visible.filter(node => node.classes.includes('report-focus-neighbor--contract'))
      expect(new Set(contracts.map(node => node.id))).toEqual(new Set(decision.contractIds))
      expect(contracts.every(node => node.classes.includes('report-focus-neighbor--left'))).toBe(true)
      const category = visible.find(node => node.id === `${contextId}:${decision.categoryId}`)!
      expect(category.classes).toContain('report-focus-neighbor--category')
      expect(category.classes).toContain('report-focus-neighbor--left')
    }
  })
})
