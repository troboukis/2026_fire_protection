import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import AntineroNetwork from './AntineroNetwork'
import { defaultContractId, report } from './data/antineroReport'
import { buildContractNetwork, connections, programmeStyle, programmeStyles } from './data/antineroGraph'

const contractIds = report.contracts.map(item => item.id)
const noop = () => {}
const props = { contractIds, relationKinds: ['amendment', 'shared', 'beneficiary'], onSelect: noop, onConnection: noop }

describe('AntiNERO all-contract network', () => {
  it('starts with all 21 contract records and no expanded category branches', () => {
    const html = renderToStaticMarkup(createElement(AntineroNetwork, props))
    expect(html.match(/class="report-graph-contract /g)).toHaveLength(21)
    expect(html).not.toContain('class="report-graph-category"')
    expect(html).not.toContain('class="report-graph-decision ')
    for (const id of contractIds) expect(html).toContain(`data-node-id="${id}"`)
    expect(html).toContain('Μάνδρας–Ειδυλλίας')
  })
  it('keeps the overview geometry and node set unchanged when selecting a contract or decision', () => {
    const selected = report.decisions.find(item => item.contractIds.includes(defaultContractId))!
    const initial = renderToStaticMarkup(createElement(AntineroNetwork, props))
    const focused = renderToStaticMarkup(createElement(AntineroNetwork, { ...props, selectedId: selected.id, selectedContractId: defaultContractId }))
    expect(focused).toBe(initial)
    expect(focused.match(/class="report-graph-contract /g)).toHaveLength(21)
    expect(focused).not.toContain('report-graph-category')
    expect(focused).not.toContain('Παύση κίνησης')
    expect(focused.match(/aria-haspopup="dialog"/g)).toHaveLength(21)
  })
  it('filters relationship paths without changing node positions', () => {
    const initial = renderToStaticMarkup(createElement(AntineroNetwork, props))
    const filtered = renderToStaticMarkup(createElement(AntineroNetwork, { ...props, relationKinds: [] }))
    const nodeTransforms = (html: string) => [...html.matchAll(/data-node-id="[^"]+"[^>]*transform="[^"]+"/g)].map(match => match[0])
    expect(nodeTransforms(initial)).toEqual(nodeTransforms(filtered))
    expect(initial.match(/class="report-connection-action"/g)).toHaveLength(connections.length)
    expect(initial).not.toMatch(/\sd="[^"]*[QC]\s/)
    expect(initial).toContain('aria-label="Υπόμνημα συνδέσεων"')
    expect(initial).toContain('aria-label="Χρώματα προγραμμάτων AntiNERO"')
    expect(initial.indexOf('aria-label="Χρώματα προγραμμάτων AntiNERO"')).toBeGreaterThan(initial.indexOf('</svg>'))
    expect(initial.indexOf('aria-label="Υπόμνημα συνδέσεων"')).toBeGreaterThan(initial.indexOf('</svg>'))
    expect(initial).toContain('Τροποποιεί')
    expect(initial).not.toContain('→')
    expect(initial.match(/marker-end="url\(#report-amendment-arrow\)"/g)).toHaveLength(4)
    expect(initial).not.toContain('class="report-connection-tooltip')
    expect(initial).toContain('aria-label="Τροποποιεί:')
    expect(initial).toContain('aria-label="Κοινές αποφάσεις Διαύγειας:')
    expect(initial).toContain('aria-label="Κοινός ανάδοχος:')
    expect(initial).toContain('Κοινές αποφάσεις Διαύγειας')
    expect(initial).toContain('Κοινός ανάδοχος')
    expect(filtered.match(/class="is-inactive"/g)).toHaveLength(3)
    expect(filtered).not.toContain('class="report-connection-action"')
  })
  it('deduplicates shared documents while retaining every contract association', () => {
    const categories = contractIds.flatMap(id => report.categories.map(category => `${id}:${category.id}`))
    const graph = buildContractNetwork(contractIds, contractIds, categories, ['amendment', 'shared'])
    expect(graph.nodes.filter(node => node.kind === 'decision')).toHaveLength(426)
    expect(graph.nodes.filter(node => node.kind === 'category')).toHaveLength(105)
    expect(new Set(graph.nodes.map(node => node.id)).size).toBe(graph.nodes.length)
    const ids = new Set(graph.nodes.map(node => node.id))
    expect(graph.edges.every(edge => ids.has(edge.source) && ids.has(edge.target))).toBe(true)
    const shared = report.decisions.find(item => item.contractIds.length > 1)!
    expect(graph.nodes.filter(node => node.id === shared.id)).toHaveLength(1)
    expect(graph.edges.filter(edge => edge.target === shared.id)).toHaveLength(shared.contractIds.length)
    expect(graph.edges.filter(edge => edge.kind === 'branch')).toHaveLength(534)
  })
  it('connects matching beneficiaries and merges them into stronger existing relations', () => {
    expect(connections).toHaveLength(10)
    expect(connections.filter(edge => edge.kind === 'amendment')).toHaveLength(4)
    expect(connections.filter(edge => edge.kind === 'beneficiary')).toHaveLength(5)
    expect(connections.filter(edge => edge.relations.includes('beneficiary'))).toHaveLength(9)
    expect(connections.find(edge => edge.id === 'amendment:26SYMV018936694')?.evidenceIds).toContain('65ΕΔ4653Π8-ΝΜΕ')
    const sameBeneficiary = connections.find(edge => edge.id === 'beneficiary:23SYMV012992150:25SYMV016570021')!
    expect(sameBeneficiary.beneficiaryNames).toEqual(['Τ & Τ ΚΑΤΑΣΚΕΥΕΣ ΑΝΩΝΥΜΗ ΕΤΑΙΡΕΙΑ'])
    const shared = connections.find(edge => edge.kind === 'shared')!
    expect(shared.evidenceIds).toHaveLength(2)
    for (const ada of shared.evidenceIds) {
      const decision = report.decisions.find(item => item.id === ada)!
      expect(decision.contractIds).toEqual(expect.arrayContaining([shared.source, shared.target]))
    }
    expect(buildContractNetwork(contractIds, [], [], []).edges).toHaveLength(0)
    expect(buildContractNetwork(contractIds, [], [], ['beneficiary']).edges).toHaveLength(9)
    expect(buildContractNetwork([defaultContractId], [], [], ['amendment', 'shared', 'beneficiary']).nodes).toHaveLength(1)
  })
  it('keeps an unspecified phase grey and does not derive programme colour from year', () => {
    expect(new Set(programmeStyles.map(item => item.color)).size).toBe(5)
    expect(programmeStyle('AntiNERO').color).toBe('#777777')
    expect(programmeStyle('AntiNERO IV').color).not.toBe(programmeStyle('AntiNERO III').color)
    const iv = report.contracts.find(item => item.id === '25SYMV017345053')!
    expect(iv.nodeContext.programme).toBe('AntiNERO IV')
    expect(iv.nodeContext.programmeNote).toContain('AntiNERO III')
  })
})
