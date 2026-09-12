import { describe, expect, it } from 'vitest'
import { connections, programmeStyle } from './antineroGraph'
import { layoutOverview, overviewNodeRadius } from './antineroLayout'
import { report } from './antineroReport'

const contractIds = report.contracts.map(contract => contract.id)

describe('compact AntiNERO overview', () => {
  it('keeps every circle and its two-line label inside the canvas without overlap', () => {
    const graph = layoutOverview(contractIds)
    expect(graph.nodes).toHaveLength(contractIds.length)
    for (const node of graph.nodes) {
      expect(Number.isFinite(node.x) && Number.isFinite(node.y)).toBe(true)
      expect(node.x - 60).toBeGreaterThanOrEqual(0)
      expect(node.x + 60).toBeLessThanOrEqual(graph.width)
      expect(node.y - overviewNodeRadius).toBeGreaterThanOrEqual(0)
      expect(node.y + 70).toBeLessThanOrEqual(graph.height)
    }
    for (let i = 0; i < graph.nodes.length; i++) for (let j = i + 1; j < graph.nodes.length; j++) {
      const a = graph.nodes[i], b = graph.nodes[j]
      expect(Math.abs(a.x - b.x) >= 135.9 || Math.abs(a.y - b.y) >= 119.9).toBe(true)
    }
  })

  it('retains the same coordinates after reordering, filtering and restoring nodes', () => {
    const graph = layoutOverview(contractIds)
    expect(layoutOverview([...contractIds].reverse())).toEqual(graph)
    const subset = contractIds.slice(0, 5)
    expect(layoutOverview(subset).nodes).toEqual(graph.nodes.filter(node => subset.includes(node.id)))
    expect(layoutOverview(contractIds)).toEqual(graph)
  })

  it('keeps every relationship and its evidence available for display filters', () => {
    const graph = layoutOverview(contractIds)
    const nodes = new Map(graph.nodes.map(node => [node.id, node]))
    expect(graph.edges).toHaveLength(connections.length)
    for (const connection of connections) {
      expect(graph.edges.find(edge => edge.id === connection.id)).toMatchObject(connection)
      const source = nodes.get(connection.source)!, target = nodes.get(connection.target)!
      expect(Math.hypot(source.x - target.x, source.y - target.y)).toBeLessThan(300)
    }
    for (const node of graph.nodes) {
      const contract = report.contracts.find(item => item.id === node.id)!
      expect(node.color).toBe(programmeStyle(contract.nodeContext.programme).color)
    }
    const subset = contractIds.slice(0, 5)
    expect(layoutOverview(subset).edges.every(edge => subset.includes(edge.source) && subset.includes(edge.target))).toBe(true)
  })

  it('does not mutate callers or expose its cached positions and relationship arrays', () => {
    const input = [...contractIds]
    const before = structuredClone({ contracts: report.contracts, connections })
    const graph = layoutOverview(input)
    const expected = structuredClone(graph)
    graph.nodes[0].x = -999
    graph.edges[0].relations.length = 0
    graph.edges[0].evidenceIds!.length = 0
    expect(input).toEqual(contractIds)
    expect({ contracts: report.contracts, connections }).toEqual(before)
    expect(layoutOverview(input)).toEqual(expected)
    expect(layoutOverview([])).toMatchObject({ nodes: [], edges: [] })
  })
})
