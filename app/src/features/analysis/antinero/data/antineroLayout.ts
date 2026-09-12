import { forceCollide, forceLink, forceManyBody, forceSimulation, forceX, forceY, type SimulationNodeDatum } from 'd3'
import { connections, programmeStyle, type ContractConnection, type NetworkEdge, type NetworkNode } from './antineroGraph'
import { report } from './antineroReport'

export const overviewSize = { width: 1120, height: 620 }
export const overviewNodeRadius = 35

// The footprint includes the circle and its two lines of text underneath.
const footprint = { width: 136, height: 120, top: 41, bottom: 79 }
const inset = 26
type LayoutNode = NetworkNode & SimulationNodeDatum
export type OverviewEdge = NetworkEdge & Pick<ContractConnection, 'relations' | 'beneficiaryNames'>

function constrain(node: LayoutNode) {
  node.x = Math.max(inset + footprint.width / 2, Math.min(overviewSize.width - inset - footprint.width / 2, node.x))
  node.y = Math.max(inset + footprint.top, Math.min(overviewSize.height - inset - footprint.bottom, node.y))
}

function separate(nodes: LayoutNode[]) {
  let overlap = false
  for (let i = 0; i < nodes.length; i++) for (let j = i + 1; j < nodes.length; j++) {
    const a = nodes[i], b = nodes[j]
    const dx = b.x - a.x, dy = b.y - a.y
    const horizontal = footprint.width - Math.abs(dx)
    const vertical = footprint.height - Math.abs(dy)
    if (horizontal <= 0 || vertical <= 0) continue
    overlap = true
    // Resolve along the shorter axis, preserving the settled graph's grouping.
    if (horizontal < vertical) {
      const shift = (horizontal + .01) / 2 * (dx < 0 ? -1 : 1)
      a.x -= shift; b.x += shift
    } else {
      const shift = (vertical + .01) / 2 * (dy < 0 ? -1 : 1)
      a.y -= shift; b.y += shift
    }
  }
  nodes.forEach(constrain)
  return overlap
}

function settleOverview() {
  // Stable ID order also fixes D3's deterministic initial conditions. All
  // contracts and relationship kinds contribute, even when filters hide them.
  const contracts = [...report.contracts].sort((a, b) => a.id.localeCompare(b.id))
  const nodes: LayoutNode[] = contracts.map((contract, index) => {
    const angle = index * Math.PI * (3 - Math.sqrt(5))
    const distance = Math.sqrt((index + .5) / contracts.length)
    return {
      id: contract.id, contractId: contract.id, kind: 'contract',
      color: programmeStyle(contract.nodeContext.programme).color,
      x: overviewSize.width / 2 + Math.cos(angle) * distance * 435,
      y: overviewSize.height / 2 - 18 + Math.sin(angle) * distance * 190,
    }
  })
  const simulation = forceSimulation(nodes).stop()
    .force('links', forceLink<LayoutNode, { source: string | LayoutNode; target: string | LayoutNode }>(connections.map(edge => ({ source: edge.source, target: edge.target })))
      .id(node => node.id).distance(150).strength(.6))
    .force('charge', forceManyBody<LayoutNode>().strength(-170))
    .force('collision', forceCollide<LayoutNode>(74).iterations(3))
    .force('x', forceX<LayoutNode>(overviewSize.width / 2).strength(.022))
    .force('y', forceY<LayoutNode>(overviewSize.height / 2 - 18).strength(.075))
    .velocityDecay(.5)
  for (let tick = 0; tick < 360; tick++) {
    simulation.tick()
    nodes.forEach(constrain)
  }
  simulation.stop()
  // Circle collision alone cannot protect the rectangular labels. Finish
  // synchronously, before React receives positions, so clicks never restart it.
  for (let pass = 0; pass < 800; pass++) if (!separate(nodes)) break
  return nodes.map(({ id, contractId, kind, color, x, y }): NetworkNode => ({ id, contractId, kind, color, x, y }))
}

let settledNodes: NetworkNode[] | undefined

export function layoutOverview(contractIds: string[]): { nodes: NetworkNode[]; edges: OverviewEdge[]; width: number; height: number } {
  settledNodes ??= settleOverview()
  const visible = new Set(contractIds)
  return {
    ...overviewSize,
    // Keep cached coordinates private: consumers can never mutate the next view.
    nodes: settledNodes.filter(node => visible.has(node.id)).map(node => ({ ...node })),
    edges: connections.filter(edge => visible.has(edge.source) && visible.has(edge.target)).map(edge => ({
      ...edge, color: '#89857c', relations: [...edge.relations], evidenceIds: [...edge.evidenceIds],
      beneficiaryNames: edge.beneficiaryNames ? [...edge.beneficiaryNames] : undefined,
    })),
  }
}
