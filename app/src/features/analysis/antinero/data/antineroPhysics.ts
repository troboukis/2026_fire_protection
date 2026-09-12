import { forceCollide, forceLink, forceManyBody, forceSimulation, forceX, forceY, type SimulationLinkDatum, type SimulationNodeDatum } from 'd3'

export interface PhysicsNode extends SimulationNodeDatum {
  id: string
  kind: 'contract' | 'category' | 'decision'
  x: number
  y: number
}
export interface PhysicsEdge { source: string; target: string; kind?: string }

// D3 mutates its working nodes/links. Callers own these copies, never report data.
export function createNetworkSimulation(nodes: PhysicsNode[], edges: PhysicsEdge[]) {
  return forceSimulation(nodes).stop()
    .force('links', forceLink<PhysicsNode, SimulationLinkDatum<PhysicsNode> & { kind?: string }>(edges.map(edge => ({ ...edge })))
      .id(node => node.id).distance(edge => edge.kind === 'shared' || edge.kind === 'amendment' || edge.kind === 'beneficiary' ? 650 : (typeof edge.source === 'object' && edge.source.kind === 'contract') ? 410 : 260).strength(edge => edge.kind === 'shared' || edge.kind === 'amendment' || edge.kind === 'beneficiary' ? .035 : .16))
    .force('charge', forceManyBody<PhysicsNode>().strength(node => node.kind === 'decision' ? -700 : -1800))
    .force('collision', forceCollide<PhysicsNode>().radius(node => node.kind === 'contract' ? 215 : node.kind === 'category' ? 125 : 145).iterations(3))
    .force('x', forceX<PhysicsNode>(0).strength(.004))
    .force('y', forceY<PhysicsNode>(0).strength(.004))
    .velocityDecay(.45).alphaDecay(.025)
}

export function graphPoint(clientX: number, clientY: number, rect: { left: number; top: number; width: number; height: number }, bound: number, camera: { x: number; y: number; k: number }) {
  const unit = Math.min(rect.width, rect.height) / (2 * bound)
  return {
    x: ((clientX - rect.left - rect.width / 2) / unit - camera.x) / camera.k,
    y: ((clientY - rect.top - rect.height / 2) / unit - camera.y) / camera.k,
  }
}
