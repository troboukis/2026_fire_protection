import { describe, expect, it } from 'vitest'
import { createNetworkSimulation, graphPoint, type PhysicsNode } from './antineroPhysics'

const nodes = (): PhysicsNode[] => [
  { id: 'root', kind: 'contract', x: 0, y: 0 },
  { id: 'studies', kind: 'category', x: 340, y: 0 },
  { id: 'doc', kind: 'decision', x: 560, y: 0 },
]
const edges = [{ source: 'root', target: 'studies' }, { source: 'studies', target: 'doc' }]

describe('AntiNERO physics', () => {
  it('holds a dragged node while its neighbours respond, then releases it', () => {
    const working = nodes()
    const simulation = createNetworkSimulation(working, edges)
    working[1].fx = 700; working[1].fy = 400
    simulation.tick(60)
    expect(working[1].x).toBe(700)
    expect(working[1].y).toBe(400)
    expect(Math.abs(working[2].y)).toBeGreaterThan(20)
    working[1].fx = null; working[1].fy = null
    simulation.alpha(.7).tick(120)
    expect(working[1].x).not.toBe(700)
    expect(working.every(node => Number.isFinite(node.x) && Number.isFinite(node.y))).toBe(true)
    expect(edges[0].source).toBe('root')
    simulation.stop()
  })
  it('separates overlapping nodes', () => {
    const working = nodes().map(node => ({ ...node, x: 0, y: 0 }))
    const simulation = createNetworkSimulation(working, edges)
    simulation.tick(150)
    for (let i = 0; i < working.length; i++) for (let j = i + 1; j < working.length; j++) {
      expect(Math.hypot(working[i].x - working[j].x, working[i].y - working[j].y)).toBeGreaterThan(190)
    }
    simulation.stop()
  })
  it('converts drag coordinates correctly with zoom, pan and SVG letterboxing', () => {
    // 1000-square viewBox rendered in a 1000x600 viewport: scale .6.
    const point = graphPoint(650, 260, { left: 50, top: 20, width: 1000, height: 600 }, 500, { x: 20, y: -10, k: 2 })
    expect(point.x).toBeCloseTo((100 / .6 - 20) / 2)
    expect(point.y).toBeCloseTo((-60 / .6 + 10) / 2)
  })
})
