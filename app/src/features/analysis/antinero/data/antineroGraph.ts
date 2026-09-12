import { report } from './antineroReport'

export const programmeStyles = [
  { label: 'AntiNERO I', color: '#897747' },
  { label: 'AntiNERO II', color: '#2864a0' },
  { label: 'AntiNERO III', color: '#277b64' },
  { label: 'AntiNERO IV', color: '#8555a3' },
  { label: 'Μη προσδιορισμένη φάση', color: '#777777' },
]
export function programmeStyle(programme: string) {
  return programmeStyles.find(item => item.label === programme) ?? programmeStyles[4]
}
export type ConnectionKind = 'amendment' | 'shared' | 'beneficiary'
export type ContractConnection = {
  id: string; source: string; target: string; kind: ConnectionKind; relations: ConnectionKind[]
  evidenceIds: string[]; beneficiaryNames?: string[]
}
export function contractConnections(): ContractConnection[] {
  const pairs = new Map<string, ContractConnection>()
  for (const decision of report.decisions) {
    const ids = [...new Set(decision.contractIds)].sort()
    for (let i = 0; i < ids.length; i++) for (let j = i + 1; j < ids.length; j++) {
      const id = `shared:${ids[i]}:${ids[j]}`
      const edge = pairs.get(id) ?? { id, source: ids[i], target: ids[j], kind: 'shared', relations: ['shared'], evidenceIds: [] }
      edge.evidenceIds.push(decision.id)
      pairs.set(id, edge)
    }
  }
  const amendments: ContractConnection[] = report.contracts.filter(c => c.parentId).map(c => ({ id: `amendment:${c.id}`, source: c.parentId!, target: c.id, kind: 'amendment', relations: ['amendment'], evidenceIds: [c.id] }))
  for (const amendment of amendments) {
    const pair = `shared:${[amendment.source, amendment.target].sort().join(':')}`
    if (pairs.has(pair)) {
      amendment.evidenceIds.push(...pairs.get(pair)!.evidenceIds)
      amendment.relations.push('shared')
      pairs.delete(pair)
    }
  }
  const result = [...amendments, ...pairs.values()]
  const connectionByPair = new Map(result.map(edge => [[edge.source, edge.target].sort().join(':'), edge]))
  for (let i = 0; i < report.contracts.length; i++) for (let j = i + 1; j < report.contracts.length; j++) {
    const source = report.contracts[i], target = report.contracts[j]
    const targetBeneficiaries = new Set(target.beneficiaries.map(item => item.id))
    const common = source.beneficiaries.filter(item => targetBeneficiaries.has(item.id))
    if (!common.length) continue
    const pair = [source.id, target.id].sort().join(':')
    const existing = connectionByPair.get(pair)
    if (existing) {
      if (!existing.relations.includes('beneficiary')) existing.relations.push('beneficiary')
      existing.beneficiaryNames = [...new Set(common.map(item => item.name))]
      continue
    }
    const edge: ContractConnection = {
      id: `beneficiary:${pair}`, source: source.id, target: target.id, kind: 'beneficiary',
      relations: ['beneficiary'], evidenceIds: [], beneficiaryNames: [...new Set(common.map(item => item.name))],
    }
    result.push(edge)
    connectionByPair.set(pair, edge)
  }
  return result
}
export const connections = contractConnections()
export type NetworkNode = {
  id: string; kind: 'contract' | 'category' | 'decision'; x: number; y: number
  contractId?: string; categoryId?: string; count?: number; open?: boolean; color: string
}
export type NetworkEdge = {
  id: string; source: string; target: string; kind: ConnectionKind | 'branch'; color: string; evidenceIds?: string[]
}
export function buildContractNetwork(contractIds: string[], expandedIds: string[], openCategories: string[], relationKinds: string[]) {
  const nodes = new Map<string, NetworkNode>()
  const edges: NetworkEdge[] = []
  const ordered = report.contracts.filter(c => contractIds.includes(c.id)).sort((a, b) => a.nodeContext.programme.localeCompare(b.nodeContext.programme) || a.id.localeCompare(b.id))
  const columns = Math.ceil(Math.sqrt(ordered.length))
  const rows = Math.ceil(ordered.length / Math.max(1, columns))
  ordered.forEach((contract, index) => {
    const x = (index % columns - (columns - 1) / 2) * 640
    const y = (Math.floor(index / columns) - (rows - 1) / 2) * 580
    nodes.set(contract.id, { id: contract.id, kind: 'contract', x, y, color: programmeStyle(contract.nodeContext.programme).color, contractId: contract.id, open: expandedIds.includes(contract.id) })
    if (!expandedIds.includes(contract.id)) return
    report.categories.forEach((category, i) => {
      const id = `${contract.id}:${category.id}`
      const angle = i * Math.PI * 2 / 5 - Math.PI / 2
      const cx = x + 410 * Math.cos(angle), cy = y + 410 * Math.sin(angle)
      const items = report.decisions.filter(d => d.contractIds.includes(contract.id) && d.categoryId === category.id)
      nodes.set(id, { id, kind: 'category', x: cx, y: cy, contractId: contract.id, categoryId: category.id, color: category.color, count: items.length, open: openCategories.includes(id) })
      edges.push({ id: `branch:${id}`, source: contract.id, target: id, kind: 'branch', color: category.color })
      if (!openCategories.includes(id)) return
      items.forEach((decision, j) => {
        // One ADA node, even when several expanded contracts reference it.
        if (!nodes.has(decision.id)) nodes.set(decision.id, { id: decision.id, kind: 'decision', x: cx + 300 * Math.cos(angle + j * .4), y: cy + 300 * Math.sin(angle + j * .4), categoryId: category.id, color: category.color })
        edges.push({ id: `branch:${id}:${decision.id}`, source: id, target: decision.id, kind: 'branch', color: category.color })
      })
    })
  })
  for (const edge of connections) {
    const activeKind = edge.relations.find(kind => relationKinds.includes(kind))
    if (activeKind && nodes.has(edge.source) && nodes.has(edge.target)) {
      edges.push({ ...edge, kind: relationKinds.includes(edge.kind) ? edge.kind : activeKind, color: '#89857c' })
    }
  }
  return { nodes: [...nodes.values()], edges }
}
