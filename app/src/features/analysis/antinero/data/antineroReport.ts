import snapshot from './antineroReport.generated.json'

export const report = snapshot
export const defaultContractId = '24SYMV014192335'
export type ReportContract = typeof report.contracts[number]
export type ReportDecision = typeof report.decisions[number]

export function resolveSelection(node: string | null, context: string | null) {
  const decision = report.decisions.find(item => item.id === node)
  const direct = report.contracts.find(item => item.id === node || (item.aliases as string[]).includes(node ?? ''))
  const contract = direct ?? report.contracts.find(item => item.id === context && decision?.contractIds.includes(item.id))
    ?? report.contracts.find(item => item.id === decision?.contractIds[0])
    ?? report.contracts.find(item => item.id === defaultContractId)!
  return { contract, decision }
}

export function contractDecisions(id: string) {
  return report.decisions.filter(item => item.contractIds.includes(id))
    .sort((a, b) => a.date.localeCompare(b.date) || a.id.localeCompare(b.id))
}
