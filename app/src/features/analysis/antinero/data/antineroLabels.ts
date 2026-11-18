import { greekOverviewUi, type OverviewUi } from '../../../../i18n/overview'
import type { ReportContract } from './antineroReport'

export function contractMark(contract: ReportContract) {
  return contract.label.match(/Έργο\s+(\S+)/i)?.[1] ?? (contract.parentId ? 'ΤΡ' : contract.label.toLocaleLowerCase('el').includes('μελέτ') ? 'ΜΕ' : 'ΑΠ')
}

export function shortArea(contract: ReportContract, ui: OverviewUi = greekOverviewUi) {
  const label = ui.organization(ui(contract.nodeContext.area.replace(/^Δήμος /, '').replace(/^Περιφερειακή Ενότητα /, '')))
  return label.length > 26 ? `${label.slice(0, 25).replace(/[,\s]+$/, '')}…` : label
}

