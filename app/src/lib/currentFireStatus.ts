export const CURRENT_FIRE_STATUS_LABELS: Record<string, string> = {
  'ΜΕΡΙΚΟΣ ΕΛΕΓΧΟΣ': 'ΥΠΟ ΜΕΡΙΚΟ ΕΛΕΓΧΟ',
  'ΠΛΗΡΗΣ ΕΛΕΓΧΟΣ': 'ΥΠΟ ΠΛΗΡΗ ΕΛΕΓΧΟ',
}

export const CURRENT_FIRE_STATUS_COLORS: Record<string, string> = {
  'ΣΕ ΕΞΕΛΙΞΗ': '#b91c1c',
  'ΥΠΟ ΜΕΡΙΚΟ ΕΛΕΓΧΟ': '#c2680a',
  'ΥΠΟ ΠΛΗΡΗ ΕΛΕΓΧΟ': '#166534',
}

export const CURRENT_FIRE_STATUS_ORDER: Record<string, number> = {
  'ΣΕ ΕΞΕΛΙΞΗ': 0,
  'ΥΠΟ ΜΕΡΙΚΟ ΕΛΕΓΧΟ': 1,
  'ΥΠΟ ΠΛΗΡΗ ΕΛΕΓΧΟ': 2,
}

export function normalizeCurrentFireStatus(value: string | null): string | null {
  const cleaned = value?.trim()
  if (!cleaned || cleaned === 'ΛΗΞΗ') return null
  return CURRENT_FIRE_STATUS_LABELS[cleaned] ?? cleaned
}

export function getCurrentFireStatusColor(value: string | null): string {
  const status = normalizeCurrentFireStatus(value)
  return (status && CURRENT_FIRE_STATUS_COLORS[status]) || CURRENT_FIRE_STATUS_COLORS['ΣΕ ΕΞΕΛΙΞΗ']
}
