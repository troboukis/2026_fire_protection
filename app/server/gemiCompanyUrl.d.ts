export function normalizeAfm(value: unknown): string | null

export function getCompanyUrlByAfm(afm: string): Promise<{
  gemiNumber: string
  url: string
}>
