type ErrorLike = {
  name?: unknown
  message?: unknown
  details?: unknown
  hint?: unknown
  cause?: unknown
}

function includesAbortSignal(value: unknown): boolean {
  return typeof value === 'string' && /abort(?:ed|error)|signal is aborted/i.test(value)
}

export function isAbortError(error: unknown): boolean {
  if (error instanceof DOMException && error.name === 'AbortError') return true
  if (!error || typeof error !== 'object') return false

  const candidate = error as ErrorLike
  if (candidate.name === 'AbortError') return true
  if (includesAbortSignal(candidate.message)) return true
  if (includesAbortSignal(candidate.details)) return true
  if (includesAbortSignal(candidate.hint)) return true

  return isAbortError(candidate.cause)
}
