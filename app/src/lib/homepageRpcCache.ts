type CacheEntry<T> = {
  data: T
  savedAt: number
  expiresAt: number
}

type CacheOptions = {
  ttlMs?: number
  staleTtlMs?: number
}

type RetryOptions = {
  retries?: number
  retryDelayMs?: number
}

const STORAGE_PREFIX = 'homepage-rpc-cache:'
const DEFAULT_TTL_MS = 60_000
const DEFAULT_STALE_TTL_MS = 6 * 60 * 60 * 1000
const memoryCache = new Map<string, CacheEntry<unknown>>()
const inFlightCache = new Map<string, Promise<unknown>>()

function serializeStable(value: unknown): string {
  if (value == null) return 'null'
  if (Array.isArray(value)) return `[${value.map((item) => serializeStable(item)).join(',')}]`
  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b))
    return `{${entries.map(([key, nested]) => `${JSON.stringify(key)}:${serializeStable(nested)}`).join(',')}}`
  }
  return JSON.stringify(value)
}

function buildStorageKey(cacheKey: string): string {
  return `${STORAGE_PREFIX}${cacheKey}`
}

function readStoredCache<T>(cacheKey: string, staleTtlMs: number): CacheEntry<T> | null {
  if (typeof window === 'undefined') return null

  try {
    const raw = window.localStorage.getItem(buildStorageKey(cacheKey))
    if (!raw) return null

    const parsed = JSON.parse(raw) as CacheEntry<T>
    if (!parsed || typeof parsed.savedAt !== 'number' || typeof parsed.expiresAt !== 'number') return null
    if (Date.now() - parsed.savedAt > staleTtlMs) return null
    return parsed
  } catch {
    return null
  }
}

function writeStoredCache<T>(cacheKey: string, entry: CacheEntry<T>) {
  if (typeof window === 'undefined') return

  try {
    window.localStorage.setItem(buildStorageKey(cacheKey), JSON.stringify(entry))
  } catch {
    // Ignore storage quota and serialization failures.
  }
}

export function createHomepageRpcCacheKey(name: string, params?: unknown): string {
  return params === undefined ? name : `${name}:${serializeStable(params)}`
}

export async function loadCachedHomepageRpc<T>(
  cacheKey: string,
  loader: () => Promise<T>,
  options: CacheOptions = {},
): Promise<T> {
  const ttlMs = options.ttlMs ?? DEFAULT_TTL_MS
  const staleTtlMs = options.staleTtlMs ?? DEFAULT_STALE_TTL_MS
  const now = Date.now()
  const memoryEntry = memoryCache.get(cacheKey) as CacheEntry<T> | undefined

  if (memoryEntry && memoryEntry.expiresAt > now) return memoryEntry.data

  const storedEntry = readStoredCache<T>(cacheKey, staleTtlMs)
  if (storedEntry && storedEntry.expiresAt > now) {
    memoryCache.set(cacheKey, storedEntry)
    return storedEntry.data
  }

  const inFlight = inFlightCache.get(cacheKey) as Promise<T> | undefined
  if (inFlight) return inFlight

  const request = (async () => {
    try {
      const data = await loader()
      const entry: CacheEntry<T> = {
        data,
        savedAt: Date.now(),
        expiresAt: Date.now() + ttlMs,
      }
      memoryCache.set(cacheKey, entry)
      writeStoredCache(cacheKey, entry)
      return data
    } catch (error) {
      const staleEntry = storedEntry ?? readStoredCache<T>(cacheKey, staleTtlMs)
      if (staleEntry) {
        memoryCache.set(cacheKey, staleEntry)
        return staleEntry.data
      }
      throw error
    } finally {
      inFlightCache.delete(cacheKey)
    }
  })()

  inFlightCache.set(cacheKey, request)
  return request
}

export async function retryHomepageRpc<T>(
  loader: () => Promise<T>,
  options: RetryOptions = {},
): Promise<T> {
  const retries = options.retries ?? 1
  const retryDelayMs = options.retryDelayMs ?? 400

  let lastError: unknown

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      return await loader()
    } catch (error) {
      lastError = error
      if (attempt === retries) break
      await new Promise((resolve) => window.setTimeout(resolve, retryDelayMs * (attempt + 1)))
    }
  }

  throw lastError
}
