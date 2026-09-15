import { beforeEach, describe, expect, it, vi } from 'vitest'
import { loadHistoricalCopernicus } from './historicalCopernicus'

const query = vi.hoisted(() => ({
  from: vi.fn(),
  select: vi.fn(),
  gte: vi.fn(),
  lte: vi.fn(),
  order: vi.fn(),
  range: vi.fn(),
  abortSignal: vi.fn(),
}))

vi.mock('./supabase', () => ({ supabase: query }))

const start = '2025-01-01T00:00:00'
const end = '2026-09-15T23:59:59'
const rows = (offset: number, length: number) => Array.from({ length }, (_, index) => ({
  copernicus_id: offset + index,
  firedate: '2025-08-12T12:00:00',
}))

beforeEach(() => {
  vi.resetAllMocks()
  for (const method of ['from', 'select', 'gte', 'lte', 'order', 'range'] as const) {
    query[method].mockReturnValue(query)
  }
})

describe('loadHistoricalCopernicus', () => {
  it('loads the complete period across multiple pages with stable ordering', async () => {
    const allRows = rows(0, 1201)
    query.abortSignal
      .mockResolvedValueOnce({ data: allRows.slice(0, 500), error: null, count: 1201 })
      .mockResolvedValueOnce({ data: allRows.slice(500, 1000), error: null, count: 1201 })
      .mockResolvedValueOnce({ data: allRows.slice(1000), error: null, count: 1201 })
    const signal = new AbortController().signal

    await expect(loadHistoricalCopernicus(start, end, signal)).resolves.toEqual(allRows)
    expect(query.range.mock.calls).toEqual([[0, 499], [500, 999], [1000, 1499]])
    expect(query.from.mock.calls).toEqual([['copernicus'], ['copernicus'], ['copernicus']])
    expect(query.gte.mock.calls).toEqual(Array(3).fill(['firedate', start]))
    expect(query.lte.mock.calls).toEqual(Array(3).fill(['firedate', end]))
    expect(query.order.mock.calls).toEqual(Array(3).fill([
      ['firedate', { ascending: false }],
      ['copernicus_id', { ascending: false }],
    ]).flat())
    expect(query.abortSignal).toHaveBeenCalledWith(signal)
  })

  it('continues when the server returns fewer rows than the requested page size', async () => {
    query.abortSignal
      .mockResolvedValueOnce({ data: rows(0, 2), error: null, count: 3 })
      .mockResolvedValueOnce({ data: rows(2, 1), error: null, count: 3 })

    await expect(loadHistoricalCopernicus(start, end, new AbortController().signal)).resolves.toEqual(rows(0, 3))
    expect(query.range.mock.calls).toEqual([[0, 499], [2, 501]])
  })

  it('returns an empty successful result when the period has no records', async () => {
    query.abortSignal.mockResolvedValueOnce({ data: [], error: null, count: 0 })
    await expect(loadHistoricalCopernicus(start, end, new AbortController().signal)).resolves.toEqual([])
    expect(query.abortSignal).toHaveBeenCalledTimes(1)
  })

  it('rejects a failed later page instead of returning incomplete totals', async () => {
    const error = new Error('Request failed')
    query.abortSignal
      .mockResolvedValueOnce({ data: rows(0, 500), error: null, count: 501 })
      .mockResolvedValueOnce({ data: null, error, count: null })
    await expect(loadHistoricalCopernicus(start, end, new AbortController().signal)).rejects.toBe(error)
  })

  it('rejects an unexpectedly empty page before the total is reached', async () => {
    query.abortSignal
      .mockResolvedValueOnce({ data: rows(0, 2), error: null, count: 3 })
      .mockResolvedValueOnce({ data: [], error: null, count: 3 })
    await expect(loadHistoricalCopernicus(start, end, new AbortController().signal)).rejects.toThrow('Incomplete')
  })

  it('does not issue requests for an already aborted load', async () => {
    const controller = new AbortController()
    controller.abort()
    await expect(loadHistoricalCopernicus(start, end, controller.signal)).rejects.toMatchObject({ name: 'AbortError' })
    expect(query.from).not.toHaveBeenCalled()
  })

  it('stops loading pages when the component unmounts during a request', async () => {
    const controller = new AbortController()
    query.abortSignal.mockImplementationOnce(async () => {
      controller.abort()
      return { data: rows(0, 500), error: null, count: 501 }
    })
    await expect(loadHistoricalCopernicus(start, end, controller.signal)).rejects.toMatchObject({ name: 'AbortError' })
    expect(query.abortSignal).toHaveBeenCalledTimes(1)
  })
})
