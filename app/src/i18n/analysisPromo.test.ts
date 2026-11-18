import { createElement, type ReactNode } from 'react'
import { act, create, type ReactTestRenderer } from 'react-test-renderer'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, describe, expect, it, vi } from 'vitest'
import I18nProvider from './I18nProvider'
import type { AppLanguage } from './languages'
import AnalysisPromo from '../features/analysis/catalog/AnalysisPromo'

vi.mock('../lib/devView', () => ({ DEV_VIEW_AVAILABLE: false, useDevViewEnabled: () => [false], useDevView: () => ({ enabled: false }) }))
function page(language: AppLanguage, children: ReactNode) {
  return createElement(I18nProvider, { language, children: createElement(MemoryRouter, {
    initialEntries: ['/en/analysis?tag=a%20b&tag=a+b#sources'],
  }, children) })
}

describe('language changes within an active analysis promo', () => {
  let tree: ReactTestRenderer | undefined
  afterEach(() => {
    if (tree) act(() => tree!.unmount())
    tree = undefined
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  it('keeps the selected slide, pause state and illustration when changing language', () => {
    vi.useFakeTimers()
    vi.stubGlobal('window', {
      matchMedia: () => ({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() }),
      setTimeout, clearTimeout,
    })
    act(() => { tree = create(page('en', createElement(AnalysisPromo))) })
    const selectors = () => tree!.root.findAll(node => node.props.className === 'analysis-promo__selector')
    expect(selectors()).toHaveLength(4)
    expect(tree!.root.findAllByType('img')).toHaveLength(0)
    act(() => selectors()[2].props.onClick())
    act(() => tree!.root.findByProps({ className: 'analysis-promo__pause' }).props.onClick())
    const shape = () => tree!.root.findAllByType('rect').map(node => [node.props.x, node.props.y, node.props.width, node.props.height])
    const geometry = shape()
    expect(tree!.root.findByType('h2').children).toEqual(['One in two contracts is awarded directly'])
    act(() => { vi.advanceTimersByTime(12000) })
    expect(selectors()[2].props['aria-pressed']).toBe(true)
    act(() => tree!.update(page('el', createElement(AnalysisPromo))))
    expect(tree!.root.findByType('h2').children).toEqual(['Μία στις δύο συμβάσεις με Απευθείας Ανάθεση'])
    expect(selectors()[2].props['aria-pressed']).toBe(true)
    expect(shape()).toEqual(geometry)
    expect(tree!.root.findByProps({ className: 'analysis-promo__pause' }).props['aria-label']).toBe('Συνέχεια αυτόματης εναλλαγής')
    act(() => tree!.update(page('en', createElement(AnalysisPromo))))
    expect(tree!.root.findByProps({ className: 'analysis-promo__pause' }).props['aria-label']).toBe('Resume automatic rotation')
    expect(tree!.root.find(node => node.type === 'a' && node.props.className === 'analysis-promo__slide').props.href).toBe('/en/analysis/statistics')
  })
})
