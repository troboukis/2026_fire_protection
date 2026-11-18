import { createElement } from 'react'
import { act, create, type ReactTestRenderer } from 'react-test-renderer'
import { MemoryRouter, Route, Routes, useLocation, useNavigate, type NavigateFunction } from 'react-router-dom'
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import Layout from '../components/Layout'
import { localizeCookieConsentLinks } from '../cookieConsent'
import RouteLanguageProvider from './RouteLanguageProvider'
import { pageRoutes } from './pageRoutes'
import { LocalizedLink } from './LocalizedLink'
import { useLocalizedNavigate } from './useLocalizedNavigate'
import { LocalizedNavLink } from './LocalizedLink'

vi.mock('../cookieConsent', () => ({ localizeCookieConsentLinks: vi.fn(), syncCookieConsentLanguage: vi.fn(), showCookiePreferences: vi.fn() }))
vi.mock('../lib/devView', () => ({ DEV_VIEW_AVAILABLE: false, useDevViewEnabled: () => [false, () => {}] }))
vi.mock('../lib/supabase', () => {
  function emptyQuery(): object {
    const result = { data: [], error: null, count: 0 }
    const query: object = new Proxy({}, {
      get: (_target, key) => key === 'then'
        ? Promise.resolve(result).then.bind(Promise.resolve(result))
        : () => query,
    })
    return query
  }
  return { supabase: { from: vi.fn(emptyQuery), rpc: vi.fn(emptyQuery) } }
})

let tree: ReactTestRenderer | undefined
let navigate: NavigateFunction
let currentLocation = ''

function LocationProbe() {
  const location = useLocation()
  navigate = useNavigate()
  currentLocation = location.pathname + location.search + location.hash
  return null
}

function clickEvent() {
  return {
    button: 0, metaKey: false, ctrlKey: false, shiftKey: false, altKey: false, defaultPrevented: false,
    preventDefault() { this.defaultPrevented = true },
  }
}

async function open(url: string) {
  await act(async () => {
    tree = create(createElement(MemoryRouter, { initialEntries: [url] },
      createElement(RouteLanguageProvider, { children: [
        createElement(LocationProbe, { key: 'probe' }),
        createElement(Routes, { key: 'routes' }, createElement(Route, { element: createElement(Layout) },
          pageRoutes.map(page => createElement(Route, { key: page.path, path: page.path, element: page.element })),
        )),
      ] }),
    ))
    if (url.includes('/diavgeia')) await import('../pages/DiavgeiaPage')
    else await import('../pages/ContractsPage')
  })
}

async function switchLanguage(text: string) {
  const link = tree!.root.findAllByType('a').find(anchor => anchor.props.children === text)
  expect(link).toBeDefined()
  await act(async () => link!.props.onClick(clickEvent()))
}

const input = (className: string) => tree!.root.findAllByType('input').find(element => element.props.className?.includes(className))!

beforeAll(() => {
  vi.stubGlobal('__LAST_COMMIT_ISO__', '2026-11-17T12:30:00Z')
  vi.stubGlobal('document', { documentElement: { lang: 'el' } })
})
afterAll(() => vi.unstubAllGlobals())
afterEach(async () => {
  if (tree) await act(async () => tree!.unmount())
  tree = undefined
})

describe('real page language navigation', () => {
  it('keeps edited contract filters across the language switch, back and forward', async () => {
    const suffix = '?q=original&dateFrom=2026-11-01&dateTo=2026-11-17&organizationKey=100&organizationKey=200&tag=a%20b&tag=a+b#sources'
    await open(`/contracts${suffix}`)
    await act(async () => {
      input('contracts-filter--search').props.onChange({ target: { value: 'Αλλαγμένο τοπικό φίλτρο' } })
      input('contracts-filter--amount').props.onChange({ target: { value: '9876' } })
      input('contracts-filter--date-from').props.onChange({ target: { value: '2026-10-01' } })
    })
    await switchLanguage('English')
    expect(currentLocation).toBe(`/en/contracts${suffix}`)
    expect(document.documentElement.lang).toBe('en')
    expect(input('contracts-filter--search').props.value).toBe('Αλλαγμένο τοπικό φίλτρο')
    expect(input('contracts-filter--amount').props.value).toBe('9876')
    expect(input('contracts-filter--date-from').props.value).toBe('2026-10-01')
    await act(async () => navigate(-1))
    expect(currentLocation).toBe(`/contracts${suffix}`)
    expect(input('contracts-filter--search').props.value).toBe('Αλλαγμένο τοπικό φίλτρο')
    await act(async () => navigate(1))
    expect(currentLocation).toBe(`/en/contracts${suffix}`)
    expect(input('contracts-filter--amount').props.value).toBe('9876')
    expect(localizeCookieConsentLinks).toHaveBeenCalledWith('en')
  })

  it('keeps local Diavgeia filters when switching to Greek and back', async () => {
    const suffix = '?q=original&allDates=1&municipalityKey=9230#sources'
    await open(`/en/diavgeia${suffix}`)
    await act(async () => {
      input('contracts-filter--search').props.onChange({ target: { value: 'Νέο φίλτρο' } })
      input('contracts-filter--date-from').props.onChange({ target: { value: '2026-01-01' } })
    })
    await switchLanguage('Ελληνικά')
    expect(currentLocation).toBe(`/diavgeia${suffix}`)
    expect(document.documentElement.lang).toBe('el')
    expect(input('contracts-filter--search').props.value).toBe('Νέο φίλτρο')
    expect(input('contracts-filter--date-from').props.value).toBe('2026-01-01')
    await switchLanguage('English')
    expect(currentLocation).toBe(`/en/diavgeia${suffix}`)
    expect(input('contracts-filter--search').props.value).toBe('Νέο φίλτρο')
  })

  it('renders English navigation on a directly opened municipality link', async () => {
    await open('/en/contracts?municipalityKey=9230')
    const links = tree!.root.findAllByType('a')
    expect(links.some(anchor => anchor.props.href === '/en/municipalities' && anchor.props.children === 'Municipalities')).toBe(true)
    expect(links.some(anchor => anchor.props.href === '/en/privacy')).toBe(true)
    expect(currentLocation).toBe('/en/contracts?municipalityKey=9230')
  })

  it('keeps the English root navigation inactive on a nested English page', async () => {
    await open('/en/contracts')
    const home = tree!.root.findAllByType('a').find(anchor => anchor.props['data-nav-item'] === 'home')!
    expect(home.props.href).toBe('/en/')
    expect(home.props['aria-current']).toBeUndefined()
  })
})

describe('localized link and callback helpers', () => {
  it.each(['/en', '/en/'])('marks the home link active on the English root %s', async path => {
    await act(async () => {
      tree = create(createElement(MemoryRouter, { initialEntries: [path] }, createElement(RouteLanguageProvider, {
        children: createElement(LocalizedNavLink, { to: '/' }, 'Home'),
      })))
    })
    expect(tree!.root.findByType('a').props['aria-current']).toBe('page')
  })

  it('keeps reloadDocument on standalone links and prefixes imperative navigation', async () => {
    function Probe() {
      const go = useLocalizedNavigate()
      return createElement('div', null,
        createElement(LocalizedLink, { to: '/analysis/west-attica-fire-2026/' }, 'Standalone'),
        createElement('button', { onClick: () => go('/municipalities?municipality=9230') }, 'Open municipality'),
      )
    }
    await act(async () => {
      tree = create(createElement(MemoryRouter, { initialEntries: ['/en/'] }, createElement(RouteLanguageProvider, { children: [
        createElement(LocationProbe, { key: 'location' }), createElement(Probe, { key: 'probe' }),
      ] })))
    })
    const link = tree!.root.findByType('a')
    expect(link.props.href).toBe('/en/analysis/west-attica-fire-2026/')
    await act(async () => tree!.root.findByType('button').props.onClick())
    expect(currentLocation).toBe('/en/municipalities?municipality=9230')
  })
})
