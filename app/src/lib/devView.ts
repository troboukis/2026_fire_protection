import { useEffect, useState } from 'react'

const DEV_VIEW_KEY = 'project_pyr_dev_view'
const DEV_VIEW_EVENT = 'project-pyr-dev-view-change'
export const DEV_VIEW_AVAILABLE = import.meta.env.VITE_ENABLE_DEV_VIEW !== '0'

function readDevViewFlag(): boolean {
  if (typeof window === 'undefined') return false
  if (!DEV_VIEW_AVAILABLE) return false
  return window.localStorage.getItem(DEV_VIEW_KEY) === '1'
}

export function setDevViewEnabled(next: boolean): void {
  if (typeof window === 'undefined') return
  if (!DEV_VIEW_AVAILABLE) return
  window.localStorage.setItem(DEV_VIEW_KEY, next ? '1' : '0')
  window.dispatchEvent(new CustomEvent<boolean>(DEV_VIEW_EVENT, { detail: next }))
}

export function useDevViewEnabled(): [boolean, (next: boolean) => void] {
  const [enabled, setEnabled] = useState<boolean>(() => readDevViewFlag())

  useEffect(() => {
    const onStorage = (e: StorageEvent) => {
      if (e.key !== DEV_VIEW_KEY) return
      setEnabled(readDevViewFlag())
    }
    const onCustom = () => setEnabled(readDevViewFlag())

    window.addEventListener('storage', onStorage)
    window.addEventListener(DEV_VIEW_EVENT, onCustom as EventListener)
    return () => {
      window.removeEventListener('storage', onStorage)
      window.removeEventListener(DEV_VIEW_EVENT, onCustom as EventListener)
    }
  }, [])

  const update = (next: boolean) => {
    if (!DEV_VIEW_AVAILABLE) {
      setEnabled(false)
      return
    }
    setDevViewEnabled(next)
    setEnabled(next)
  }

  return [enabled, update]
}
