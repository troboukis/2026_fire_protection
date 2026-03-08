import { DEV_VIEW_AVAILABLE, useDevViewEnabled } from '../lib/devView'

export default function DevViewToggle() {
  if (!DEV_VIEW_AVAILABLE) return null
  const [enabled, setEnabled] = useDevViewEnabled()
  return (
    <button
      type="button"
      onClick={() => setEnabled(!enabled)}
      aria-pressed={enabled}
      title="Toggle Dev View"
      style={{
        position: 'fixed',
        top: '10px',
        left: '10px',
        zIndex: 5000,
        border: '1px solid #111',
        background: enabled ? '#ffeb3b' : '#f7f5ee',
        color: '#111',
        fontFamily: 'IBM Plex Mono, SFMono-Regular, Menlo, monospace',
        fontSize: '11px',
        lineHeight: 1.2,
        padding: '6px 8px',
        cursor: 'pointer',
      }}
    >
      {enabled ? 'DEV: ON' : 'DEV: OFF'}
    </button>
  )
}
