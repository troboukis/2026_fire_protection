import type { CSSProperties } from 'react'
import { useDevViewEnabled } from '../lib/devView'

type Props = {
  name: string
  className?: string
  kind?: 'COMPONENT' | 'CLASS'
  style?: CSSProperties
}

export default function ComponentTag({ name, className, kind = 'COMPONENT', style }: Props) {
  const [devViewEnabled] = useDevViewEnabled()
  if (!devViewEnabled) return null

  return (
    <div className={`component-tag${className ? ` ${className}` : ''}`} style={style}>
      {kind}: {name}
    </div>
  )
}
