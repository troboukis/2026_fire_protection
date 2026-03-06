import { useDevViewEnabled } from '../lib/devView'

type Props = {
  name: string
  className?: string
}

export default function ComponentTag({ name, className }: Props) {
  const [devViewEnabled] = useDevViewEnabled()
  if (!devViewEnabled) return null

  return (
    <div className={`component-tag${className ? ` ${className}` : ''}`}>
      COMPONENT: {name}
    </div>
  )
}
