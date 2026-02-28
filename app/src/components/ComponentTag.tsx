type Props = {
  name: string
  className?: string
}

export default function ComponentTag({ name, className }: Props) {
  return (
    <div className={`component-tag${className ? ` ${className}` : ''}`}>
      COMPONENT: {name}
    </div>
  )
}
