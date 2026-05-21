type MapTilerLogoProps = {
  className?: string
}

export default function MapTilerLogo({ className }: MapTilerLogoProps) {
  return (
    <a
      className={['maptiler-logo', className].filter(Boolean).join(' ')}
      href="https://www.maptiler.com/"
      target="_blank"
      rel="noreferrer"
      aria-label="MapTiler"
    >
      <img src="https://api.maptiler.com/resources/logo.svg" alt="MapTiler logo" />
    </a>
  )
}
