import { useEffect, useState, type CSSProperties } from 'react'
import { Link } from 'react-router-dom'
import ComponentTag from '../../../components/ComponentTag'
import './AnalysisPromo.css'

const slides = [
  {
    eyebrow: 'Στατιστική ανάλυση',
    title: 'Μία στις δύο συμβάσεις με Απευθείας Ανάλυση',
    detail: 'Η πλειονότητα των συμβάσεων έχει αξία που φτάνει στο νόμιμο όριο των απευθείας αναθέσεων, δηλαδή τις 30.000 ευρώ.',
    to: '/analysis/statistics',
    visual: 'statistics',
  },
  {
    eyebrow: 'Δυτική Αττική',
    title: 'Εξερευνήστε το δίκτυο συμβάσεων έργων AntiNERO',
    detail: 'Πότε έπρεπε να ολοκληρωθεί κάθε έργο και πότε ολοκληρώθηκε στην πράξη, σύμφωνα με τα διαθέσιμα δημόσια έγγραφα στη Διαύγεια.',
    to: '/analysis/antinero-west-attica',
    visual: 'network',
  },
] as const

const SLIDE_DURATION_MS = 6000

// Decorative illustrations, not charts of research data.
function PromoIllustration({ visual }: { visual: typeof slides[number]['visual'] }) {
  return (
    <svg className="analysis-promo__illustration" viewBox="0 0 560 140" aria-hidden="true" focusable="false">
      {visual === 'statistics' ? (
        <g>
          {[36, 58, 48, 82, 71, 105, 122].map((height, index) => (
            <rect
              key={index}
              className="analysis-promo__bar"
              x={40 + index * 70}
              y={135 - height}
              width="42"
              height={height}
              style={{ '--item-delay': `${index * 75}ms` } as CSSProperties}
            />
          ))}
          <path className="analysis-promo__baseline" d="M24 135H540" />
        </g>
      ) : (
        <g>
          <path className="analysis-promo__connections" pathLength="1" d="M60 75L188 28L314 70L470 24M60 75L198 120L314 70L486 117M188 28L198 120M314 70L506 72M470 24L506 72L486 117" />
          {[[30, 75], [94, 28], [99, 120], [157, 70], [235, 24], [253, 72], [243, 117]].map(([cx, cy], index) => (
            <circle
              key={index}
              className={`analysis-promo__node${index === 3 ? ' analysis-promo__node--central' : ''}`}
              cx={cx * 2}
              cy={cy}
              r={index === 3 ? 13 : 7}
              style={{ '--item-delay': `${index * 85}ms` } as CSSProperties}
            />
          ))}
        </g>
      )}
    </svg>
  )
}

export default function AnalysisPromo() {
  const [activeIndex, setActiveIndex] = useState(0)
  const [paused, setPaused] = useState(false)
  const [hovered, setHovered] = useState(false)
  const [focused, setFocused] = useState(false)
  const [reducedMotion, setReducedMotion] = useState(() => (
    typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches
  ))
  const slide = slides[activeIndex]

  useEffect(() => {
    const media = window.matchMedia('(prefers-reduced-motion: reduce)')
    const update = () => setReducedMotion(media.matches)
    update()
    media.addEventListener('change', update)
    return () => media.removeEventListener('change', update)
  }, [])

  useEffect(() => {
    if (paused || hovered || focused || reducedMotion) return
    const timer = window.setTimeout(() => {
      setActiveIndex((index) => (index + 1) % slides.length)
    }, SLIDE_DURATION_MS)
    return () => window.clearTimeout(timer)
  }, [activeIndex, paused, hovered, focused, reducedMotion])

  return (
    <section
      className="analysis-promo"
      aria-label="Προτεινόμενες αναλύσεις"
      aria-roledescription="καρουζέλ"
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      onFocusCapture={() => setFocused(true)}
      onBlurCapture={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget as Node | null)) setFocused(false)
      }}
    >
      <ComponentTag name="AnalysisPromo" />
      <div className="analysis-promo__header">
        <span className="analysis-promo__eyebrow">{slide.eyebrow}</span>
        <div className="analysis-promo__controls" role="group" aria-label="Επιλογή ανάλυσης">
          {slides.map((item, index) => (
            <button
              key={item.to}
              type="button"
              className="analysis-promo__selector"
              aria-label={item.title}
              aria-pressed={index === activeIndex}
              onClick={() => setActiveIndex(index)}
            >
              {String(index + 1).padStart(2, '0')}
            </button>
          ))}
          {!reducedMotion && (
            <button
              type="button"
              className="analysis-promo__pause"
              aria-label={paused ? 'Συνέχεια αυτόματης εναλλαγής' : 'Παύση αυτόματης εναλλαγής'}
              onClick={() => setPaused((value) => !value)}
            >
              <svg viewBox="0 0 16 16" aria-hidden="true" focusable="false">
                {paused ? <path d="M5 3L13 8L5 13Z" /> : <path d="M4 3H6V13H4ZM10 3H12V13H10Z" />}
              </svg>
            </button>
          )}
        </div>
      </div>
      <div aria-live={paused || focused || reducedMotion ? 'polite' : 'off'} aria-atomic="true">
        <Link key={slide.to} className="analysis-promo__slide" to={slide.to}>
          <div className="analysis-promo__copy">
            <h2>{slide.title}</h2>
            <p>{slide.detail}</p>
          </div>
          <div className="analysis-promo__visual">
            <PromoIllustration visual={slide.visual} />
            <span className="analysis-promo__action">
              <span>Περισσότερα</span>
              <span className="analysis-promo__arrow" aria-hidden="true">↗</span>
            </span>
          </div>
        </Link>
      </div>
    </section>
  )
}
