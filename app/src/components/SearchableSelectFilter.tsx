import { useEffect, useId, useRef, useState } from 'react'
import { MAX_SEARCH_QUERY_LENGTH, normalizeSearchText } from '../lib/searchQuery'

type Props = {
  options: Array<{ value: string; label: string }>
  value: string
  onChange: (value: string) => void
  loading?: boolean
  failed?: boolean
  label: string
  allLabel: string
  loadingLabel: string
  failedLabel: string
  emptyLabel: string
  className: string
}

export default function SearchableSelectFilter({ options, value, onChange, loading = false, failed = false, label, allLabel, loadingLabel, failedLabel, emptyLabel, className }: Props) {
  const selectId = useId()
  const suggestionsId = useId()
  const inputRef = useRef<HTMLInputElement>(null)
  const listRef = useRef<HTMLDivElement>(null)
  const [editing, setEditing] = useState(false)
  const [query, setQuery] = useState('')
  const [activeIndex, setActiveIndex] = useState(0)
  const [suggestionsVisible, setSuggestionsVisible] = useState(false)
  const normalizedQuery = normalizeSearchText(query)
  const matches = options.filter((option) => normalizeSearchText(option.label).includes(normalizedQuery))
  const choices = normalizedQuery ? matches : [{ value: '', label: allLabel }, ...matches]

  const selectedLabel = options.find((option) => option.value === value)?.label ?? value
  const active = Math.min(activeIndex, Math.max(0, choices.length - 1))

  useEffect(() => {
    const list = listRef.current
    const option = list?.children[active] as HTMLElement | undefined
    if (!list || !option) return
    if (option.offsetTop < list.scrollTop) list.scrollTop = option.offsetTop
    else if (option.offsetTop + option.offsetHeight > list.scrollTop + list.clientHeight) {
      list.scrollTop = option.offsetTop + option.offsetHeight - list.clientHeight
    }
  }, [active, suggestionsVisible, query])

  const choose = (nextValue: string) => {
    onChange(nextValue)
    setQuery('')
    setEditing(false)
    setSuggestionsVisible(false)
  }

  const openOptions = () => {
    inputRef.current?.focus()
    setSuggestionsVisible(true)
  }

  return (
    <div className={`${className} searchable-select-filter`}>
      <select
        id={selectId}
        className="contracts-filter"
        value={value}
        aria-hidden="true"
        tabIndex={-1}
        aria-busy={loading}
        title={selectedLabel || allLabel}
        onChange={(event) => choose(event.target.value)}
        onPointerDown={(event) => { event.preventDefault(); openOptions() }}
        onMouseDown={(event) => event.preventDefault()}
      >
        <option value="">{allLabel}</option>
        {value && !matches.some((option) => option.value === value) && <option value={value} hidden>{selectedLabel}</option>}
        {matches.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
        {matches.length === 0 && (
          <option disabled>
            {loading ? loadingLabel : failed ? failedLabel : emptyLabel}
          </option>
        )}
      </select>
      <input
        ref={inputRef}
        className="searchable-select-filter__search"
        type="text"
        role="combobox"
        aria-label={`Αναζήτηση: ${label}`}
        aria-autocomplete="list"
        aria-expanded={suggestionsVisible}
        aria-controls={suggestionsVisible ? suggestionsId : selectId}
        aria-activedescendant={suggestionsVisible && choices.length ? `${suggestionsId}-${active}` : undefined}
        autoComplete="off"
        autoCorrect="off"
        autoCapitalize="off"
        spellCheck={false}
        value={editing || query ? query : selectedLabel}
        placeholder={selectedLabel || allLabel}
        title={`Πληκτρολογήστε και ανοίξτε τη λίστα από το βέλος για επιλογή: ${label}`}
        maxLength={MAX_SEARCH_QUERY_LENGTH}
        onFocus={() => { setEditing(true); setQuery(''); setActiveIndex(0); setSuggestionsVisible(true) }}
        onClick={() => setSuggestionsVisible(true)}
        onBlur={() => { setEditing(false); setSuggestionsVisible(false) }}
        onChange={(event) => { setQuery(event.target.value); setEditing(true); setActiveIndex(0); setSuggestionsVisible(true) }}
        onKeyDown={(event) => {
          if (suggestionsVisible && (event.key === 'ArrowDown' || event.key === 'ArrowUp')) {
            event.preventDefault()
            if (choices.length) setActiveIndex((active + (event.key === 'ArrowDown' ? 1 : -1) + choices.length) % choices.length)
          } else if (suggestionsVisible && event.key === 'Enter') {
            event.preventDefault()
            if (choices[active]) choose(choices[active].value)
          } else if (event.key === 'ArrowDown' || event.key === 'Enter') {
            event.preventDefault()
            openOptions()
          } else if (event.key === 'Escape') {
            setQuery('')
            setEditing(false)
            setSuggestionsVisible(false)
            event.currentTarget.blur()
          }
        }}
      />
      {suggestionsVisible && (
        <div ref={listRef} id={suggestionsId} className="searchable-select-filter__suggestions" role="listbox" aria-label={label} aria-busy={loading}>
          {choices.map((option, index) => (
            <button
              key={option.value}
              id={`${suggestionsId}-${index}`}
              type="button"
              role="option"
              aria-selected={option.value === value}
              className={index === active ? 'is-active' : undefined}
              onMouseDown={(event) => event.preventDefault()}
              onMouseEnter={() => setActiveIndex(index)}
              onClick={() => choose(option.value)}
            >
              <span aria-hidden="true">{option.value === value ? '✓' : ''}</span>
              {option.label}
            </button>
          ))}
          {matches.length === 0 && (
            <div className="searchable-select-filter__empty" role="status">
              {loading ? loadingLabel : failed ? failedLabel : emptyLabel}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
