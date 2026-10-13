import SearchableSelectFilter from './SearchableSelectFilter'

type Props = {
  options: string[]
  value: string
  onChange: (value: string) => void
  loading?: boolean
  failed?: boolean
}

export default function OrganizationFilter(props: Props) {
  return (
    <SearchableSelectFilter
      {...props}
      options={props.options.map((label) => ({ value: label, label }))}
      className="contracts-filter--organization"
      label="Φορέας"
      allLabel="Όλοι οι φορείς"
      loadingLabel="Φόρτωση φορέων…"
      failedLabel="Δεν ήταν δυνατή η φόρτωση των φορέων."
      emptyLabel="Δεν βρέθηκαν φορείς."
    />
  )
}
