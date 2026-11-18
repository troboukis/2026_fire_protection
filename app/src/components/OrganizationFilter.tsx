import { useProcurementI18n } from '../i18n/useProcurementI18n'
import SearchableSelectFilter from './SearchableSelectFilter'

type Props = {
  options: string[]
  value: string
  onChange: (value: string) => void
  loading?: boolean
  failed?: boolean
}

export default function OrganizationFilter(props: Props) {
  const { p, organization } = useProcurementI18n()
  return (
    <SearchableSelectFilter
      {...props}
      options={props.options.map((label) => ({ value: label, label: organization(label) }))}
      className="contracts-filter--organization"
      label={p('organization')}
      allLabel={p('allOrganizations')}
      loadingLabel={p('loadingOrganizations')}
      failedLabel={p('organizationsFailed')}
      emptyLabel={p('organizationsEmpty')}
    />
  )
}
