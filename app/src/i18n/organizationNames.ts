import municipalities from './catalogs/municipalityNames.en.json'
import regions from './catalogs/regionNames.en.json'
import first from './catalogs/organizationNames1.en.json'
import second from './catalogs/organizationNames2.en.json'
import third from './catalogs/organizationNames3.en.json'
import fourth from './catalogs/organizationNames4.en.json'
import fifth from './catalogs/organizationNames5.en.json'
import aliases from './organizationAliases.json'

const normalize = (value: string) => value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLocaleUpperCase('el-GR').replace(/\s+/g, ' ').trim()
const messages: Record<string, string> = { ...municipalities, ...regions, ...first, ...second, ...third, ...fourth, ...fifth }
const names = new Map(Object.entries(aliases).map(([source, alias]) => [source,
  alias.prefix === 'municipality' ? `Municipality of ${messages[alias.key]}` : messages[alias.key],
]))

// Only the reviewed, explicit name aliases are accepted. No fuzzy entity
// matching, database writes, translated identifiers or live API calls.
export function translatedOrganizationName(source: string): string | undefined {
  return names.get(normalize(source))
}
