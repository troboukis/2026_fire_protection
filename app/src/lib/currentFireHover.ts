export const CURRENT_FIRE_HOVER_EVENT = 'firewatch:current-fire-hover'

export type CurrentFireHoverDetail = {
  incidentKey: string | null
}

export function dispatchCurrentFireHover(incidentKey: string | null) {
  window.dispatchEvent(new CustomEvent<CurrentFireHoverDetail>(CURRENT_FIRE_HOVER_EVENT, {
    detail: { incidentKey },
  }))
}
