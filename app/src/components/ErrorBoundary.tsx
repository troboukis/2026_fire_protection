import { Component, type ErrorInfo, type ReactNode } from 'react'
import { useOverviewUi } from '../i18n/overview'
import { logError } from '../lib/logger'

type Props = {
  fallback?: ReactNode
  children: ReactNode
}

type State = {
  hasError: boolean
}

export function DefaultFallback() {
  const ui = useOverviewUi()
  return <div className="ca-empty-note">{ui('Η ενότητα ανάλυσης δεν είναι διαθέσιμη προσωρινά.')}</div>
}

export default class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false }

  static getDerivedStateFromError(): State {
    return { hasError: true }
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    if (!import.meta.env.DEV) return
    logError('[ErrorBoundary] component crash', error, info)
  }

  render(): ReactNode {
    if (this.state.hasError) {
      return this.props.fallback ?? <DefaultFallback />
    }
    return this.props.children
  }
}
