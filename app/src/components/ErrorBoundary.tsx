import { Component, type ErrorInfo, type ReactNode } from 'react'

type Props = {
  fallback?: ReactNode
  children: ReactNode
}

type State = {
  hasError: boolean
}

export default class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false }

  static getDerivedStateFromError(): State {
    return { hasError: true }
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    // Keep a console trail for fast debugging in browser devtools.
    console.error('[ErrorBoundary] component crash', error, info)
  }

  render(): ReactNode {
    if (this.state.hasError) {
      return this.props.fallback ?? <div className="ca-empty-note">Η ενότητα ανάλυσης δεν είναι διαθέσιμη προσωρινά.</div>
    }
    return this.props.children
  }
}

