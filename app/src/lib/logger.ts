const isDev = import.meta.env.DEV

export function logDebug(message: string, ...args: unknown[]): void {
  if (!isDev) return
  console.debug(message, ...args)
}

export function logWarn(message: string, ...args: unknown[]): void {
  if (!isDev) return
  console.warn(message, ...args)
}

export function logError(message: string, ...args: unknown[]): void {
  if (!isDev) return
  console.error(message, ...args)
}
