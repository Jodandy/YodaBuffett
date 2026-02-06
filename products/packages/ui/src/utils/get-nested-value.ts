/* eslint-disable @typescript-eslint/no-explicit-any */

export function getNestedValue(obj: any, path: string): any {
  return path.split('.').reduce((current, key) => current?.[key], obj)
}
