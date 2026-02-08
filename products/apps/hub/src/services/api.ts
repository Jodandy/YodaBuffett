/**
 * Hub API Client
 * Configured axios instance for backend communication
 */

import { createApiClient } from '@yodabuffett/api-client'

export const api = createApiClient({
  baseURL: '/api/v1',
  timeout: 30000,
})

// Helper to convert snake_case response to camelCase
export function toCamelCase<T>(obj: unknown): T {
  if (obj === null || obj === undefined) return obj as T
  if (Array.isArray(obj)) return obj.map(toCamelCase) as T
  if (typeof obj !== 'object') return obj as T

  const newObj: Record<string, unknown> = {}
  for (const key in obj as Record<string, unknown>) {
    const camelKey = key.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase())
    newObj[camelKey] = toCamelCase((obj as Record<string, unknown>)[key])
  }
  return newObj as T
}

// Helper to convert camelCase request to snake_case
export function toSnakeCase<T>(obj: unknown): T {
  if (obj === null || obj === undefined) return obj as T
  if (Array.isArray(obj)) return obj.map(toSnakeCase) as T
  if (typeof obj !== 'object') return obj as T

  const newObj: Record<string, unknown> = {}
  for (const key in obj as Record<string, unknown>) {
    const snakeKey = key.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`)
    newObj[snakeKey] = toSnakeCase((obj as Record<string, unknown>)[key])
  }
  return newObj as T
}
