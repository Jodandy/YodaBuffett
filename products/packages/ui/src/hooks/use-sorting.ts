import { useState, useMemo } from 'react'
import type { ColumnSort } from '@yodabuffett/types'
import { getNestedValue } from '../utils/get-nested-value'

export function useSorting<T>(items: T[]) {
  const [sortBy, setSortBy] = useState<ColumnSort | null>(null)

  const sortedItems = useMemo(() => {
    if (!sortBy) return items
    return [...items].sort((a, b) => {
      const aValue = getNestedValue(a, sortBy.column)
      const bValue = getNestedValue(b, sortBy.column)

      if (aValue === null || aValue === undefined) return 1
      if (bValue === null || bValue === undefined) return -1

      const comparison =
        typeof aValue === 'number' && typeof bValue === 'number'
          ? aValue - bValue
          : String(aValue).localeCompare(String(bValue))

      return sortBy.direction === 'desc' ? -comparison : comparison
    })
  }, [items, sortBy])

  const handleSort = (column: string) => {
    setSortBy((prev) => {
      if (!prev || prev.column !== column) {
        return { column, direction: 'desc' }
      }
      if (prev.direction === 'desc') {
        return { column, direction: 'asc' }
      }
      return null
    })
  }

  return { sortBy, sortedItems, handleSort }
}
