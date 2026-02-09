/**
 * Calendar Hooks
 * React Query hooks for fetching global calendar events
 */

import { useQuery } from '@tanstack/react-query'
import { fetchGlobalCalendar } from '../api'
import type { CalendarFilters } from '../types'

// Query keys
export const calendarKeys = {
  all: ['calendar'] as const,
  global: (filters: CalendarFilters) => [...calendarKeys.all, 'global', filters] as const,
}

/**
 * Fetch global calendar events across all companies
 */
export function useGlobalCalendar(filters: CalendarFilters = {}) {
  return useQuery({
    queryKey: calendarKeys.global(filters),
    queryFn: () => fetchGlobalCalendar(filters),
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}
