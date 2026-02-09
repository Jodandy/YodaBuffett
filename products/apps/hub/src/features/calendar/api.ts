/**
 * Calendar API
 * API functions for fetching global calendar events
 */

import { api, toCamelCase } from '@/services/api'
import type { GlobalCalendarResponse, CalendarFilters } from './types'

/**
 * Fetch global calendar events across all companies
 */
export async function fetchGlobalCalendar(
  filters: CalendarFilters = {}
): Promise<GlobalCalendarResponse> {
  const params: Record<string, unknown> = {
    limit: 500,
  }

  if (filters.eventType) {
    params.event_type = filters.eventType
  }
  if (filters.startDate) {
    params.start_date = filters.startDate
  }
  if (filters.endDate) {
    params.end_date = filters.endDate
  }
  if (filters.daysAhead !== undefined) {
    params.days_ahead = filters.daysAhead
  }
  if (filters.daysBack !== undefined) {
    params.days_back = filters.daysBack
  }

  const response = await api.get('/market-data/calendar', { params })
  return toCamelCase<GlobalCalendarResponse>(response.data)
}
