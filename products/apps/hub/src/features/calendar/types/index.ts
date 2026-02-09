/**
 * Calendar Feature Types
 * Types for the financial calendar page
 */

// Event types
export type EventType = 'earnings' | 'dividend' | 'agm' | 'other'

// Calendar event with company info
export interface GlobalCalendarEvent {
  id: string
  eventType: EventType | string
  eventDate: string
  eventTime?: string
  title?: string
  description?: string
  confirmed: boolean
  webcastUrl?: string
  sourceUrl?: string
  // Dividend-specific fields
  dividendAmount?: number
  dividendCurrency?: string
  exDividendDate?: string
  paymentDate?: string
  // Company info
  symbol: string
  companyName: string
}

// API response
export interface GlobalCalendarResponse {
  events: GlobalCalendarEvent[]
  totalCount: number
  upcomingCount: number
  eventTypeCounts: Record<string, number>
}

// Filter options
export interface CalendarFilters {
  eventType?: EventType | null
  startDate?: string
  endDate?: string
  daysAhead?: number
  daysBack?: number
}

// View mode
export type CalendarViewMode = 'list' | 'calendar'
