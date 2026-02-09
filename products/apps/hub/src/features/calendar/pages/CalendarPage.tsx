/**
 * CalendarPage
 * Global financial calendar showing events across all companies
 */

import { useState, useMemo } from 'react'
import { Link } from 'react-router-dom'
import {
  CalendarIcon,
  CurrencyDollarIcon,
  ChartBarIcon,
  UsersIcon,
  ExclamationTriangleIcon,
  FunnelIcon,
  ArrowPathIcon,
} from '@heroicons/react/24/outline'
import { useGlobalCalendar } from '../hooks/useCalendar'
import type { CalendarFilters, GlobalCalendarEvent, EventType } from '../types'

// Event type configuration
const EVENT_TYPES: { type: EventType | null; label: string; color: string; bgColor: string; icon: React.ElementType }[] = [
  { type: null, label: 'All Events', color: 'text-foreground', bgColor: 'bg-muted', icon: CalendarIcon },
  { type: 'earnings', label: 'Earnings', color: 'text-blue-500', bgColor: 'bg-blue-500/10', icon: ChartBarIcon },
  { type: 'dividend', label: 'Dividends', color: 'text-green-500', bgColor: 'bg-green-500/10', icon: CurrencyDollarIcon },
  { type: 'agm', label: 'AGM', color: 'text-purple-500', bgColor: 'bg-purple-500/10', icon: UsersIcon },
]

// Stats card component
function StatCard({ label, value, icon: Icon, color }: {
  label: string
  value: string | number
  icon: React.ElementType
  color: string
}) {
  return (
    <div className="bg-card border border-border rounded-lg p-4">
      <div className="flex items-center space-x-3">
        <div className={`p-2 ${color} rounded-lg`}>
          <Icon className="w-5 h-5" />
        </div>
        <div>
          <p className="text-sm text-muted-foreground">{label}</p>
          <p className="text-xl font-bold text-foreground">{value}</p>
        </div>
      </div>
    </div>
  )
}

// Event card component
function EventCard({ event }: { event: GlobalCalendarEvent }) {
  const eventConfig = EVENT_TYPES.find(t => t.type === event.eventType) || EVENT_TYPES[0]
  const eventDate = new Date(event.eventDate)
  const isToday = new Date().toDateString() === eventDate.toDateString()
  const isPast = eventDate < new Date() && !isToday
  const isUpcoming = eventDate > new Date() && (eventDate.getTime() - new Date().getTime()) < 7 * 24 * 60 * 60 * 1000

  return (
    <div className={`bg-card border border-border rounded-lg p-4 hover:border-blue-500/50 transition-colors ${isPast ? 'opacity-60' : ''}`}>
      <div className="flex items-start justify-between gap-4">
        {/* Left: Date block */}
        <div className="flex-shrink-0 text-center min-w-[60px]">
          <div className={`text-xs font-medium uppercase ${isToday ? 'text-blue-500' : 'text-muted-foreground'}`}>
            {eventDate.toLocaleDateString('en-US', { weekday: 'short' })}
          </div>
          <div className={`text-2xl font-bold ${isToday ? 'text-blue-500' : 'text-foreground'}`}>
            {eventDate.getDate()}
          </div>
          <div className="text-xs text-muted-foreground">
            {eventDate.toLocaleDateString('en-US', { month: 'short' })}
          </div>
        </div>

        {/* Middle: Event details */}
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${eventConfig.bgColor} ${eventConfig.color}`}>
              <eventConfig.icon className="w-3 h-3 mr-1" />
              {event.eventType.charAt(0).toUpperCase() + event.eventType.slice(1)}
            </span>
            {isToday && (
              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-blue-500 text-white">
                Today
              </span>
            )}
            {isUpcoming && !isToday && (
              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-yellow-500/10 text-yellow-500">
                This week
              </span>
            )}
            {event.confirmed && (
              <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-green-500/10 text-green-500">
                Confirmed
              </span>
            )}
          </div>

          <Link
            to={`/company/${event.symbol}`}
            className="font-semibold text-foreground hover:text-blue-500 transition-colors block truncate"
          >
            {event.companyName}
          </Link>
          <div className="text-sm text-muted-foreground">{event.symbol}</div>

          {event.title && (
            <p className="text-sm text-muted-foreground mt-1 truncate">{event.title}</p>
          )}

          {/* Dividend details */}
          {event.eventType === 'dividend' && event.dividendAmount && (
            <div className="mt-2 text-sm">
              <span className="text-green-500 font-medium">
                {event.dividendAmount.toFixed(2)} {event.dividendCurrency || 'SEK'}
              </span>
              {event.exDividendDate && (
                <span className="text-muted-foreground ml-2">
                  Ex-date: {new Date(event.exDividendDate).toLocaleDateString()}
                </span>
              )}
            </div>
          )}

          {event.eventTime && (
            <div className="text-xs text-muted-foreground mt-1">
              {event.eventTime}
            </div>
          )}
        </div>

        {/* Right: Links */}
        <div className="flex-shrink-0 flex flex-col gap-1">
          {event.webcastUrl && (
            <a
              href={event.webcastUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-blue-500 hover:text-blue-400"
            >
              Webcast
            </a>
          )}
          {event.sourceUrl && (
            <a
              href={event.sourceUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="text-xs text-muted-foreground hover:text-foreground"
            >
              Source
            </a>
          )}
        </div>
      </div>
    </div>
  )
}

// Group events by date
function groupEventsByDate(events: GlobalCalendarEvent[]): Map<string, GlobalCalendarEvent[]> {
  const groups = new Map<string, GlobalCalendarEvent[]>()

  for (const event of events) {
    const dateKey = event.eventDate
    if (!groups.has(dateKey)) {
      groups.set(dateKey, [])
    }
    groups.get(dateKey)!.push(event)
  }

  return groups
}

export default function CalendarPage() {
  // Filter state
  const [selectedEventType, setSelectedEventType] = useState<EventType | null>(null)
  const [daysAhead, setDaysAhead] = useState(90)
  const [daysBack, setDaysBack] = useState(7)

  // Build filters
  const filters: CalendarFilters = useMemo(() => ({
    eventType: selectedEventType,
    daysAhead,
    daysBack,
  }), [selectedEventType, daysAhead, daysBack])

  // Fetch data
  const { data, isLoading, error, refetch, isFetching } = useGlobalCalendar(filters)

  // Group events by date
  const groupedEvents = useMemo(() => {
    if (!data?.events) return new Map()
    return groupEventsByDate(data.events)
  }, [data?.events])

  // Loading state
  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
      </div>
    )
  }

  // Error state
  if (error) {
    return (
      <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-6 text-center">
        <ExclamationTriangleIcon className="w-12 h-12 text-red-400 mx-auto mb-4" />
        <p className="text-red-400 font-medium">Failed to load calendar data</p>
        <p className="text-sm text-muted-foreground mt-2">
          {error instanceof Error ? error.message : 'Unknown error'}
        </p>
        <p className="text-xs text-muted-foreground mt-4">
          Make sure the backend is running at localhost:8000
        </p>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-3xl font-bold text-foreground">Financial Calendar</h1>
          <p className="text-muted-foreground mt-2">
            Upcoming earnings, dividends, AGMs, and other financial events
          </p>
        </div>
        <button
          onClick={() => refetch()}
          disabled={isFetching}
          className="inline-flex items-center px-3 py-2 bg-muted text-muted-foreground rounded-lg hover:text-foreground hover:bg-muted/80 transition-colors disabled:opacity-50"
        >
          <ArrowPathIcon className={`w-4 h-4 mr-2 ${isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          label="Total Events"
          value={data?.totalCount || 0}
          icon={CalendarIcon}
          color="bg-blue-500/10 text-blue-500"
        />
        <StatCard
          label="Upcoming"
          value={data?.upcomingCount || 0}
          icon={CalendarIcon}
          color="bg-green-500/10 text-green-500"
        />
        <StatCard
          label="Earnings"
          value={data?.eventTypeCounts?.earnings || 0}
          icon={ChartBarIcon}
          color="bg-blue-500/10 text-blue-500"
        />
        <StatCard
          label="Dividends"
          value={data?.eventTypeCounts?.dividend || 0}
          icon={CurrencyDollarIcon}
          color="bg-green-500/10 text-green-500"
        />
      </div>

      {/* Filters */}
      <div className="bg-card border border-border rounded-lg p-4">
        <div className="flex items-center gap-2 mb-4">
          <FunnelIcon className="w-5 h-5 text-muted-foreground" />
          <span className="text-sm font-medium text-foreground">Filters</span>
        </div>

        <div className="flex flex-wrap gap-4">
          {/* Event type filter */}
          <div className="flex flex-wrap gap-2">
            {EVENT_TYPES.map(({ type, label, color, bgColor }) => (
              <button
                key={type ?? 'all'}
                onClick={() => setSelectedEventType(type)}
                className={`inline-flex items-center px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                  selectedEventType === type
                    ? 'bg-blue-600 text-white'
                    : `${bgColor} ${color} hover:opacity-80`
                }`}
              >
                {label}
                {type && data?.eventTypeCounts?.[type] !== undefined && (
                  <span className="ml-1.5 text-xs opacity-75">
                    ({data.eventTypeCounts[type]})
                  </span>
                )}
              </button>
            ))}
          </div>

          {/* Time range */}
          <div className="flex items-center gap-2 ml-auto">
            <label className="text-sm text-muted-foreground">Show:</label>
            <select
              value={daysBack}
              onChange={(e) => setDaysBack(Number(e.target.value))}
              className="bg-muted border border-border rounded px-2 py-1 text-sm"
            >
              <option value={0}>No past events</option>
              <option value={7}>Past 7 days</option>
              <option value={30}>Past 30 days</option>
            </select>
            <span className="text-muted-foreground">to</span>
            <select
              value={daysAhead}
              onChange={(e) => setDaysAhead(Number(e.target.value))}
              className="bg-muted border border-border rounded px-2 py-1 text-sm"
            >
              <option value={30}>Next 30 days</option>
              <option value={90}>Next 90 days</option>
              <option value={180}>Next 6 months</option>
              <option value={365}>Next year</option>
            </select>
          </div>
        </div>
      </div>

      {/* Results count */}
      <div className="text-sm text-muted-foreground">
        Showing {data?.events?.length || 0} events
      </div>

      {/* Events list grouped by date */}
      {!data?.events?.length ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <CalendarIcon className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-foreground mb-2">
            No events found
          </h2>
          <p className="text-muted-foreground">
            Try adjusting your filters to see more events
          </p>
        </div>
      ) : (
        <div className="space-y-6">
          {Array.from(groupedEvents.entries()).map(([dateKey, events]: [string, GlobalCalendarEvent[]]) => {
            const eventDate = new Date(dateKey)
            const isToday = new Date().toDateString() === eventDate.toDateString()

            return (
              <div key={dateKey}>
                {/* Date header */}
                <div className={`sticky top-0 z-10 bg-background/95 backdrop-blur py-2 mb-3 border-b border-border ${isToday ? 'border-blue-500' : ''}`}>
                  <h3 className={`text-sm font-semibold ${isToday ? 'text-blue-500' : 'text-muted-foreground'}`}>
                    {isToday && <span className="mr-2">Today -</span>}
                    {eventDate.toLocaleDateString('en-US', {
                      weekday: 'long',
                      month: 'long',
                      day: 'numeric',
                      year: 'numeric',
                    })}
                    <span className="ml-2 text-xs font-normal">
                      ({events.length} event{events.length !== 1 ? 's' : ''})
                    </span>
                  </h3>
                </div>

                {/* Events for this date */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                  {events.map((event) => (
                    <EventCard key={event.id} event={event} />
                  ))}
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
