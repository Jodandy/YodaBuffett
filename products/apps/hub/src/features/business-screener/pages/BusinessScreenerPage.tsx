/**
 * BusinessScreenerPage
 * Main page for the Business Screener Deluxe - 15 investment screens
 */

import { useState, useMemo } from 'react'
import {
  ChartBarSquareIcon,
  ExclamationTriangleIcon,
  SparklesIcon,
  BeakerIcon,
  Squares2X2Icon,
  CalendarIcon,
} from '@heroicons/react/24/outline'
import { useAllResults, useMultiHits } from '../hooks/useBusinessScreener'
import { BusinessScreenerFilters } from '../components/BusinessScreenerFilters'
import { ScreenResultCard, ScreenResultCardExpanded } from '../components/ScreenResultCard'
import type {
  BusinessScreenerFilters as Filters,
  SortField,
  SortDirection,
  ScreenType,
} from '../types'
import { SCREEN_NAMES } from '../types'

// Stats summary component
function StatCard({
  label,
  value,
  icon: Icon,
  color,
}: {
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

// Screen type summary card
function ScreenSummaryCard({
  screenType,
  count,
  avgScore,
}: {
  screenType: ScreenType
  count: number
  avgScore: number
}) {
  const screen = SCREEN_NAMES[screenType]
  return (
    <div className="bg-card border border-border rounded-lg p-3 flex items-center justify-between">
      <div>
        <span className="text-sm font-medium text-foreground">
          {screenType}. {screen.shortName}
        </span>
        <p className="text-xs text-muted-foreground">{count} results</p>
      </div>
      <div className="text-right">
        <span className="text-lg font-bold text-foreground">{Math.round(avgScore)}</span>
        <p className="text-xs text-muted-foreground">avg</p>
      </div>
    </div>
  )
}

// Format date for display
function formatDateLabel(dateStr: string | null): string {
  if (!dateStr) return 'Today'
  const date = new Date(dateStr)
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

export default function BusinessScreenerPage() {
  // State
  const [filters, setFilters] = useState<Filters>({
    tier: 'all',
    activeOnly: true,
  })
  const [sortField, setSortField] = useState<SortField>('score')
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc')
  const [viewMode, setViewMode] = useState<'grid' | 'expanded' | 'multi-hits'>('grid')
  const [selectedDate, setSelectedDate] = useState<string | null>(null) // null = today (current results)

  // Fetch data with point-in-time date support
  const { data: results = [], isLoading, error } = useAllResults(
    filters.activeOnly !== false,
    selectedDate || undefined
  )
  const { data: multiHits = [] } = useMultiHits()

  // Filter and sort results
  const filteredResults = useMemo(() => {
    let filtered = [...results]

    // Search filter
    if (filters.searchQuery) {
      const query = filters.searchQuery.toLowerCase()
      filtered = filtered.filter(
        (r) =>
          r.companyName?.toLowerCase().includes(query) ||
          r.primaryTicker?.toLowerCase().includes(query)
      )
    }

    // Screen type filter
    if (filters.screenTypes && filters.screenTypes.length > 0) {
      filtered = filtered.filter((r) => filters.screenTypes!.includes(r.screenType))
    }

    // Tier filter
    if (filters.tier && filters.tier !== 'all') {
      filtered = filtered.filter((r) => r.tier === filters.tier)
    }

    // Min score filter
    if (filters.minScore) {
      filtered = filtered.filter((r) => r.score >= filters.minScore!)
    }

    // Sort
    filtered.sort((a, b) => {
      let aVal: number | string
      let bVal: number | string

      switch (sortField) {
        case 'companyName':
          aVal = a.companyName || ''
          bVal = b.companyName || ''
          break
        case 'screenType':
          aVal = a.screenType
          bVal = b.screenType
          break
        case 'triggeredAt':
          aVal = new Date(a.triggeredAt).getTime()
          bVal = new Date(b.triggeredAt).getTime()
          break
        case 'score':
        default:
          aVal = a.score
          bVal = b.score
          break
      }

      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDirection === 'asc'
          ? aVal.localeCompare(bVal)
          : bVal.localeCompare(aVal)
      }

      return sortDirection === 'asc'
        ? (aVal as number) - (bVal as number)
        : (bVal as number) - (aVal as number)
    })

    return filtered
  }, [results, filters, sortField, sortDirection])

  // Stats
  const stats = useMemo(() => {
    const screenCounts = new Map<ScreenType, { count: number; totalScore: number }>()
    let tierBCount = 0
    let tierCCount = 0

    for (const result of results) {
      const existing = screenCounts.get(result.screenType) || { count: 0, totalScore: 0 }
      screenCounts.set(result.screenType, {
        count: existing.count + 1,
        totalScore: existing.totalScore + result.score,
      })
      if (result.requiresTierB) tierBCount++
      if (result.requiresTierC) tierCCount++
    }

    const avgScore =
      results.length > 0
        ? Math.round(results.reduce((sum, r) => sum + r.score, 0) / results.length)
        : 0

    const byScreen = Array.from(screenCounts.entries())
      .map(([screenType, data]) => ({
        screenType,
        count: data.count,
        avgScore: data.count > 0 ? data.totalScore / data.count : 0,
      }))
      .sort((a, b) => b.count - a.count)

    return {
      total: results.length,
      multiHits: multiHits.length,
      avgScore,
      tierBNeeded: tierBCount,
      tierCNeeded: tierCCount,
      byScreen,
    }
  }, [results, multiHits])

  // Handle sort change
  const handleSortChange = (field: SortField, direction: SortDirection) => {
    setSortField(field)
    setSortDirection(direction)
  }

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
        <p className="text-red-400 font-medium">Failed to load screener data</p>
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
          <h1 className="text-3xl font-bold text-foreground">Business Screener</h1>
          <p className="text-muted-foreground mt-2">
            15 systematic investment screens across value, growth, quality, and special situations
            {selectedDate && (
              <span className="ml-2 text-yellow-500">
                (Viewing results as of {formatDateLabel(selectedDate)})
              </span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-4">
          {/* Date Picker - Point in Time */}
          <div className="flex items-center gap-2">
            <CalendarIcon className="w-5 h-5 text-muted-foreground" />
            <div className="relative">
              <input
                type="date"
                value={selectedDate || ''}
                onChange={(e) => setSelectedDate(e.target.value || null)}
                max={new Date().toISOString().split('T')[0]}
                min="2021-06-01"
                className="bg-card border border-border rounded-lg px-3 py-1.5 text-sm text-foreground focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
              {selectedDate && (
                <button
                  onClick={() => setSelectedDate(null)}
                  className="absolute right-8 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                  title="Reset to today"
                >
                  ×
                </button>
              )}
            </div>
            {selectedDate && (
              <span className="text-xs text-yellow-500 font-medium">
                Historical
              </span>
            )}
          </div>

          {/* View Mode Toggle */}
          <button
            onClick={() => setViewMode('grid')}
            className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
              viewMode === 'grid'
                ? 'bg-blue-600 text-white'
                : 'bg-muted text-muted-foreground hover:text-foreground'
            }`}
          >
            Grid
          </button>
          <button
            onClick={() => setViewMode('expanded')}
            className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
              viewMode === 'expanded'
                ? 'bg-blue-600 text-white'
                : 'bg-muted text-muted-foreground hover:text-foreground'
            }`}
          >
            Expanded
          </button>
          <button
            onClick={() => setViewMode('multi-hits')}
            className={`px-3 py-1.5 rounded-lg text-sm transition-colors flex items-center gap-1 ${
              viewMode === 'multi-hits'
                ? 'bg-blue-600 text-white'
                : 'bg-muted text-muted-foreground hover:text-foreground'
            }`}
          >
            <Squares2X2Icon className="w-4 h-4" />
            Multi-Hits
          </button>
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <StatCard
          label="Total Results"
          value={stats.total}
          icon={ChartBarSquareIcon}
          color="bg-blue-500/10 text-blue-500"
        />
        <StatCard
          label="Multi-Hits"
          value={stats.multiHits}
          icon={SparklesIcon}
          color="bg-yellow-500/10 text-yellow-500"
        />
        <StatCard
          label="Avg Score"
          value={stats.avgScore}
          icon={ChartBarSquareIcon}
          color="bg-green-500/10 text-green-500"
        />
        <StatCard
          label="Need Tier B"
          value={stats.tierBNeeded}
          icon={BeakerIcon}
          color="bg-purple-500/10 text-purple-500"
        />
        <StatCard
          label="Need Tier C"
          value={stats.tierCNeeded}
          icon={SparklesIcon}
          color="bg-pink-500/10 text-pink-500"
        />
      </div>

      {/* Screen Type Summary */}
      {stats.byScreen.length > 0 && (
        <div>
          <h2 className="text-sm font-medium text-muted-foreground mb-2">Results by Screen</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-2">
            {stats.byScreen.map((s) => (
              <ScreenSummaryCard
                key={s.screenType}
                screenType={s.screenType}
                count={s.count}
                avgScore={s.avgScore}
              />
            ))}
          </div>
        </div>
      )}

      {/* Filters */}
      <BusinessScreenerFilters
        filters={filters}
        onFiltersChange={setFilters}
        sortField={sortField}
        sortDirection={sortDirection}
        onSortChange={handleSortChange}
      />

      {/* Results count */}
      <div className="text-sm text-muted-foreground">
        Showing {filteredResults.length} of {results.length} results
      </div>

      {/* Results Grid */}
      {filteredResults.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <ChartBarSquareIcon className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-foreground mb-2">No results found</h2>
          <p className="text-muted-foreground">
            {results.length === 0
              ? 'Run the screener CLI to find investment opportunities'
              : 'Try adjusting your filters to see more results'}
          </p>
          <p className="text-xs text-muted-foreground mt-4">
            Run: python domains/business_screener/cli.py run --all-tier-a
          </p>
        </div>
      ) : viewMode === 'multi-hits' ? (
        // Multi-hits view
        <div className="space-y-4">
          {multiHits.length === 0 ? (
            <div className="bg-card border border-border rounded-lg p-8 text-center">
              <Squares2X2Icon className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
              <h2 className="text-lg font-semibold text-foreground mb-2">
                No multi-hit companies
              </h2>
              <p className="text-muted-foreground">
                Companies passing multiple screens will appear here
              </p>
            </div>
          ) : (
            multiHits.map((hit) => (
              <div
                key={hit.companyId}
                className="bg-card border border-border rounded-lg p-4"
              >
                <div className="flex items-start justify-between mb-3">
                  <div>
                    <h3 className="text-lg font-semibold text-foreground">
                      {hit.companyName}
                    </h3>
                    <span className="text-sm font-mono text-muted-foreground">
                      {hit.primaryTicker}
                    </span>
                  </div>
                  <div className="text-right">
                    <div className="text-2xl font-bold text-yellow-400">
                      {hit.screenCount} screens
                    </div>
                    <div className="text-sm text-muted-foreground">
                      Avg: {Math.round(hit.avgScore)}
                    </div>
                  </div>
                </div>
                <div className="flex flex-wrap gap-2">
                  {hit.screens.map((s) => (
                    <span
                      key={s.screenType}
                      className="text-xs px-2 py-1 rounded-full bg-blue-500/10 text-blue-400"
                    >
                      #{s.screenType} {s.screenName}: {Math.round(s.score)}
                    </span>
                  ))}
                </div>
              </div>
            ))
          )}
        </div>
      ) : viewMode === 'grid' ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {filteredResults.map((result) => (
            <ScreenResultCard key={`${result.companyId}-${result.screenType}`} result={result} />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {filteredResults.map((result) => (
            <ScreenResultCardExpanded
              key={`${result.companyId}-${result.screenType}`}
              result={result}
            />
          ))}
        </div>
      )}
    </div>
  )
}
