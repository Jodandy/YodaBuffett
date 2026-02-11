/**
 * ScreenerPage
 * Main page for the Fat Pitch stock screener
 */

import { useState, useMemo } from 'react'
import { ChartBarSquareIcon, SparklesIcon, ExclamationTriangleIcon, BeakerIcon, CalendarIcon } from '@heroicons/react/24/outline'
import { usePitches, useWeightProfiles } from '../hooks/usePitches'
import { CompanyCard, CompanyCardExpanded } from '../components/CompanyCard'
import { ScreenerFilters } from '../components/ScreenerFilters'
import type { ScreenerFilters as Filters, SortDirection } from '../types'

// Format date for display
function formatDateLabel(dateStr: string | null): string {
  if (!dateStr) return 'Today'
  const date = new Date(dateStr)
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })
}

// Sort field type - includes main fields and dimensions
type SortField = 'fatPitchScore' | 'qualityScore' | 'cheapnessScore' | 'companyName' | 'qualityTier' | 'scoreMomentum' | string

// Stats summary component
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

export default function ScreenerPage() {
  // State
  const [filters, setFilters] = useState<Filters>({
    stage: 'all',
    actionableOnly: false,
  })
  const [sortField, setSortField] = useState<SortField>('fatPitchScore')
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc')
  const [viewMode, setViewMode] = useState<'grid' | 'expanded'>('grid')
  const [selectedProfile, setSelectedProfile] = useState<string>('garp')
  const [selectedDate, setSelectedDate] = useState<string | null>(null) // null = today

  // Fetch weight profiles
  const { data: profileData } = useWeightProfiles()

  // Fetch data with selected profile and date
  const { data: pitches = [], isLoading, error } = usePitches(
    selectedProfile,
    selectedDate || undefined
  )

  // Filter and sort pitches
  const filteredPitches = useMemo(() => {
    let result = [...pitches]

    // Search filter
    if (filters.searchQuery) {
      const query = filters.searchQuery.toLowerCase()
      result = result.filter(
        (p) =>
          p.companyName?.toLowerCase().includes(query) ||
          p.symbol?.toLowerCase().includes(query)
      )
    }

    // Stage filter
    if (filters.stage && filters.stage !== 'all') {
      result = result.filter((p) => p.stage === filters.stage)
    }

    // Tier filter
    if (filters.maxTier) {
      result = result.filter((p) => p.qualityTier <= filters.maxTier!)
    }

    // Actionable filter
    if (filters.actionableOnly) {
      result = result.filter((p) => p.isActionable)
    }

    // Momentum filter
    if (filters.minMomentum) {
      result = result.filter((p) => (p.scoreMomentum ?? -999) >= filters.minMomentum!)
    }

    // Sort
    result.sort((a, b) => {
      let aVal: number | string
      let bVal: number | string

      if (sortField === 'companyName') {
        aVal = a.companyName || ''
        bVal = b.companyName || ''
      } else if (sortField === 'fatPitchScore') {
        aVal = a.fatPitchScore ?? 0
        bVal = b.fatPitchScore ?? 0
      } else if (sortField === 'qualityScore') {
        aVal = a.qualityScore ?? 0
        bVal = b.qualityScore ?? 0
      } else if (sortField === 'cheapnessScore') {
        aVal = a.cheapnessScore ?? 0
        bVal = b.cheapnessScore ?? 0
      } else if (sortField === 'qualityTier') {
        aVal = a.qualityTier ?? 5
        bVal = b.qualityTier ?? 5
      } else if (sortField === 'scoreMomentum') {
        aVal = a.scoreMomentum ?? -999
        bVal = b.scoreMomentum ?? -999
      } else {
        // Dimension field
        aVal = a.dimensionScores?.[sortField] ?? 0
        bVal = b.dimensionScores?.[sortField] ?? 0
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

    return result
  }, [pitches, filters, sortField, sortDirection])

  // Stats
  const stats = useMemo(() => {
    const actionableCount = pitches.filter((p) => p.isActionable).length
    const tier1Count = pitches.filter((p) => p.qualityTier === 1).length
    const avgScore = pitches.length > 0
      ? Math.round(pitches.reduce((sum, p) => sum + (p.fatPitchScore || 0), 0) / pitches.length)
      : 0

    return { total: pitches.length, actionable: actionableCount, tier1: tier1Count, avgScore }
  }, [pitches])

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
          <h1 className="text-3xl font-bold text-foreground">Stock Screener</h1>
          <p className="text-muted-foreground mt-2">
            Fat Pitch Machine - find quality companies at attractive prices
            {selectedDate && (
              <span className="ml-2 text-yellow-500">
                (Viewing scores as of {formatDateLabel(selectedDate)})
              </span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-4">
          {/* Date Picker */}
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

          {/* Weight Profile Selector */}
          <div className="flex items-center gap-2">
            <BeakerIcon className="w-5 h-5 text-muted-foreground" />
            <select
              value={selectedProfile}
              onChange={(e) => setSelectedProfile(e.target.value)}
              className="bg-card border border-border rounded-lg px-3 py-1.5 text-sm text-foreground focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              {profileData?.profiles.map((profile) => (
                <option key={profile.name} value={profile.name}>
                  {profile.name.charAt(0).toUpperCase() + profile.name.slice(1)}
                  {profile.isDefault && ' (Default)'}
                </option>
              )) || (
                <>
                  <option value="garp">GARP (Default)</option>
                  <option value="optimal">Optimal</option>
                  <option value="buffett">Buffett</option>
                  <option value="quality">Quality</option>
                  <option value="value">Value</option>
                  <option value="equal">Equal</option>
                </>
              )}
            </select>
          </div>

          {/* View Mode Toggle */}
          <div className="flex items-center gap-2">
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
          </div>
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          label="Total Companies"
          value={stats.total}
          icon={ChartBarSquareIcon}
          color="bg-blue-500/10 text-blue-500"
        />
        <StatCard
          label="Actionable"
          value={stats.actionable}
          icon={SparklesIcon}
          color="bg-yellow-500/10 text-yellow-500"
        />
        <StatCard
          label="Tier 1"
          value={stats.tier1}
          icon={ChartBarSquareIcon}
          color="bg-green-500/10 text-green-500"
        />
        <StatCard
          label="Avg Score"
          value={stats.avgScore}
          icon={ChartBarSquareIcon}
          color="bg-purple-500/10 text-purple-500"
        />
      </div>

      {/* Filters */}
      <ScreenerFilters
        filters={filters}
        onFiltersChange={setFilters}
        sortField={sortField}
        sortDirection={sortDirection}
        onSortChange={handleSortChange}
      />

      {/* Results count */}
      <div className="text-sm text-muted-foreground">
        Showing {filteredPitches.length} of {pitches.length} companies
      </div>

      {/* Results Grid */}
      {filteredPitches.length === 0 ? (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <ChartBarSquareIcon className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-foreground mb-2">
            No companies match your filters
          </h2>
          <p className="text-muted-foreground">
            Try adjusting your search or filter criteria
          </p>
        </div>
      ) : viewMode === 'grid' ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {filteredPitches.map((pitch) => (
            <CompanyCard key={pitch.companyId || pitch.symbol} pitch={pitch} />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {filteredPitches.map((pitch) => (
            <CompanyCardExpanded key={pitch.companyId || pitch.symbol} pitch={pitch} />
          ))}
        </div>
      )}
    </div>
  )
}
