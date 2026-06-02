/**
 * BusinessScreenerFilters Component
 * Filters and sorting controls for the business screener
 */

import { MagnifyingGlassIcon, FunnelIcon } from '@heroicons/react/24/outline'
import type {
  BusinessScreenerFilters,
  ScreenType,
  Tier,
  SortField,
  SortDirection,
} from '../types'
import { SCREEN_NAMES } from '../types'

interface BusinessScreenerFiltersProps {
  filters: BusinessScreenerFilters
  onFiltersChange: (filters: BusinessScreenerFilters) => void
  sortField: SortField
  sortDirection: SortDirection
  onSortChange: (field: SortField, direction: SortDirection) => void
}

const tierOptions: { value: Tier | 'all'; label: string }[] = [
  { value: 'all', label: 'All Tiers' },
  { value: 'A', label: 'Tier A (SQL)' },
  { value: 'B', label: 'Tier B (Local LLM)' },
  { value: 'C', label: 'Tier C (Claude)' },
]

const scoreOptions = [
  { value: undefined, label: 'Any Score' },
  { value: 70, label: '70+ (Strong)' },
  { value: 50, label: '50+ (Moderate)' },
  { value: 30, label: '30+ (Weak)' },
]

const sortOptions: { value: SortField; label: string }[] = [
  { value: 'score', label: 'Score' },
  { value: 'companyName', label: 'Company Name' },
  { value: 'screenType', label: 'Screen Type' },
  { value: 'triggeredAt', label: 'Date Found' },
]

// Group screens for multi-select
const screenGroups = [
  { label: 'Value', screens: [1, 2, 3, 5, 13, 14, 16] as ScreenType[] },
  { label: 'Growth', screens: [4, 6, 12] as ScreenType[] },
  { label: 'Special', screens: [7, 8, 9, 10] as ScreenType[] },
  { label: 'Quality', screens: [11, 12, 15] as ScreenType[] },
]

export function BusinessScreenerFilters({
  filters,
  onFiltersChange,
  sortField,
  sortDirection,
  onSortChange,
}: BusinessScreenerFiltersProps) {
  const toggleScreen = (screenType: ScreenType) => {
    const current = filters.screenTypes || []
    const updated = current.includes(screenType)
      ? current.filter((s) => s !== screenType)
      : [...current, screenType]
    onFiltersChange({ ...filters, screenTypes: updated.length ? updated : undefined })
  }

  const selectScreenGroup = (screens: ScreenType[]) => {
    onFiltersChange({ ...filters, screenTypes: screens })
  }

  const clearScreenFilter = () => {
    onFiltersChange({ ...filters, screenTypes: undefined })
  }

  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-4">
      {/* Search */}
      <div className="relative">
        <MagnifyingGlassIcon className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-muted-foreground" />
        <input
          type="text"
          placeholder="Search companies or tickers..."
          value={filters.searchQuery || ''}
          onChange={(e) => onFiltersChange({ ...filters, searchQuery: e.target.value })}
          className="w-full pl-10 pr-4 py-2 bg-background border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>

      {/* Filter Row */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-2">
          <FunnelIcon className="w-4 h-4 text-muted-foreground" />
          <span className="text-sm text-muted-foreground">Filters:</span>
        </div>

        {/* Tier Filter */}
        <select
          value={filters.tier || 'all'}
          onChange={(e) =>
            onFiltersChange({
              ...filters,
              tier: e.target.value as Tier | 'all',
            })
          }
          className="px-3 py-1.5 bg-background border border-border rounded-lg text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {tierOptions.map((t) => (
            <option key={t.value} value={t.value}>
              {t.label}
            </option>
          ))}
        </select>

        {/* Min Score Filter */}
        <select
          value={filters.minScore || ''}
          onChange={(e) =>
            onFiltersChange({
              ...filters,
              minScore: e.target.value ? Number(e.target.value) : undefined,
            })
          }
          className="px-3 py-1.5 bg-background border border-border rounded-lg text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {scoreOptions.map((s) => (
            <option key={s.label} value={s.value || ''}>
              {s.label}
            </option>
          ))}
        </select>

        {/* Active Only */}
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="checkbox"
            checked={filters.activeOnly !== false}
            onChange={(e) =>
              onFiltersChange({ ...filters, activeOnly: e.target.checked })
            }
            className="w-4 h-4 rounded border-border text-blue-600 focus:ring-blue-500"
          />
          <span className="text-sm text-foreground">Active only</span>
        </label>

        {/* Spacer */}
        <div className="flex-1" />

        {/* Sort */}
        <div className="flex items-center gap-2">
          <span className="text-sm text-muted-foreground">Sort by:</span>
          <select
            value={sortField}
            onChange={(e) => onSortChange(e.target.value as SortField, sortDirection)}
            className="px-3 py-1.5 bg-background border border-border rounded-lg text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {sortOptions.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
          <button
            onClick={() =>
              onSortChange(sortField, sortDirection === 'desc' ? 'asc' : 'desc')
            }
            className="px-2 py-1.5 bg-background border border-border rounded-lg text-sm text-foreground hover:bg-muted transition-colors"
          >
            {sortDirection === 'desc' ? '↓' : '↑'}
          </button>
        </div>
      </div>

      {/* Screen Type Filter */}
      <div className="space-y-2">
        <div className="flex items-center gap-2">
          <span className="text-sm text-muted-foreground">Screen types:</span>
          {/* Quick select groups */}
          {screenGroups.map((group) => (
            <button
              key={group.label}
              onClick={() => selectScreenGroup(group.screens)}
              className="px-2 py-1 text-xs bg-muted hover:bg-muted/80 rounded-md text-muted-foreground hover:text-foreground transition-colors"
            >
              {group.label}
            </button>
          ))}
          <button
            onClick={clearScreenFilter}
            className="px-2 py-1 text-xs bg-muted hover:bg-muted/80 rounded-md text-muted-foreground hover:text-foreground transition-colors"
          >
            All
          </button>
        </div>

        {/* Screen type toggle buttons */}
        <div className="flex flex-wrap gap-2">
          {(Object.keys(SCREEN_NAMES) as unknown as ScreenType[]).map((screenType) => {
            const isSelected = filters.screenTypes?.includes(Number(screenType) as ScreenType)
            const screen = SCREEN_NAMES[Number(screenType) as ScreenType]
            return (
              <button
                key={screenType}
                onClick={() => toggleScreen(Number(screenType) as ScreenType)}
                title={screen.description}
                className={`px-2 py-1 text-xs rounded-md transition-colors ${
                  isSelected
                    ? 'bg-blue-600 text-white'
                    : 'bg-muted text-muted-foreground hover:text-foreground hover:bg-muted/80'
                }`}
              >
                {screenType}. {screen.shortName}
              </button>
            )
          })}
        </div>
      </div>
    </div>
  )
}
