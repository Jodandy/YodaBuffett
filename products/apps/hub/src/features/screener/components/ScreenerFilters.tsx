/**
 * ScreenerFilters Component
 * Filters and sorting controls for the screener
 */

import { MagnifyingGlassIcon, FunnelIcon } from '@heroicons/react/24/outline'
import type { BusinessStage, ScreenerFilters, SortDirection } from '../types'

type SortField = string

interface ScreenerFiltersProps {
  filters: ScreenerFilters
  onFiltersChange: (filters: ScreenerFilters) => void
  sortField: SortField
  sortDirection: SortDirection
  onSortChange: (field: SortField, direction: SortDirection) => void
}

const stages: { value: BusinessStage | 'all'; label: string }[] = [
  { value: 'all', label: 'All Stages' },
  { value: 'compounder', label: 'Compounder' },
  { value: 'growth_stage', label: 'Growth' },
  { value: 'mature_yield', label: 'Mature Yield' },
  { value: 'early_stage', label: 'Early Stage' },
  { value: 'established', label: 'Established' },
]

const tiers = [
  { value: undefined, label: 'All Tiers' },
  { value: 1, label: 'Tier 1' },
  { value: 2, label: 'Tier 1-2' },
  { value: 3, label: 'Tier 1-3' },
]

const sortOptions: { value: SortField; label: string }[] = [
  { value: 'fatPitchScore', label: 'Fat Pitch Score' },
  { value: 'scoreMomentum', label: 'Score Momentum' },
  { value: 'qualityScore', label: 'Quality Score' },
  { value: 'cheapnessScore', label: 'Cheapness' },
  { value: 'qualityTier', label: 'Tier' },
  { value: 'companyName', label: 'Name' },
  { value: 'quality', label: 'Quality Dim' },
  { value: 'profitability', label: 'Profitability' },
  { value: 'growth', label: 'Growth' },
  { value: 'value', label: 'Value' },
  { value: 'momentum', label: 'Momentum' },
  { value: 'beneish_mscore', label: 'Beneish' },
]

const momentumOptions = [
  { value: undefined, label: 'All' },
  { value: 5, label: '+5 or more' },
  { value: 10, label: '+10 or more' },
  { value: 15, label: '+15 or more' },
]

export function ScreenerFilters({
  filters,
  onFiltersChange,
  sortField,
  sortDirection,
  onSortChange,
}: ScreenerFiltersProps) {
  return (
    <div className="bg-card border border-border rounded-lg p-4 space-y-4">
      {/* Search */}
      <div className="relative">
        <MagnifyingGlassIcon className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-muted-foreground" />
        <input
          type="text"
          placeholder="Search companies..."
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

        {/* Stage Filter */}
        <select
          value={filters.stage || 'all'}
          onChange={(e) =>
            onFiltersChange({
              ...filters,
              stage: e.target.value as BusinessStage | 'all',
            })
          }
          className="px-3 py-1.5 bg-background border border-border rounded-lg text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {stages.map((s) => (
            <option key={s.value} value={s.value}>
              {s.label}
            </option>
          ))}
        </select>

        {/* Tier Filter */}
        <select
          value={filters.maxTier || ''}
          onChange={(e) =>
            onFiltersChange({
              ...filters,
              maxTier: e.target.value ? (Number(e.target.value) as 1 | 2 | 3) : undefined,
            })
          }
          className="px-3 py-1.5 bg-background border border-border rounded-lg text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {tiers.map((t) => (
            <option key={t.label} value={t.value || ''}>
              {t.label}
            </option>
          ))}
        </select>

        {/* Min Score Momentum Filter */}
        <select
          value={filters.minMomentum || ''}
          onChange={(e) =>
            onFiltersChange({
              ...filters,
              minMomentum: e.target.value ? Number(e.target.value) : undefined,
            })
          }
          className="px-3 py-1.5 bg-background border border-border rounded-lg text-sm text-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {momentumOptions.map((m) => (
            <option key={m.label} value={m.value || ''}>
              Momentum: {m.label}
            </option>
          ))}
        </select>

        {/* Actionable Only */}
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="checkbox"
            checked={filters.actionableOnly || false}
            onChange={(e) =>
              onFiltersChange({ ...filters, actionableOnly: e.target.checked })
            }
            className="w-4 h-4 rounded border-border text-blue-600 focus:ring-blue-500"
          />
          <span className="text-sm text-foreground">Actionable only</span>
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
    </div>
  )
}
