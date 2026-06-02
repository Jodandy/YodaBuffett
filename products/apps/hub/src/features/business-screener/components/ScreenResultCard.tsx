/**
 * ScreenResultCard Component
 * Displays a screen result with metrics in a card layout
 */

import { Link } from 'react-router-dom'
import {
  ExclamationTriangleIcon,
  CheckBadgeIcon,
  ClockIcon,
  BeakerIcon,
  SparklesIcon,
} from '@heroicons/react/24/solid'
import type { ScreenResult, ScreenType } from '../types'
import { SCREEN_NAMES, SCREEN_COLORS, getScoreColor, getScoreBgColor } from '../types'

interface ScreenResultCardProps {
  result: ScreenResult
}

// Format date for display
function formatDate(dateStr: string): string {
  const date = new Date(dateStr)
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
}

// Format metric value
function formatMetric(value: unknown): string {
  if (value === null || value === undefined) return '—'
  if (typeof value === 'number') {
    if (value >= 1000000000) return `${(value / 1000000000).toFixed(1)}B`
    if (value >= 1000000) return `${(value / 1000000).toFixed(1)}M`
    if (value >= 1000) return `${(value / 1000).toFixed(1)}K`
    if (Number.isInteger(value)) return value.toString()
    return value.toFixed(2)
  }
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'
  return String(value)
}

// Get tier badge color
function getTierColor(tier: string): string {
  switch (tier) {
    case 'A':
      return 'bg-green-500/10 text-green-400'
    case 'B':
      return 'bg-blue-500/10 text-blue-400'
    case 'C':
      return 'bg-purple-500/10 text-purple-400'
    default:
      return 'bg-gray-500/10 text-gray-400'
  }
}

// Key metrics to highlight per screen type
const SCREEN_KEY_METRICS: Record<ScreenType, string[]> = {
  1: ['ncav_to_price_ratio', 'ncav', 'market_cap', 'is_profitable'], // Net-Nets
  2: ['current_ratio', 'pe_ratio', 'pb_ratio', 'dividend_yield'], // Defensive
  3: ['pb_ratio', 'book_value', 'tangible_book', 'market_cap'], // Asset Plays
  4: ['revenue_growth', 'gross_margin', 'price_change_52w'], // Turnarounds
  5: ['operating_margin', 'avg_margin', 'margin_deviation'], // Distressed
  6: ['peg_ratio', 'revenue_growth', 'eps_growth', 'pe_ratio'], // GARP
  7: ['normalized_earnings', 'current_earnings', 'compression_ratio'], // Compressed
  8: ['event_type', 'event_date', 'expected_catalyst'], // Special Sit
  9: ['nav_discount', 'portfolio_value', 'market_cap'], // Holdings
  10: ['sum_of_parts', 'market_cap', 'discount'], // SoTP
  11: ['buyback_yield', 'total_buyback', 'shares_reduced_pct'], // Cannibals
  12: ['roic', 'roe', 'operating_margin', 'pe_ratio'], // Wonderful
  13: ['legal_reserve', 'market_cap', 'price_impact'], // Crisis
  14: ['cycle_position', 'normalized_pe', 'peak_margin'], // Cyclicals
  15: ['price_vs_52w_high', 'dividend_yield', 'years_profitable'], // Stalwarts
}

export function ScreenResultCard({ result }: ScreenResultCardProps) {
  const screen = SCREEN_NAMES[result.screenType]
  const colors = SCREEN_COLORS[result.screenType]
  const keyMetrics = SCREEN_KEY_METRICS[result.screenType] || []

  // Extract key metrics from result
  const displayMetrics = keyMetrics
    .map((key) => ({
      key,
      value: result.metrics[key],
      label: key.replace(/_/g, ' ').replace(/\b\w/g, (l) => l.toUpperCase()),
    }))
    .filter((m) => m.value !== null && m.value !== undefined)
    .slice(0, 4)

  return (
    <Link
      to={`/company/${result.primaryTicker}`}
      className="block bg-card border border-border rounded-lg p-4 hover:border-blue-500 transition-colors"
    >
      {/* Header */}
      <div className="flex items-start justify-between mb-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="text-lg font-semibold text-foreground truncate">
              {result.companyName}
            </h3>
            {result.requiresTierB && (
              <BeakerIcon className="w-4 h-4 text-blue-400 flex-shrink-0" title="Needs Tier B analysis" />
            )}
            {result.requiresTierC && (
              <SparklesIcon className="w-4 h-4 text-purple-400 flex-shrink-0" title="Needs Tier C analysis" />
            )}
          </div>
          <div className="flex items-center gap-2 mt-1">
            <span className="text-sm font-mono text-muted-foreground">
              {result.primaryTicker}
            </span>
            <span className={`text-xs px-2 py-0.5 rounded-full ${colors.bg} ${colors.text}`}>
              {screen.shortName}
            </span>
            <span className={`text-xs px-2 py-0.5 rounded-full ${getTierColor(result.tier)}`}>
              Tier {result.tier}
            </span>
          </div>
        </div>

        {/* Score */}
        <div className="text-right flex-shrink-0">
          <div className={`text-2xl font-bold ${getScoreColor(result.score)}`}>
            {Math.round(result.score)}
          </div>
          <div className="flex items-center justify-end gap-1 mt-1">
            {result.isActive ? (
              <CheckBadgeIcon className="w-4 h-4 text-green-400" />
            ) : (
              <ClockIcon className="w-4 h-4 text-muted-foreground" />
            )}
            <span className="text-xs text-muted-foreground">
              {formatDate(result.triggeredAt)}
            </span>
          </div>
        </div>
      </div>

      {/* Score Bar */}
      <div className="mb-4">
        <div className="w-full h-2 bg-muted rounded-full overflow-hidden">
          <div
            className={`h-full ${getScoreBgColor(result.score)} transition-all`}
            style={{ width: `${Math.min(100, result.score)}%` }}
          />
        </div>
      </div>

      {/* Key Metrics */}
      {displayMetrics.length > 0 && (
        <div className="grid grid-cols-2 gap-2 mb-4">
          {displayMetrics.map((metric) => (
            <div key={metric.key} className="text-sm">
              <span className="text-muted-foreground">{metric.label}: </span>
              <span className="font-medium text-foreground">
                {formatMetric(metric.value)}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* Flags */}
      {result.flags.length > 0 && (
        <div className="space-y-1">
          {result.flags.slice(0, 2).map((flag, i) => (
            <div
              key={i}
              className={`flex items-start gap-2 text-xs ${
                flag.includes('WARNING') || flag.includes('CAUTION')
                  ? 'text-orange-400'
                  : 'text-green-400'
              }`}
            >
              {flag.includes('WARNING') || flag.includes('CAUTION') ? (
                <ExclamationTriangleIcon className="w-3 h-3 flex-shrink-0 mt-0.5" />
              ) : (
                <CheckBadgeIcon className="w-3 h-3 flex-shrink-0 mt-0.5" />
              )}
              <span className="truncate">{flag.split(': ').pop()}</span>
            </div>
          ))}
          {result.flags.length > 2 && (
            <span className="text-xs text-muted-foreground">
              +{result.flags.length - 2} more
            </span>
          )}
        </div>
      )}
    </Link>
  )
}

// Expanded card for multi-hit view
export function ScreenResultCardExpanded({ result }: ScreenResultCardProps) {
  const screen = SCREEN_NAMES[result.screenType]
  const colors = SCREEN_COLORS[result.screenType]

  // Get all non-null metrics
  const allMetrics = Object.entries(result.metrics)
    .filter(([, value]) => value !== null && value !== undefined)
    .map(([key, value]) => ({
      key,
      value,
      label: key.replace(/_/g, ' ').replace(/\b\w/g, (l) => l.toUpperCase()),
    }))

  return (
    <Link
      to={`/company/${result.primaryTicker}`}
      className="block bg-card border border-border rounded-lg p-6 hover:border-blue-500 transition-colors"
    >
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="text-xl font-semibold text-foreground truncate">
              {result.companyName}
            </h3>
            {result.requiresTierB && (
              <BeakerIcon className="w-5 h-5 text-blue-400 flex-shrink-0" title="Needs Tier B analysis" />
            )}
            {result.requiresTierC && (
              <SparklesIcon className="w-5 h-5 text-purple-400 flex-shrink-0" title="Needs Tier C analysis" />
            )}
          </div>
          <div className="flex items-center gap-2 mt-1">
            <span className="text-sm font-mono text-muted-foreground">
              {result.primaryTicker}
            </span>
            <span className={`text-xs px-2 py-0.5 rounded-full ${colors.bg} ${colors.text}`}>
              #{result.screenType} {screen.name}
            </span>
            <span className={`text-xs px-2 py-0.5 rounded-full ${getTierColor(result.tier)}`}>
              Tier {result.tier}
            </span>
            {result.isActive ? (
              <span className="text-xs px-2 py-0.5 rounded-full bg-green-500/10 text-green-400">
                Active
              </span>
            ) : (
              <span className="text-xs px-2 py-0.5 rounded-full bg-gray-500/10 text-gray-400">
                Expired
              </span>
            )}
          </div>
        </div>

        {/* Score */}
        <div className="text-right flex-shrink-0">
          <div className={`text-3xl font-bold ${getScoreColor(result.score)}`}>
            {Math.round(result.score)}
          </div>
          <div className="text-xs text-muted-foreground mt-1">
            Found {formatDate(result.triggeredAt)}
          </div>
        </div>
      </div>

      {/* Score Bar */}
      <div className="mb-4">
        <div className="w-full h-3 bg-muted rounded-full overflow-hidden">
          <div
            className={`h-full ${getScoreBgColor(result.score)} transition-all`}
            style={{ width: `${Math.min(100, result.score)}%` }}
          />
        </div>
      </div>

      {/* Screen Description */}
      <p className="text-sm text-muted-foreground mb-4">{screen.description}</p>

      {/* All Metrics Grid */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mb-4">
        {allMetrics.slice(0, 9).map((metric) => (
          <div
            key={metric.key}
            className="bg-muted/50 rounded-lg p-2"
          >
            <div className="text-xs text-muted-foreground truncate">{metric.label}</div>
            <div className="text-sm font-medium text-foreground">
              {formatMetric(metric.value)}
            </div>
          </div>
        ))}
      </div>

      {/* All Flags */}
      {result.flags.length > 0 && (
        <div className="space-y-1">
          {result.flags.map((flag, i) => (
            <div
              key={i}
              className={`flex items-start gap-2 text-xs ${
                flag.includes('WARNING') || flag.includes('CAUTION')
                  ? 'text-orange-400'
                  : 'text-green-400'
              }`}
            >
              {flag.includes('WARNING') || flag.includes('CAUTION') ? (
                <ExclamationTriangleIcon className="w-3 h-3 flex-shrink-0 mt-0.5" />
              ) : (
                <CheckBadgeIcon className="w-3 h-3 flex-shrink-0 mt-0.5" />
              )}
              <span>{flag}</span>
            </div>
          ))}
        </div>
      )}
    </Link>
  )
}
