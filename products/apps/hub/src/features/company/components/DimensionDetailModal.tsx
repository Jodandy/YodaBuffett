/**
 * DimensionDetailModal Component
 * Modal that shows detailed breakdown of a dimension score
 */

import { cn } from '@yodabuffett/ui'
import type { DimensionDetail } from '../types'

interface DimensionDetailModalProps {
  isOpen: boolean
  onClose: () => void
  dimensionCode: string
  detail: DimensionDetail | null
  isLoading: boolean
}

// Dimension display names
const dimensionNames: Record<string, string> = {
  profitability: 'Profitability',
  returns: 'Returns',
  growth: 'Growth',
  financial_health: 'Financial Health',
  earnings_quality: 'Earnings Quality',
  capital_allocation: 'Capital Allocation',
  working_capital: 'Working Capital',
  beneish_mscore: 'Beneish M-Score',
  value: 'Value',
  risk: 'Risk',
  momentum: 'Momentum',
  quality: 'Quality',
  valuation_percentile: 'Valuation Percentile',
  sentiment: 'Sentiment',
}

// Get score color class
function getScoreColor(score: number | null | undefined): string {
  if (score === null || score === undefined) return 'text-muted-foreground'
  if (score >= 80) return 'text-green-500'
  if (score >= 60) return 'text-green-400'
  if (score >= 40) return 'text-yellow-400'
  if (score >= 20) return 'text-orange-400'
  return 'text-red-400'
}

// Format metric name for display
function formatMetricName(key: string): string {
  return key
    .replace(/_/g, ' ')
    .replace(/([A-Z])/g, ' $1')
    .replace(/^./, str => str.toUpperCase())
    .trim()
}

// Format number for display
function formatNumber(value: number | null | undefined, decimals: number = 2): string {
  if (value === null || value === undefined) return 'N/A'
  if (Math.abs(value) >= 1e9) return `${(value / 1e9).toFixed(1)}B`
  if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(1)}M`
  if (Math.abs(value) >= 1e3) return `${(value / 1e3).toFixed(1)}K`
  return value.toFixed(decimals)
}

// Get trend icon and color
function getTrendDisplay(trend: string | null): { icon: string; color: string } {
  if (!trend) return { icon: '—', color: 'text-muted-foreground' }
  const t = trend.toLowerCase()
  if (t.includes('strong') && t.includes('decline')) return { icon: '↓↓', color: 'text-red-400' }
  if (t.includes('decline') || t.includes('declining')) return { icon: '↓', color: 'text-orange-400' }
  if (t.includes('strong') && t.includes('improv')) return { icon: '↑↑', color: 'text-green-500' }
  if (t.includes('improv')) return { icon: '↑', color: 'text-green-400' }
  if (t.includes('stable')) return { icon: '→', color: 'text-blue-400' }
  return { icon: '—', color: 'text-muted-foreground' }
}

// Helper to render a single metric with all its component scores
function renderMetric(key: string, metricObj: unknown) {
  if (typeof metricObj !== 'object' || metricObj === null) return null
  const m = metricObj as Record<string, number | string | null | undefined>

  // Extract all scores
  const rawScore = typeof m.rawScore === 'number' ? m.rawScore : (typeof m.raw_score === 'number' ? m.raw_score : null)
  const trendScore = typeof m.trendScore === 'number' ? m.trendScore : (typeof m.trend_score === 'number' ? m.trend_score : null)
  const sectorPct = typeof m.sectorPercentile === 'number' ? m.sectorPercentile : (typeof m.sector_percentile === 'number' ? m.sector_percentile : null)
  const stability = typeof m.stability === 'number' ? m.stability : null

  // Extract values
  const current = typeof m.current === 'number' ? m.current : (typeof m.value === 'number' ? m.value : null)
  const trend = typeof m.trend === 'string' ? m.trend : null
  const trendDisplay = getTrendDisplay(trend)

  // Format margin as percentage if it's a ratio
  const formatValue = (val: number | null) => {
    if (val === null) return 'N/A'
    // If it's a margin ratio (between -1 and 2), show as percentage
    if (Math.abs(val) <= 2) return `${(val * 100).toFixed(1)}%`
    return formatNumber(val)
  }

  return (
    <div key={key} className="bg-muted/30 rounded-lg p-4">
      {/* Metric name and current value */}
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <span className="font-medium text-foreground">
            {formatMetricName(key)}
          </span>
          <span className={cn('text-lg', trendDisplay.color)}>
            {trendDisplay.icon}
          </span>
        </div>
        {current !== null && (
          <span className="font-mono font-semibold text-foreground">
            {formatValue(current)}
          </span>
        )}
      </div>

      {/* Four component scores for this metric */}
      <div className="grid grid-cols-4 gap-2 text-xs">
        <div className="text-center">
          <div className="text-muted-foreground mb-1">Level</div>
          <div className={cn('font-mono font-semibold', getScoreColor(rawScore))}>
            {rawScore !== null ? rawScore.toFixed(0) : '—'}
          </div>
        </div>
        <div className="text-center">
          <div className="text-muted-foreground mb-1">vs Peers</div>
          <div className={cn('font-mono font-semibold', getScoreColor(sectorPct))}>
            {sectorPct !== null ? sectorPct.toFixed(0) : '—'}
          </div>
        </div>
        <div className="text-center">
          <div className="text-muted-foreground mb-1">Trend</div>
          <div className={cn('font-mono font-semibold', getScoreColor(trendScore))}>
            {trendScore !== null ? trendScore.toFixed(0) : '—'}
          </div>
        </div>
        <div className="text-center">
          <div className="text-muted-foreground mb-1">Stability</div>
          <div className={cn('font-mono font-semibold', getScoreColor(stability))}>
            {stability !== null ? stability.toFixed(0) : '—'}
          </div>
        </div>
      </div>
    </div>
  )
}

export function DimensionDetailModal({
  isOpen,
  onClose,
  dimensionCode,
  detail,
  isLoading,
}: DimensionDetailModalProps) {
  if (!isOpen) return null

  const displayName = dimensionNames[dimensionCode] || dimensionCode

  // Extract component scores if available (cast to proper type)
  const rawComponentScores = detail?.metadata?.componentScores || detail?.metadata?.component_scores
  const componentScores = (typeof rawComponentScores === 'object' && rawComponentScores !== null)
    ? rawComponentScores as Record<string, number>
    : null

  // Extract metrics if available (cast to proper type)
  const rawMetrics = detail?.metadata?.metrics
  const metrics = (typeof rawMetrics === 'object' && rawMetrics !== null)
    ? rawMetrics as Record<string, unknown>
    : null

  // Get interpretation/context (can be string or object)
  const rawInterpretation = detail?.metadata?.interpretation || detail?.metadata?.value_context
  const interpretation: string | Record<string, unknown> | null =
    typeof rawInterpretation === 'string' ? rawInterpretation :
    (typeof rawInterpretation === 'object' && rawInterpretation !== null) ? rawInterpretation as Record<string, unknown> :
    null

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center"
      onClick={onClose}
    >
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/60" />

      {/* Modal */}
      <div
        className="relative bg-card border border-border rounded-lg shadow-xl max-w-2xl w-full mx-4 max-h-[85vh] overflow-hidden"
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-border">
          <div>
            <h2 className="text-xl font-semibold text-foreground">{displayName}</h2>
            {detail?.score !== null && detail?.score !== undefined && (
              <div className="flex items-center gap-2 mt-1">
                <span className="text-sm text-muted-foreground">Score:</span>
                <span className={cn('text-lg font-mono font-bold', getScoreColor(detail.score))}>
                  {Math.round(detail.score)}
                </span>
                {detail.scoreLow !== null && detail.scoreHigh !== null && (
                  <span className="text-xs text-muted-foreground">
                    (range: {Math.round(detail.scoreLow)} - {Math.round(detail.scoreHigh)})
                  </span>
                )}
              </div>
            )}
          </div>
          <button
            onClick={onClose}
            className="p-2 text-muted-foreground hover:text-foreground rounded-lg hover:bg-muted transition-colors"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Content */}
        <div className="px-6 py-4 overflow-y-auto max-h-[calc(85vh-80px)]">
          {isLoading ? (
            <div className="flex items-center justify-center py-12">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary" />
            </div>
          ) : !detail ? (
            <div className="text-center py-12 text-muted-foreground">
              No data available for this dimension
            </div>
          ) : (
            <div className="space-y-6">
              {/* Component Scores with explanation */}
              {componentScores && Object.keys(componentScores).length > 0 && (
                <div>
                  <h3 className="text-sm font-semibold text-foreground mb-2">How the Score is Calculated</h3>
                  <p className="text-xs text-muted-foreground mb-3">
                    The final score combines four components with different weights:
                  </p>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                    {[
                      { key: 'raw', label: 'Level', weight: 40, desc: 'Absolute value' },
                      { key: 'peer', label: 'vs Peers', weight: 25, desc: 'Sector comparison' },
                      { key: 'trend', label: 'Trend', weight: 25, desc: 'Improving or declining' },
                      { key: 'stability', label: 'Stability', weight: 10, desc: 'Consistency' },
                    ].map(({ key, label, weight, desc }) => {
                      const value = componentScores[key]
                      return (
                        <div key={key} className="bg-muted/50 rounded-lg p-3">
                          <div className="flex items-center justify-between mb-1">
                            <span className="text-xs text-muted-foreground">{label}</span>
                            <span className="text-xs text-muted-foreground">×{weight}%</span>
                          </div>
                          <div className={cn('text-lg font-mono font-semibold', getScoreColor(value))}>
                            {typeof value === 'number' ? value.toFixed(1) : 'N/A'}
                          </div>
                          <div className="text-xs text-muted-foreground mt-1">{desc}</div>
                        </div>
                      )
                    })}
                  </div>
                </div>
              )}

              {/* Metrics Breakdown */}
              {metrics !== null && Object.keys(metrics).length > 0 ? (
                <div>
                  <h3 className="text-sm font-semibold text-foreground mb-2">Metrics Breakdown</h3>
                  <p className="text-xs text-muted-foreground mb-3">
                    Each metric contributes to the component scores above. Level = absolute value, vs Peers = sector percentile, Trend = direction, Stability = consistency.
                  </p>
                  <div className="space-y-3">
                    {Object.entries(metrics)
                      .map(([key, metricObj]) => renderMetric(key, metricObj))
                      .filter((el): el is JSX.Element => el !== null)}
                  </div>
                </div>
              ) : null}

              {/* Interpretation/Context */}
              {interpretation !== null && (
                <div>
                  <h3 className="text-sm font-semibold text-foreground mb-2">Interpretation</h3>
                  <div className="bg-muted/30 rounded-lg p-4">
                    {typeof interpretation === 'string' ? (
                      <p className="text-sm text-foreground">{interpretation}</p>
                    ) : (
                      <div className="space-y-1 text-sm">
                        {Object.entries(interpretation).map(([key, value]) => (
                          <div key={key}>
                            <span className="text-muted-foreground capitalize">{formatMetricName(key)}: </span>
                            <span className="text-foreground">{String(value ?? '').replace(/_/g, ' ')}</span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Confidence and Data Quality */}
              <div className="flex gap-4 text-sm">
                {detail.confidence !== null && (
                  <div>
                    <span className="text-muted-foreground">Confidence: </span>
                    <span className="font-mono text-foreground">
                      {(detail.confidence * 100).toFixed(0)}%
                    </span>
                  </div>
                )}
                {detail.dataQuality !== null && (
                  <div>
                    <span className="text-muted-foreground">Data Quality: </span>
                    <span className="font-mono text-foreground">
                      {(detail.dataQuality * 100).toFixed(0)}%
                    </span>
                  </div>
                )}
              </div>

              {/* Raw metadata for debugging (collapsed) */}
              <details className="text-xs">
                <summary className="cursor-pointer text-muted-foreground hover:text-foreground">
                  View raw metadata
                </summary>
                <pre className="mt-2 p-3 bg-muted rounded-lg overflow-x-auto text-muted-foreground">
                  {JSON.stringify(detail.metadata, null, 2)}
                </pre>
              </details>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
