/**
 * PriceChart Component
 * Interactive price chart using recharts with optional score overlay
 */

import { useMemo } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Area,
  ComposedChart,
} from 'recharts'
import { cn } from '@yodabuffett/ui'
import type { PriceDataPoint, PriceTimeRange, HistoricalScorePoint, AnomalyPoint } from '../types'

interface PriceChartProps {
  prices: PriceDataPoint[]
  currentRange: PriceTimeRange
  onRangeChange: (range: PriceTimeRange) => void
  loading?: boolean
  // Score overlay props
  historicalScores?: HistoricalScorePoint[]
  showScoreOverlay?: boolean
  onToggleScoreOverlay?: () => void
  // Anomaly overlay props
  anomalies?: AnomalyPoint[]
  showAnomalyOverlay?: boolean
  onToggleAnomalyOverlay?: () => void
}

const timeRanges: PriceTimeRange[] = ['1M', '3M', '6M', '1Y', '3Y', '5Y', 'MAX']

// Format date for display
function formatDate(dateStr: string, range: PriceTimeRange): string {
  const date = new Date(dateStr)
  if (range === '1M' || range === '3M') {
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
  }
  if (range === '6M' || range === '1Y') {
    return date.toLocaleDateString('en-US', { month: 'short', year: '2-digit' })
  }
  return date.toLocaleDateString('en-US', { year: 'numeric' })
}

// Format price for tooltip
function formatPrice(value: number): string {
  return value.toLocaleString('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })
}

// Extended data point with score and anomaly
interface ChartDataPoint extends PriceDataPoint {
  dateFormatted: string
  fatPitchScore?: number
  anomalyScore?: number
}

// Custom tooltip component
function CustomTooltip({ active, payload }: any) {
  if (!active || !payload || !payload.length) return null

  const data = payload[0].payload as ChartDataPoint
  const date = new Date(data.date)

  return (
    <div className="bg-card border border-border rounded-lg p-3 shadow-lg">
      <p className="text-sm font-medium text-foreground mb-1">
        {date.toLocaleDateString('en-US', {
          weekday: 'short',
          month: 'short',
          day: 'numeric',
          year: 'numeric',
        })}
      </p>
      <div className="space-y-1 text-sm">
        <div className="flex justify-between gap-4">
          <span className="text-muted-foreground">Close:</span>
          <span className="font-mono text-foreground">{formatPrice(data.close)}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-muted-foreground">High:</span>
          <span className="font-mono text-foreground">{formatPrice(data.high)}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-muted-foreground">Low:</span>
          <span className="font-mono text-foreground">{formatPrice(data.low)}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-muted-foreground">Volume:</span>
          <span className="font-mono text-foreground">
            {(data.volume / 1e6).toFixed(1)}M
          </span>
        </div>
        {data.fatPitchScore !== undefined && (
          <div className="flex justify-between gap-4 pt-1 border-t border-border/50 mt-1">
            <span className={cn(
              data.fatPitchScore >= 70 ? 'text-green-500' :
              data.fatPitchScore >= 60 ? 'text-blue-500' : 'text-red-500'
            )}>Fat Pitch Score:</span>
            <span className={cn(
              'font-mono',
              data.fatPitchScore >= 70 ? 'text-green-500' :
              data.fatPitchScore >= 60 ? 'text-blue-500' : 'text-red-500'
            )}>{data.fatPitchScore.toFixed(1)}</span>
          </div>
        )}
        {data.anomalyScore !== undefined && (
          <div className="flex justify-between gap-4 pt-1 border-t border-border/50 mt-1">
            <span className={cn(
              data.anomalyScore >= 40 ? 'text-orange-500' :
              data.anomalyScore >= 25 ? 'text-yellow-500' : 'text-green-500'
            )}>Anomaly Score:</span>
            <span className={cn(
              'font-mono',
              data.anomalyScore >= 40 ? 'text-orange-500' :
              data.anomalyScore >= 25 ? 'text-yellow-500' : 'text-green-500'
            )}>{data.anomalyScore.toFixed(1)}</span>
          </div>
        )}
      </div>
    </div>
  )
}

export function PriceChart({
  prices,
  currentRange,
  onRangeChange,
  loading = false,
  historicalScores,
  showScoreOverlay = false,
  onToggleScoreOverlay,
  anomalies,
  showAnomalyOverlay = false,
  onToggleAnomalyOverlay,
}: PriceChartProps) {
  // Calculate performance stats
  const stats = useMemo(() => {
    if (!prices || prices.length < 2) return null

    const first = prices[0]
    const last = prices[prices.length - 1]
    const change = last.close - first.close
    const changePercent = (change / first.close) * 100
    const high = Math.max(...prices.map(p => p.high))
    const low = Math.min(...prices.map(p => p.low))

    return {
      change,
      changePercent,
      high,
      low,
      current: last.close,
    }
  }, [prices])

  // Create a map of scores by date for efficient lookup
  const scoresByDate = useMemo(() => {
    if (!historicalScores) return new Map<string, number>()
    const map = new Map<string, number>()
    for (const s of historicalScores) {
      map.set(s.scoreDate, s.score)
    }
    return map
  }, [historicalScores])

  // Create a map of anomalies by date for efficient lookup
  const anomaliesByDate = useMemo(() => {
    if (!anomalies) return new Map<string, number>()
    const map = new Map<string, number>()
    for (const a of anomalies) {
      map.set(a.date, a.anomalyScore)
    }
    return map
  }, [anomalies])

  // Prepare chart data with optional score and anomaly overlay
  const chartData = useMemo(() => {
    if (!prices) return []

    return prices.map(p => {
      const baseData: ChartDataPoint = {
        ...p,
        dateFormatted: formatDate(p.date, currentRange),
      }

      // Add score if overlay is enabled and we have score data
      if (showScoreOverlay && scoresByDate.size > 0) {
        // Find the most recent score on or before this date
        const priceDate = p.date
        let latestScore: number | undefined
        let latestScoreDate: string | undefined

        for (const [scoreDate, score] of scoresByDate.entries()) {
          if (scoreDate <= priceDate) {
            if (!latestScoreDate || scoreDate > latestScoreDate) {
              latestScoreDate = scoreDate
              latestScore = score
            }
          }
        }

        if (latestScore !== undefined) {
          baseData.fatPitchScore = latestScore
        }
      }

      // Add anomaly if overlay is enabled and we have anomaly data
      if (showAnomalyOverlay && anomaliesByDate.size > 0) {
        // Find the most recent anomaly on or before this date
        const priceDate = p.date
        let latestAnomaly: number | undefined
        let latestAnomalyDate: string | undefined

        for (const [anomalyDate, score] of anomaliesByDate.entries()) {
          if (anomalyDate <= priceDate) {
            if (!latestAnomalyDate || anomalyDate > latestAnomalyDate) {
              latestAnomalyDate = anomalyDate
              latestAnomaly = score
            }
          }
        }

        if (latestAnomaly !== undefined) {
          baseData.anomalyScore = latestAnomaly
        }
      }

      return baseData
    })
  }, [prices, currentRange, showScoreOverlay, scoresByDate, showAnomalyOverlay, anomaliesByDate])

  // Check if we have score data available
  const hasScoreData = historicalScores && historicalScores.length > 0

  // Check if we have anomaly data available
  const hasAnomalyData = anomalies && anomalies.length > 0

  // Determine chart color based on performance
  const isPositive = stats ? stats.changePercent >= 0 : true
  const chartColor = isPositive ? '#22c55e' : '#ef4444'

  if (loading) {
    return (
      <div className="bg-card border border-border rounded-lg p-6">
        <div className="h-[300px] flex items-center justify-center">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
        </div>
      </div>
    )
  }

  if (!prices || prices.length === 0) {
    return (
      <div className="bg-card border border-border rounded-lg p-6">
        <div className="h-[300px] flex items-center justify-center text-muted-foreground">
          Price data not available
        </div>
      </div>
    )
  }

  return (
    <div className="bg-card border border-border rounded-lg p-6">
      {/* Header with stats */}
      <div className="flex items-start justify-between mb-4">
        <div>
          <h3 className="text-lg font-semibold text-foreground">Price History</h3>
          {stats && (
            <div className="flex items-baseline gap-2 mt-1">
              <span className="text-2xl font-bold text-foreground">
                {formatPrice(stats.current)}
              </span>
              <span className={cn(
                'text-sm font-medium',
                isPositive ? 'text-green-500' : 'text-red-500'
              )}>
                {stats.changePercent >= 0 ? '+' : ''}{stats.changePercent.toFixed(2)}%
              </span>
            </div>
          )}
        </div>

        <div className="flex items-center gap-4">
          {/* Score overlay toggle */}
          {hasScoreData && onToggleScoreOverlay && (
            <button
              onClick={onToggleScoreOverlay}
              className={cn(
                'px-3 py-1.5 text-sm rounded-lg transition-colors flex items-center gap-2',
                showScoreOverlay
                  ? 'bg-blue-600 text-white'
                  : 'bg-muted text-muted-foreground hover:text-foreground'
              )}
            >
              <svg
                className="w-4 h-4"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                />
              </svg>
              Score
            </button>
          )}

          {/* Anomaly overlay toggle */}
          {hasAnomalyData && onToggleAnomalyOverlay && (
            <button
              onClick={onToggleAnomalyOverlay}
              className={cn(
                'px-3 py-1.5 text-sm rounded-lg transition-colors flex items-center gap-2',
                showAnomalyOverlay
                  ? 'bg-orange-600 text-white'
                  : 'bg-muted text-muted-foreground hover:text-foreground'
              )}
            >
              <svg
                className="w-4 h-4"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                />
              </svg>
              Anomaly
            </button>
          )}

          {/* Time range selector */}
          <div className="flex gap-1">
            {timeRanges.map(range => (
              <button
                key={range}
                onClick={() => onRangeChange(range)}
                className={cn(
                  'px-3 py-1 text-sm rounded-lg transition-colors',
                  currentRange === range
                    ? 'bg-blue-600 text-white'
                    : 'text-muted-foreground hover:text-foreground hover:bg-muted'
                )}
              >
                {range}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Chart */}
      <div className="h-[300px]">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={chartData} margin={{ top: 10, right: (showScoreOverlay || showAnomalyOverlay) ? 60 : 10, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="colorPrice" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={chartColor} stopOpacity={0.3} />
                <stop offset="95%" stopColor={chartColor} stopOpacity={0} />
              </linearGradient>
              {/* Score color gradient: green (>=70), blue (60-70), red (<60) */}
              <linearGradient id="scoreGradient" x1="0" y1="0" x2="0" y2="1">
                {/* Top = 100, bottom = 0 */}
                <stop offset="0%" stopColor="#22c55e" />      {/* score 100 - green */}
                <stop offset="30%" stopColor="#22c55e" />     {/* score 70 - green */}
                <stop offset="30%" stopColor="#3b82f6" />     {/* score 70 - blue */}
                <stop offset="40%" stopColor="#3b82f6" />     {/* score 60 - blue */}
                <stop offset="40%" stopColor="#ef4444" />     {/* score 60 - red */}
                <stop offset="100%" stopColor="#ef4444" />    {/* score 0 - red */}
              </linearGradient>
              {/* Anomaly color gradient: green (<25), yellow (25-40), orange (>=40) */}
              <linearGradient id="anomalyGradient" x1="0" y1="0" x2="0" y2="1">
                {/* Top = 100 (high anomaly = bad), bottom = 0 (low anomaly = good) */}
                <stop offset="0%" stopColor="#f97316" />      {/* anomaly 100 - orange (bad) */}
                <stop offset="60%" stopColor="#f97316" />     {/* anomaly 40 - orange */}
                <stop offset="60%" stopColor="#eab308" />     {/* anomaly 40 - yellow */}
                <stop offset="75%" stopColor="#eab308" />     {/* anomaly 25 - yellow */}
                <stop offset="75%" stopColor="#22c55e" />     {/* anomaly 25 - green */}
                <stop offset="100%" stopColor="#22c55e" />    {/* anomaly 0 - green (good) */}
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" className="opacity-20" />
            <XAxis
              dataKey="dateFormatted"
              tick={{ fill: 'currentColor', fontSize: 11 }}
              className="text-muted-foreground"
              tickLine={false}
              axisLine={false}
              interval="preserveStartEnd"
              minTickGap={50}
            />
            <YAxis
              yAxisId="price"
              domain={['auto', 'auto']}
              tick={{ fill: 'currentColor', fontSize: 11 }}
              className="text-muted-foreground"
              tickLine={false}
              axisLine={false}
              tickFormatter={formatPrice}
              width={60}
            />
            {(showScoreOverlay || showAnomalyOverlay) && (
              <YAxis
                yAxisId="overlay"
                orientation="right"
                domain={[0, 100]}
                tick={{ fill: 'currentColor', fontSize: 11 }}
                className="text-muted-foreground"
                tickLine={false}
                axisLine={false}
                width={40}
                ticks={showAnomalyOverlay ? [0, 25, 40, 100] : [0, 60, 70, 100]}
              />
            )}
            <Tooltip content={<CustomTooltip />} />
            <Area
              yAxisId="price"
              type="monotone"
              dataKey="close"
              stroke={chartColor}
              strokeWidth={2}
              fill="url(#colorPrice)"
            />
            {showScoreOverlay && (
              <Line
                yAxisId="overlay"
                type="stepAfter"
                dataKey="fatPitchScore"
                stroke="url(#scoreGradient)"
                strokeWidth={2}
                dot={false}
                connectNulls
              />
            )}
            {showAnomalyOverlay && (
              <Line
                yAxisId="overlay"
                type="stepAfter"
                dataKey="anomalyScore"
                stroke="url(#anomalyGradient)"
                strokeWidth={2}
                dot={false}
                connectNulls
                strokeDasharray="5 3"
              />
            )}
          </ComposedChart>
        </ResponsiveContainer>
      </div>

      {/* High/Low stats and legend */}
      <div className="flex justify-between items-start mt-4 text-sm">
        {stats && (
          <div className="flex gap-6">
            <div>
              <span className="text-muted-foreground">Period High: </span>
              <span className="font-mono text-foreground">{formatPrice(stats.high)}</span>
            </div>
            <div>
              <span className="text-muted-foreground">Period Low: </span>
              <span className="font-mono text-foreground">{formatPrice(stats.low)}</span>
            </div>
          </div>
        )}
        {(showScoreOverlay || showAnomalyOverlay) && (
          <div className="flex items-center gap-4 flex-wrap">
            <div className="flex items-center gap-2">
              <div className={cn(
                'w-4 h-0.5',
                isPositive ? 'bg-green-500' : 'bg-red-500'
              )} />
              <span className="text-muted-foreground">Price</span>
            </div>
            {showScoreOverlay && (
              <>
                <span className="text-muted-foreground">|</span>
                <span className="text-xs text-muted-foreground">Score:</span>
                <div className="flex items-center gap-1">
                  <div className="w-3 h-0.5 bg-green-500" />
                  <span className="text-green-500 text-xs">&ge;70</span>
                </div>
                <div className="flex items-center gap-1">
                  <div className="w-3 h-0.5 bg-blue-500" />
                  <span className="text-blue-500 text-xs">60-70</span>
                </div>
                <div className="flex items-center gap-1">
                  <div className="w-3 h-0.5 bg-red-500" />
                  <span className="text-red-500 text-xs">&lt;60</span>
                </div>
              </>
            )}
            {showAnomalyOverlay && (
              <>
                <span className="text-muted-foreground">|</span>
                <span className="text-xs text-muted-foreground">Anomaly:</span>
                <div className="flex items-center gap-1">
                  <div className="w-3 h-0.5 bg-green-500" style={{ borderStyle: 'dashed' }} />
                  <span className="text-green-500 text-xs">&lt;25</span>
                </div>
                <div className="flex items-center gap-1">
                  <div className="w-3 h-0.5 bg-yellow-500" style={{ borderStyle: 'dashed' }} />
                  <span className="text-yellow-500 text-xs">25-40</span>
                </div>
                <div className="flex items-center gap-1">
                  <div className="w-3 h-0.5 bg-orange-500" style={{ borderStyle: 'dashed' }} />
                  <span className="text-orange-500 text-xs">&ge;40</span>
                </div>
              </>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

// Simplified mini chart for cards/lists
export function PriceChartMini({ prices }: { prices: PriceDataPoint[] }) {
  if (!prices || prices.length < 2) return null

  const first = prices[0]
  const last = prices[prices.length - 1]
  const isPositive = last.close >= first.close
  const color = isPositive ? '#22c55e' : '#ef4444'

  return (
    <div className="h-12 w-24">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={prices}>
          <Line
            type="monotone"
            dataKey="close"
            stroke={color}
            strokeWidth={1.5}
            dot={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
