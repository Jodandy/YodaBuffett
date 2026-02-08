/**
 * PriceChart Component
 * Interactive price chart using recharts
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
  AreaChart,
} from 'recharts'
import { cn } from '@yodabuffett/ui'
import type { PriceDataPoint, PriceTimeRange } from '../types'

interface PriceChartProps {
  prices: PriceDataPoint[]
  currentRange: PriceTimeRange
  onRangeChange: (range: PriceTimeRange) => void
  loading?: boolean
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

// Custom tooltip component
function CustomTooltip({ active, payload }: any) {
  if (!active || !payload || !payload.length) return null

  const data = payload[0].payload as PriceDataPoint
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
      </div>
    </div>
  )
}

export function PriceChart({
  prices,
  currentRange,
  onRangeChange,
  loading = false,
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

  // Prepare chart data
  const chartData = useMemo(() => {
    if (!prices) return []
    return prices.map(p => ({
      ...p,
      dateFormatted: formatDate(p.date, currentRange),
    }))
  }, [prices, currentRange])

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

      {/* Chart */}
      <div className="h-[300px]">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="colorPrice" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor={chartColor} stopOpacity={0.3} />
                <stop offset="95%" stopColor={chartColor} stopOpacity={0} />
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
              domain={['auto', 'auto']}
              tick={{ fill: 'currentColor', fontSize: 11 }}
              className="text-muted-foreground"
              tickLine={false}
              axisLine={false}
              tickFormatter={formatPrice}
              width={60}
            />
            <Tooltip content={<CustomTooltip />} />
            <Area
              type="monotone"
              dataKey="close"
              stroke={chartColor}
              strokeWidth={2}
              fill="url(#colorPrice)"
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* High/Low stats */}
      {stats && (
        <div className="flex gap-6 mt-4 text-sm">
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
