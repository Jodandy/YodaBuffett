/**
 * MetricCard Component
 * Displays a single metric in a card format
 */

import { cn } from '@yodabuffett/ui'

interface MetricCardProps {
  label: string
  value: string | number | null | undefined
  sublabel?: string
  change?: number
  format?: 'number' | 'currency' | 'percentage' | 'multiple'
  size?: 'sm' | 'md' | 'lg'
  className?: string
}

function formatValue(
  value: string | number | null | undefined,
  format: 'number' | 'currency' | 'percentage' | 'multiple' = 'number'
): string {
  if (value === null || value === undefined) return '-'
  if (typeof value === 'string') return value

  switch (format) {
    case 'currency':
      if (Math.abs(value) >= 1e12) {
        return `$${(value / 1e12).toFixed(1)}T`
      } else if (Math.abs(value) >= 1e9) {
        return `$${(value / 1e9).toFixed(1)}B`
      } else if (Math.abs(value) >= 1e6) {
        return `$${(value / 1e6).toFixed(1)}M`
      } else if (Math.abs(value) >= 1e3) {
        return `$${(value / 1e3).toFixed(1)}K`
      }
      return `$${value.toFixed(0)}`

    case 'percentage':
      return `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`

    case 'multiple':
      return `${value.toFixed(1)}x`

    default:
      if (Math.abs(value) >= 1e9) {
        return `${(value / 1e9).toFixed(1)}B`
      } else if (Math.abs(value) >= 1e6) {
        return `${(value / 1e6).toFixed(1)}M`
      } else if (Math.abs(value) >= 1e3) {
        return `${(value / 1e3).toFixed(1)}K`
      }
      return value.toFixed(value % 1 === 0 ? 0 : 2)
  }
}

export function MetricCard({
  label,
  value,
  sublabel,
  change,
  format = 'number',
  size = 'md',
  className,
}: MetricCardProps) {
  const sizeClasses = {
    sm: 'p-3',
    md: 'p-4',
    lg: 'p-6',
  }

  const valueSizeClasses = {
    sm: 'text-lg',
    md: 'text-2xl',
    lg: 'text-3xl',
  }

  return (
    <div className={cn(
      'bg-card border border-border rounded-lg',
      sizeClasses[size],
      className
    )}>
      <p className="text-sm text-muted-foreground mb-1">{label}</p>
      <div className="flex items-baseline gap-2">
        <p className={cn('font-bold text-foreground', valueSizeClasses[size])}>
          {formatValue(value, format)}
        </p>
        {change !== undefined && (
          <span className={cn(
            'text-sm font-medium',
            change > 0 ? 'text-green-500' : change < 0 ? 'text-red-500' : 'text-muted-foreground'
          )}>
            {change >= 0 ? '+' : ''}{change.toFixed(1)}%
          </span>
        )}
      </div>
      {sublabel && (
        <p className="text-xs text-muted-foreground mt-1">{sublabel}</p>
      )}
    </div>
  )
}

// Grid of metric cards
interface MetricGridProps {
  metrics: Array<{
    label: string
    value: string | number | null | undefined
    sublabel?: string
    change?: number
    format?: 'number' | 'currency' | 'percentage' | 'multiple'
  }>
  columns?: 2 | 3 | 4
}

export function MetricGrid({ metrics, columns = 4 }: MetricGridProps) {
  const colClasses = {
    2: 'grid-cols-2',
    3: 'grid-cols-2 md:grid-cols-3',
    4: 'grid-cols-2 md:grid-cols-4',
  }

  return (
    <div className={cn('grid gap-4', colClasses[columns])}>
      {metrics.map((metric, i) => (
        <MetricCard key={i} {...metric} />
      ))}
    </div>
  )
}
