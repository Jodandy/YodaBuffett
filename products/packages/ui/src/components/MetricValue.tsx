import { cn } from '../utils/cn'
import { formatCurrency, formatPercentage, formatNumber } from '../utils/formatters'

interface MetricValueProps {
  value: number | null | undefined
  format?: 'number' | 'percentage' | 'currency'
  decimals?: number
  colorCode?: boolean
  className?: string
}

export function MetricValue({
  value,
  format = 'number',
  decimals = 2,
  colorCode = false,
  className,
}: MetricValueProps) {
  if (value === null || value === undefined || isNaN(value)) {
    return <span className={cn('text-muted-foreground', className)}>-</span>
  }

  let formatted: string
  switch (format) {
    case 'currency':
      formatted = formatCurrency(value, decimals)
      break
    case 'percentage':
      formatted = formatPercentage(value, decimals)
      break
    default:
      formatted = formatNumber(value, decimals)
  }

  const colorClass = colorCode
    ? value > 0
      ? 'performance-positive'
      : value < 0
        ? 'performance-negative'
        : 'performance-neutral'
    : undefined

  return <span className={cn(colorClass, className)}>{formatted}</span>
}
