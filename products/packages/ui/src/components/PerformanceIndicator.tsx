import { cn } from '../utils/cn'
import { formatPercentage } from '../utils/formatters'

interface PerformanceIndicatorProps {
  value: number
  className?: string
}

export function PerformanceIndicator({
  value,
  className,
}: PerformanceIndicatorProps) {
  const colorClass =
    value > 0
      ? 'performance-positive'
      : value < 0
        ? 'performance-negative'
        : 'performance-neutral'

  return (
    <span className={cn(colorClass, className)}>
      {formatPercentage(value)}
    </span>
  )
}
