/**
 * DimensionBar Component
 * Displays a single dimension score as a horizontal bar
 */

interface DimensionBarProps {
  code: string
  score: number
  showLabel?: boolean
  compact?: boolean
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
  valuation_percentile: 'Valuation %',
  sentiment: 'Sentiment',
}

// Get color based on score
function getScoreColor(score: number): string {
  if (score >= 80) return 'bg-green-500'
  if (score >= 60) return 'bg-green-400'
  if (score >= 40) return 'bg-yellow-400'
  if (score >= 20) return 'bg-orange-400'
  return 'bg-red-400'
}

// Get text color based on score
function getTextColor(score: number): string {
  if (score >= 60) return 'text-green-400'
  if (score >= 40) return 'text-yellow-400'
  return 'text-red-400'
}

export function DimensionBar({ code, score, showLabel = true, compact = false }: DimensionBarProps) {
  const displayName = dimensionNames[code] || code
  const roundedScore = Math.round(score)

  if (compact) {
    return (
      <div className="flex items-center gap-1" title={`${displayName}: ${roundedScore}`}>
        <div className="w-12 h-1.5 bg-muted rounded-full overflow-hidden">
          <div
            className={`h-full ${getScoreColor(score)} transition-all duration-300`}
            style={{ width: `${Math.min(100, Math.max(0, score))}%` }}
          />
        </div>
        <span className={`text-xs font-mono ${getTextColor(score)}`}>
          {roundedScore}
        </span>
      </div>
    )
  }

  return (
    <div className="space-y-1">
      {showLabel && (
        <div className="flex justify-between text-xs">
          <span className="text-muted-foreground">{displayName}</span>
          <span className={`font-mono ${getTextColor(score)}`}>{roundedScore}</span>
        </div>
      )}
      <div className="w-full h-2 bg-muted rounded-full overflow-hidden">
        <div
          className={`h-full ${getScoreColor(score)} transition-all duration-300`}
          style={{ width: `${Math.min(100, Math.max(0, score))}%` }}
        />
      </div>
    </div>
  )
}
