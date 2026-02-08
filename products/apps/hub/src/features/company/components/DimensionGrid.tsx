/**
 * DimensionGrid Component
 * Displays all 14 dimension scores in a grid layout with visual bars
 */

import { cn } from '@yodabuffett/ui'

interface DimensionGridProps {
  dimensionScores: Record<string, number>
  dimensionContributions?: Record<string, number>
  showContributions?: boolean
}

// Dimension metadata
const dimensionMeta: Record<string, { name: string; description: string; category: 'fundamental' | 'market' }> = {
  profitability: { name: 'Profitability', description: 'Margin levels and trends', category: 'fundamental' },
  returns: { name: 'Returns', description: 'ROE, ROIC, DuPont analysis', category: 'fundamental' },
  growth: { name: 'Growth', description: 'Revenue/earnings growth, CAGR', category: 'fundamental' },
  financial_health: { name: 'Financial Health', description: 'Debt coverage, liquidity, Z-score', category: 'fundamental' },
  earnings_quality: { name: 'Earnings Quality', description: 'Accruals, cash backing', category: 'fundamental' },
  capital_allocation: { name: 'Capital Allocation', description: 'Reinvestment, dividend coverage', category: 'fundamental' },
  working_capital: { name: 'Working Capital', description: 'Operating efficiency', category: 'fundamental' },
  beneish_mscore: { name: 'Beneish M-Score', description: 'Earnings manipulation detection', category: 'fundamental' },
  value: { name: 'Value', description: 'P/E, P/B, EV/EBITDA vs peers', category: 'market' },
  risk: { name: 'Risk', description: 'Volatility, drawdown, beta', category: 'market' },
  momentum: { name: 'Momentum', description: 'Price trends, RSI', category: 'market' },
  quality: { name: 'Quality', description: 'Overall quality composite', category: 'market' },
  valuation_percentile: { name: 'Valuation %ile', description: 'Historical cheapness ranking', category: 'market' },
  sentiment: { name: 'Sentiment', description: 'Communication pattern analysis', category: 'market' },
}

// Order for display
const displayOrder = [
  // Fundamentals first
  'profitability',
  'returns',
  'growth',
  'financial_health',
  'earnings_quality',
  'capital_allocation',
  'working_capital',
  'beneish_mscore',
  // Then market perception
  'quality',
  'value',
  'momentum',
  'risk',
  'valuation_percentile',
  'sentiment',
]

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

interface DimensionBarProps {
  code: string
  score: number
  contribution?: number
  showContribution?: boolean
}

function DimensionBar({ code, score, contribution, showContribution }: DimensionBarProps) {
  const meta = dimensionMeta[code] || { name: code, description: '', category: 'fundamental' }
  const roundedScore = Math.round(score)

  return (
    <div className="space-y-1.5">
      <div className="flex justify-between items-baseline">
        <div>
          <span className="text-sm font-medium text-foreground">{meta.name}</span>
          {showContribution && contribution !== undefined && (
            <span className="text-xs text-muted-foreground ml-2">
              (+{contribution.toFixed(1)} pts)
            </span>
          )}
        </div>
        <span className={cn('text-sm font-mono font-semibold', getTextColor(score))}>
          {roundedScore}
        </span>
      </div>
      <div className="w-full h-2.5 bg-muted rounded-full overflow-hidden">
        <div
          className={cn('h-full rounded-full transition-all duration-500', getScoreColor(score))}
          style={{ width: `${Math.min(100, Math.max(0, score))}%` }}
        />
      </div>
      <p className="text-xs text-muted-foreground">{meta.description}</p>
    </div>
  )
}

export function DimensionGrid({ dimensionScores, dimensionContributions, showContributions = false }: DimensionGridProps) {
  // Separate into categories
  const fundamentals = displayOrder.filter(d => dimensionMeta[d]?.category === 'fundamental')
  const market = displayOrder.filter(d => dimensionMeta[d]?.category === 'market')

  return (
    <div className="space-y-6">
      {/* Business Fundamentals */}
      <div>
        <h3 className="text-lg font-semibold text-foreground mb-4">Business Fundamentals</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-4">
          {fundamentals.map(code => {
            const score = dimensionScores[code]
            if (score === undefined || score === null) return null
            return (
              <DimensionBar
                key={code}
                code={code}
                score={score}
                contribution={dimensionContributions?.[code]}
                showContribution={showContributions}
              />
            )
          })}
        </div>
      </div>

      {/* Market Perception */}
      <div>
        <h3 className="text-lg font-semibold text-foreground mb-4">Market Perception</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-4">
          {market.map(code => {
            const score = dimensionScores[code]
            if (score === undefined || score === null) return null
            return (
              <DimensionBar
                key={code}
                code={code}
                score={score}
                contribution={dimensionContributions?.[code]}
                showContribution={showContributions}
              />
            )
          })}
        </div>
      </div>
    </div>
  )
}

// Compact version for sidebars or summaries
export function DimensionGridCompact({ dimensionScores }: { dimensionScores: Record<string, number> }) {
  return (
    <div className="grid grid-cols-2 gap-3">
      {displayOrder.map(code => {
        const score = dimensionScores[code]
        if (score === undefined || score === null) return null
        const meta = dimensionMeta[code] || { name: code }
        const roundedScore = Math.round(score)

        return (
          <div key={code} className="flex items-center gap-2">
            <div className="w-16 h-1.5 bg-muted rounded-full overflow-hidden">
              <div
                className={cn('h-full rounded-full', getScoreColor(score))}
                style={{ width: `${Math.min(100, Math.max(0, score))}%` }}
              />
            </div>
            <span className="text-xs text-muted-foreground truncate flex-1">{meta.name}</span>
            <span className={cn('text-xs font-mono', getTextColor(score))}>{roundedScore}</span>
          </div>
        )
      })}
    </div>
  )
}
