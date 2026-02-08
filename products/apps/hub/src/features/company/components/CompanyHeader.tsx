/**
 * CompanyHeader Component
 * Displays company name, symbol, stage, tier, and score
 */

import { Link } from 'react-router-dom'
import { ArrowLeftIcon, StarIcon, SparklesIcon } from '@heroicons/react/24/solid'
import { cn } from '@yodabuffett/ui'
import type { CompanyDetail, BusinessStage, QualityTier } from '../types'

interface CompanyHeaderProps {
  company: CompanyDetail
  backLink?: string
  backLabel?: string
}

// Stage display configuration
const stageConfig: Record<BusinessStage, { name: string; color: string; bgColor: string }> = {
  early_stage: { name: 'Early Stage', color: 'text-purple-400', bgColor: 'bg-purple-500/10' },
  growth_stage: { name: 'Growth', color: 'text-blue-400', bgColor: 'bg-blue-500/10' },
  mature_yield: { name: 'Mature Yield', color: 'text-amber-400', bgColor: 'bg-amber-500/10' },
  compounder: { name: 'Compounder', color: 'text-green-400', bgColor: 'bg-green-500/10' },
  established: { name: 'Established', color: 'text-gray-400', bgColor: 'bg-gray-500/10' },
}

// Generate tier stars
function TierStars({ tier }: { tier: QualityTier }) {
  const stars = 5 - tier + 1 // Tier 1 = 5 stars, Tier 5 = 1 star
  return (
    <div className="flex items-center gap-0.5">
      {Array.from({ length: 5 }, (_, i) => (
        <StarIcon
          key={i}
          className={cn(
            'w-4 h-4',
            i < stars ? 'text-yellow-400' : 'text-gray-600'
          )}
        />
      ))}
    </div>
  )
}

export function CompanyHeader({ company, backLink = '/screener', backLabel = 'Back to Screener' }: CompanyHeaderProps) {
  const stage = stageConfig[company.stage] || stageConfig.established

  return (
    <div className="space-y-4">
      {/* Back navigation */}
      <Link
        to={backLink}
        className="inline-flex items-center gap-2 text-muted-foreground hover:text-foreground transition-colors"
      >
        <ArrowLeftIcon className="w-4 h-4" />
        <span className="text-sm">{backLabel}</span>
      </Link>

      {/* Main header */}
      <div className="flex items-start justify-between">
        {/* Left side: Company info */}
        <div className="space-y-2">
          {/* Symbol */}
          <div className="text-sm font-mono text-muted-foreground">
            {company.symbol}
          </div>

          {/* Company name with actionable badge */}
          <div className="flex items-center gap-3">
            <h1 className="text-3xl font-bold text-foreground">
              {company.companyName}
            </h1>
            {company.isActionable && (
              <SparklesIcon className="w-6 h-6 text-yellow-400" title="Actionable" />
            )}
          </div>

          {/* Badges */}
          <div className="flex items-center gap-2 flex-wrap">
            {/* Stage badge */}
            <span className={cn(
              'text-sm px-3 py-1 rounded-full',
              stage.bgColor,
              stage.color
            )}>
              {stage.name}
            </span>

            {/* Tier badge */}
            <span className="text-sm px-3 py-1 rounded-full bg-gray-500/10 text-gray-400">
              Tier {company.qualityTier}
            </span>

            {/* Cheap badge */}
            {company.cheapnessScore > 60 && (
              <span className="text-sm px-3 py-1 rounded-full bg-green-500/10 text-green-400">
                Cheap
              </span>
            )}

            {/* Actionable badge */}
            {company.isActionable && (
              <span className="text-sm px-3 py-1 rounded-full bg-yellow-500/10 text-yellow-400">
                Actionable
              </span>
            )}
          </div>

          {/* Warnings */}
          {company.warnings && company.warnings.length > 0 && (
            <div className="flex flex-wrap gap-2 mt-2">
              {company.warnings.map((warning, i) => (
                <span
                  key={i}
                  className="text-xs px-2 py-0.5 rounded bg-red-500/10 text-red-400"
                >
                  {warning}
                </span>
              ))}
            </div>
          )}
        </div>

        {/* Right side: Score */}
        <div className="text-right">
          <div className="bg-card border border-border rounded-lg p-4 min-w-[120px]">
            <div className="text-4xl font-bold text-foreground">
              {Math.round(company.fatPitchScore)}
            </div>
            <div className="text-xs text-muted-foreground mb-2">
              Fat Pitch Score
            </div>
            <TierStars tier={company.qualityTier} />
          </div>

          {/* Sub-scores */}
          <div className="flex gap-4 mt-3 text-sm">
            <div className="text-center">
              <div className="text-lg font-semibold text-foreground">
                {Math.round(company.qualityScore)}
              </div>
              <div className="text-xs text-muted-foreground">Quality</div>
            </div>
            <div className="text-center">
              <div className="text-lg font-semibold text-foreground">
                {Math.round(company.cheapnessScore)}
              </div>
              <div className="text-xs text-muted-foreground">Cheapness</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
