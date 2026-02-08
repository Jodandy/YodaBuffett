/**
 * CompanyCard Component
 * Displays a company with its dimension scores in a card layout
 */

import { Link } from 'react-router-dom'
import { StarIcon, SparklesIcon, ChartBarIcon } from '@heroicons/react/24/solid'
import { DimensionBar } from './DimensionBar'
import type { FatPitch, BusinessStage, DimensionCode } from '../types'

interface CompanyCardProps {
  pitch: FatPitch
}

// Stage display names and colors
const stageConfig: Record<BusinessStage, { name: string; color: string; bgColor: string }> = {
  early_stage: { name: 'Early Stage', color: 'text-purple-400', bgColor: 'bg-purple-500/10' },
  growth_stage: { name: 'Growth', color: 'text-blue-400', bgColor: 'bg-blue-500/10' },
  mature_yield: { name: 'Mature Yield', color: 'text-amber-400', bgColor: 'bg-amber-500/10' },
  compounder: { name: 'Compounder', color: 'text-green-400', bgColor: 'bg-green-500/10' },
  established: { name: 'Established', color: 'text-gray-400', bgColor: 'bg-gray-500/10' },
}

// Core dimensions to show in compact view
const coreDimensions: DimensionCode[] = [
  'quality',
  'profitability',
  'growth',
  'value',
  'momentum',
  'beneish_mscore',
]

// All dimensions for expanded view
const allDimensions: DimensionCode[] = [
  'profitability',
  'returns',
  'growth',
  'financial_health',
  'earnings_quality',
  'capital_allocation',
  'working_capital',
  'beneish_mscore',
  'value',
  'risk',
  'momentum',
  'quality',
  'valuation_percentile',
]

function getTierStars(tier: number) {
  return Array.from({ length: 5 - tier + 1 }, (_, i) => (
    <StarIcon key={i} className="w-3 h-3 text-yellow-400" />
  ))
}

export function CompanyCard({ pitch }: CompanyCardProps) {
  const stage = stageConfig[pitch.stage] || stageConfig.established

  return (
    <Link
      to={`/company/${pitch.symbol}`}
      className="block bg-card border border-border rounded-lg p-4 hover:border-blue-500 transition-colors"
    >
      {/* Header */}
      <div className="flex items-start justify-between mb-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="text-lg font-semibold text-foreground truncate">
              {pitch.companyName}
            </h3>
            {pitch.isActionable && (
              <SparklesIcon className="w-4 h-4 text-yellow-400 flex-shrink-0" title="Actionable" />
            )}
          </div>
          <div className="flex items-center gap-2 mt-1">
            <span className="text-sm font-mono text-muted-foreground">
              {pitch.symbol}
            </span>
            <span className={`text-xs px-2 py-0.5 rounded-full ${stage.bgColor} ${stage.color}`}>
              {stage.name}
            </span>
          </div>
        </div>

        {/* Score */}
        <div className="text-right flex-shrink-0">
          <div className="text-2xl font-bold text-foreground">
            {Math.round(pitch.fatPitchScore || 0)}
          </div>
          <div className="flex items-center justify-end gap-0.5 mt-1">
            {getTierStars(pitch.qualityTier)}
          </div>
        </div>
      </div>

      {/* Quick Stats Row */}
      <div className="flex items-center gap-3 mb-4 text-sm">
        <div className="flex items-center gap-1">
          <ChartBarIcon className="w-4 h-4 text-muted-foreground" />
          <span className="text-muted-foreground">Tier</span>
          <span className="font-medium text-foreground">{pitch.qualityTier}</span>
        </div>
        {pitch.cheapnessScore > 60 && (
          <span className="text-xs px-2 py-0.5 rounded-full bg-green-500/10 text-green-400">
            Cheap
          </span>
        )}
      </div>

      {/* Core Dimensions */}
      <div className="space-y-2">
        {coreDimensions.map((dim) => {
          const score = pitch.dimensionScores?.[dim]
          if (score === undefined || score === null) return null
          return (
            <DimensionBar key={dim} code={dim} score={score} />
          )
        })}
      </div>
    </Link>
  )
}

// Expanded version showing all dimensions
export function CompanyCardExpanded({ pitch }: CompanyCardProps) {
  const stage = stageConfig[pitch.stage] || stageConfig.established

  return (
    <Link
      to={`/company/${pitch.symbol}`}
      className="block bg-card border border-border rounded-lg p-6 hover:border-blue-500 transition-colors"
    >
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h3 className="text-xl font-semibold text-foreground truncate">
              {pitch.companyName}
            </h3>
            {pitch.isActionable && (
              <SparklesIcon className="w-5 h-5 text-yellow-400 flex-shrink-0" title="Actionable" />
            )}
          </div>
          <div className="flex items-center gap-2 mt-1">
            <span className="text-sm font-mono text-muted-foreground">
              {pitch.symbol}
            </span>
            <span className={`text-xs px-2 py-0.5 rounded-full ${stage.bgColor} ${stage.color}`}>
              {stage.name}
            </span>
            {pitch.cheapnessScore > 60 && (
              <span className="text-xs px-2 py-0.5 rounded-full bg-green-500/10 text-green-400">
                Cheap
              </span>
            )}
          </div>
        </div>

        {/* Score */}
        <div className="text-right flex-shrink-0">
          <div className="text-3xl font-bold text-foreground">
            {Math.round(pitch.fatPitchScore || 0)}
          </div>
          <div className="flex items-center justify-end gap-0.5 mt-1">
            {getTierStars(pitch.qualityTier)}
          </div>
          <div className="text-xs text-muted-foreground mt-1">
            Tier {pitch.qualityTier}
          </div>
        </div>
      </div>

      {/* All Dimensions - Two Column Grid */}
      <div className="grid grid-cols-2 gap-x-6 gap-y-3">
        {allDimensions.map((dim) => {
          const score = pitch.dimensionScores?.[dim]
          if (score === undefined || score === null) return null
          return (
            <DimensionBar key={dim} code={dim} score={score} />
          )
        })}
      </div>
    </Link>
  )
}
