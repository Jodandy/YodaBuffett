/**
 * Screener Types
 * Types for Fat Pitch Machine data
 */

// Business lifecycle stages
export type BusinessStage =
  | 'early_stage'
  | 'growth_stage'
  | 'mature_yield'
  | 'compounder'
  | 'established'

// Quality tiers (1 = best)
export type QualityTier = 1 | 2 | 3 | 4 | 5

// The 14 dimensions
export type DimensionCode =
  | 'profitability'
  | 'returns'
  | 'growth'
  | 'financial_health'
  | 'earnings_quality'
  | 'capital_allocation'
  | 'working_capital'
  | 'beneish_mscore'
  | 'value'
  | 'risk'
  | 'momentum'
  | 'quality'
  | 'valuation_percentile'
  | 'sentiment'

// Dimension score from database
export interface DimensionScore {
  code: DimensionCode
  score: number // 0-100
  confidence: number // 0-1
}

// Fat Pitch result for a company
export interface FatPitch {
  companyId: string
  companyName: string
  symbol: string
  stage: BusinessStage
  stageConfidence: number
  qualityScore: number
  cheapnessScore: number
  fatPitchScore: number
  qualityTier: QualityTier
  dimensionScores: Record<string, number>
  dimensionContributions: Record<string, number>
  flags: string[]
  warnings: string[]
  isActionable: boolean
  pitchSummary: string
}

// Summary statistics per stage
export interface StageSummary {
  stage: BusinessStage
  count: number
  avgScore: number
  tier1Count: number
  tier2Count: number
  tier3Count: number
  actionableCount: number
}

// API response for /fat-pitch/pitches
export interface FatPitchResponse {
  pitches: FatPitch[]
  totalCount: number
  generatedAt: string
}

// API response for /fat-pitch/summary
export interface FatPitchSummary {
  stages: StageSummary[]
  totalCompanies: number
  totalActionable: number
}

// Filter options for the screener
export interface ScreenerFilters {
  stage?: BusinessStage | 'all'
  minScore?: number
  maxTier?: QualityTier
  actionableOnly?: boolean
  searchQuery?: string
}

// Sort options
export type SortDirection = 'asc' | 'desc'

// Weight profile for scoring
export interface WeightProfile {
  name: string
  description: string
  weights: Record<string, number>
  isDefault: boolean
}

// Weight profile list response
export interface WeightProfileListResponse {
  profiles: WeightProfile[]
  defaultProfile: string
}
