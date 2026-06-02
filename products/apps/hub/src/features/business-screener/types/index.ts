/**
 * Business Screener Types
 * Types for the Business Screener Deluxe - 15 investment screens
 */

// Screen types 1-16
export type ScreenType = 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 | 16

// Analysis tiers
export type Tier = 'A' | 'B' | 'C'

// Run frequencies
export type RunFrequency = 'daily' | 'weekly' | 'monthly' | 'quarterly' | 'annually'

// Screen definition metadata
export interface ScreenDefinition {
  screenType: ScreenType
  name: string
  shortName: string
  description: string
  tierAEnabled: boolean
  tierBEnabled: boolean
  tierCEnabled: boolean
  tiers: string // e.g., "A+B", "A", "B+C"
  runFrequency: RunFrequency
  isActive: boolean
}

// Screen result for a single company
export interface ScreenResult {
  id?: number
  companyId: string
  companyName: string
  primaryTicker: string
  screenType: ScreenType
  tier: Tier
  passed: boolean
  score: number // 0-100
  metrics: Record<string, unknown>
  flags: string[]
  requiresTierB: boolean
  requiresTierC: boolean
  isActive: boolean
  triggeredAt: string
  expiresAt?: string
}

// Multi-hit company (passes multiple screens)
export interface MultiHit {
  companyId: string
  companyName: string
  primaryTicker: string
  screens: {
    screenType: ScreenType
    screenName: string
    score: number
    flags: string[]
  }[]
  screenCount: number
  avgScore: number
  bestScreen: ScreenType
}

// Dashboard summary
export interface ScreenerDashboard {
  totalResults: number
  activeScreens: number
  byScreen: {
    screenType: ScreenType
    screenName: string
    resultCount: number
    avgScore: number
  }[]
  multiHitCount: number
  lastUpdated: string
}

// Filter options for the screener
export interface BusinessScreenerFilters {
  screenTypes?: ScreenType[]
  minScore?: number
  tier?: Tier | 'all'
  searchQuery?: string
  flagFilter?: string
  activeOnly?: boolean
}

// Sort options
export type SortField = 'score' | 'companyName' | 'screenType' | 'triggeredAt'
export type SortDirection = 'asc' | 'desc'

// Pre-defined screen names for display
export const SCREEN_NAMES: Record<ScreenType, { name: string; shortName: string; description: string }> = {
  1: { name: 'Net-Nets', shortName: 'Net-Nets', description: 'Below liquidation value (NCAV > market cap)' },
  2: { name: 'Defensive Bargains', shortName: 'Defensive', description: "Graham's multi-factor safety screen" },
  3: { name: 'Asset Plays', shortName: 'Asset Plays', description: 'Real assets below book value' },
  4: { name: 'Revenue Turnarounds', shortName: 'Turnarounds', description: 'Intact unit economics at death prices' },
  5: { name: 'Distressed Stable Earners', shortName: 'Distressed', description: 'Temporary margin compression' },
  6: { name: 'Growth at Reasonable Prices', shortName: 'GARP', description: 'Demonstrated growth, not hypothetical' },
  7: { name: 'Compressed Fundamentals', shortName: 'Compressed', description: 'Coiled spring - temporary earnings suppression' },
  8: { name: 'Special Situations', shortName: 'Special Sit', description: 'Event-driven with defined timelines' },
  9: { name: 'Holding Company Discounts', shortName: 'Holdings', description: 'Portfolios below sum of parts' },
  10: { name: 'Sum-of-Parts', shortName: 'SoTP', description: 'Hidden value in the footnotes' },
  11: { name: 'Cannibal Companies', shortName: 'Cannibals', description: 'Buyback compounders' },
  12: { name: 'Wonderful Business at Fair Price', shortName: 'Wonderful', description: "Munger's compounders" },
  13: { name: 'Crisis Bargains', shortName: 'Crisis', description: 'Legal or regulatory overhang' },
  14: { name: 'Cyclicals', shortName: 'Cyclicals', description: 'Inverted screen for cyclical companies' },
  15: { name: 'Stalwarts', shortName: 'Stalwarts', description: 'Blue chip dip buys' },
  16: { name: 'Industrial Asset Recovery', shortName: 'Asset Recovery', description: 'Asset-heavy industrials at liquidation valuations' },
}

// Screen category groups
export const SCREEN_CATEGORIES = {
  value: [1, 2, 3, 5, 13, 14, 16] as ScreenType[],
  growth: [4, 6, 12] as ScreenType[],
  special: [7, 8, 9, 10] as ScreenType[],
  quality: [11, 12, 15] as ScreenType[],
}

// Helper to get score color
export function getScoreColor(score: number): string {
  if (score >= 80) return 'text-green-400'
  if (score >= 60) return 'text-green-300'
  if (score >= 40) return 'text-yellow-400'
  if (score >= 20) return 'text-orange-400'
  return 'text-red-400'
}

// Helper to get score background
export function getScoreBgColor(score: number): string {
  if (score >= 80) return 'bg-green-500'
  if (score >= 60) return 'bg-green-400'
  if (score >= 40) return 'bg-yellow-400'
  if (score >= 20) return 'bg-orange-400'
  return 'bg-red-400'
}

// Screen type badge colors
export const SCREEN_COLORS: Record<ScreenType, { bg: string; text: string }> = {
  1: { bg: 'bg-blue-500/10', text: 'text-blue-400' },
  2: { bg: 'bg-green-500/10', text: 'text-green-400' },
  3: { bg: 'bg-purple-500/10', text: 'text-purple-400' },
  4: { bg: 'bg-orange-500/10', text: 'text-orange-400' },
  5: { bg: 'bg-red-500/10', text: 'text-red-400' },
  6: { bg: 'bg-teal-500/10', text: 'text-teal-400' },
  7: { bg: 'bg-indigo-500/10', text: 'text-indigo-400' },
  8: { bg: 'bg-pink-500/10', text: 'text-pink-400' },
  9: { bg: 'bg-amber-500/10', text: 'text-amber-400' },
  10: { bg: 'bg-cyan-500/10', text: 'text-cyan-400' },
  11: { bg: 'bg-lime-500/10', text: 'text-lime-400' },
  12: { bg: 'bg-emerald-500/10', text: 'text-emerald-400' },
  13: { bg: 'bg-rose-500/10', text: 'text-rose-400' },
  14: { bg: 'bg-violet-500/10', text: 'text-violet-400' },
  15: { bg: 'bg-sky-500/10', text: 'text-sky-400' },
  16: { bg: 'bg-stone-500/10', text: 'text-stone-400' },
}
