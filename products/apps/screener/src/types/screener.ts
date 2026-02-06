// Re-export shared types from @yodabuffett/types
export type {
  Company,
  ApiResponse,
  ApiError,
  ExportRequest,
  ExportResponse,
  ColumnSort,
  TableFilter,
  PaginationOptions,
} from '@yodabuffett/types'

// Screener-specific types

export interface MetricDefinition {
  id: string
  name: string
  description: string
  category: string // fundamental, technical, derived, market
  data_type: string // number, percentage, ratio, currency
  unit?: string
  is_relative: boolean
  source_type: string // database, calculated
}

export interface QueryCondition {
  id: string
  leftOperand: string // metric ID
  operator: string // >, <, >=, <=, =, !=, between
  rightOperand: string | number | Array<string | number>
  isRelative: boolean // true for metric vs metric comparisons
}

export interface QueryGroup {
  id: string
  conditions: QueryCondition[]
  logicalOperator: 'AND' | 'OR'
}

export interface ScreenerQuery {
  id?: string
  name?: string
  description?: string
  groups: QueryGroup[]
  groupLogic: 'AND' | 'OR'
  asOfDate?: string // ISO date string for point-in-time screening
  columns: string[] // metric IDs to display
  includeForwardReturns?: string[] // ['1W', '1M', '3M', '6M', '1Y', '2Y']
}

import type { Company } from '@yodabuffett/types'

export interface ScreenerResult {
  company: Company
  values: Record<string, string | number | null>
  forwardReturns?: Record<string, number>
  rank?: number
}

export interface ResultSummary {
  count: number
  averages: Record<string, number>
  medians: Record<string, number>
  winRates?: Record<string, number>
  sharpeRatios?: Record<string, number>
}

export interface ScreenerResponse {
  query: ScreenerQuery
  results: ScreenerResult[]
  summary: ResultSummary
  executionTime: number
  asOfDate: string
  totalMatches: number
}

// Backtest types
export interface BacktestRequest {
  query: ScreenerQuery
  startDate: string // ISO date string
  endDate: string
  frequency: 'daily' | 'weekly' | 'monthly'
  forwardPeriods: string[]
}

export interface BacktestResult {
  date: string
  matches: number
  avgReturn: Record<string, number>
  winRate: Record<string, number>
  sharpeRatio: Record<string, number>
  topPerformers: ScreenerResult[]
}

export interface BacktestSummary {
  totalSignals: number
  avgReturns: Record<string, number>
  winRates: Record<string, number>
  sharpeRatios: Record<string, number>
  bestMonth: Record<string, string | number>
  worstMonth: Record<string, string | number>
  maxDrawdown: number
}

export interface BacktestResponse {
  query: ScreenerQuery
  results: BacktestResult[]
  summary: BacktestSummary
  totalExecutionTime: number
}

// Saved query types
export interface SavedQuery {
  id: string
  name: string
  description?: string
  query: ScreenerQuery
  createdAt: string
  updatedAt: string
  isPublic: boolean
  tags: string[]
}

export interface SaveQueryRequest {
  name: string
  description?: string
  query: ScreenerQuery
  isPublic: boolean
  tags: string[]
}

// UI-specific types
export interface QueryBuilderState {
  query: ScreenerQuery
  availableMetrics: MetricDefinition[]
  isLoading: boolean
  errors: string[]
}

export interface ScreenerPageState {
  query: ScreenerQuery
  results: ScreenerResponse | null
  isExecuting: boolean
  lastExecutionTime?: number
}

export interface BacktestPageState {
  request: BacktestRequest
  results: BacktestResponse | null
  isExecuting: boolean
  progress?: number
}
