/**
 * Company Detail Types
 * Types for the company detail page data
 */

// Business lifecycle stages (from Fat Pitch)
export type BusinessStage =
  | 'early_stage'
  | 'growth_stage'
  | 'mature_yield'
  | 'compounder'
  | 'established'

// Quality tiers (1 = best)
export type QualityTier = 1 | 2 | 3 | 4 | 5

// Dimension codes for scoring
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

// Company detail from Fat Pitch endpoint
export interface CompanyDetail {
  companyId: string
  symbol: string
  companyName: string
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

// Nordic company info
export interface NordicCompany {
  id: string
  name: string
  ticker: string
  yahooSymbol?: string
  exchange: string
  country: string
  marketCapCategory?: string
  sector?: string
  irEmail?: string
  irWebsite?: string
  website?: string
  reportingLanguage?: string
}

// Price data point for charts
export interface PriceDataPoint {
  date: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  adjustedClose?: number
}

// Price history response
export interface PriceHistory {
  symbol: string
  companyName?: string
  prices: PriceDataPoint[]
  latestPrice?: number
  priceChange?: number
  priceChangePercent?: number
}

// Financial statement line item
export interface FinancialLineItem {
  label: string
  value: number | null
  previousValue?: number | null
  yoyChange?: number | null
}

// Income statement
export interface IncomeStatement {
  periodDate: string
  statementType: string
  fiscalYear?: number | null
  fiscalQuarter?: number | null
  totalRevenue: number | null
  grossProfit: number | null
  operatingIncome: number | null
  netIncome: number | null
  ebit: number | null
  ebitda: number | null
  basicEps: number | null
  dilutedEps: number | null
  researchDevelopment: number | null
  sellingGeneralAdministrative: number | null
  interestExpense: number | null
  taxExpense: number | null
  currency: string | null
}

// Balance sheet
export interface BalanceSheet {
  periodDate: string
  statementType: string
  totalAssets: number | null
  currentAssets: number | null
  cashAndEquivalents: number | null
  accountsReceivable: number | null
  inventory: number | null
  totalLiabilities: number | null
  currentLiabilities: number | null
  totalDebt: number | null
  longTermDebt: number | null
  accountsPayable: number | null
  totalEquity: number | null
  retainedEarnings: number | null
  sharesOutstanding: number | null
  currency: string | null
}

// Cash flow statement
export interface CashFlowStatement {
  periodDate: string
  statementType: string
  operatingCashFlow: number | null
  netIncome: number | null
  depreciationAmortization: number | null
  investingCashFlow: number | null
  capitalExpenditure: number | null
  financingCashFlow: number | null
  dividendsPaid: number | null
  freeCashFlow: number | null
  currency: string | null
}

// Financial data bundle
export interface FinancialData {
  incomeStatements: IncomeStatement[]
  balanceSheets: BalanceSheet[]
  cashFlowStatements: CashFlowStatement[]
}

// Calendar event
export interface CalendarEvent {
  id: string
  eventType: string
  eventDate: string
  eventTime?: string
  title?: string
  description?: string
  confirmed: boolean
  webcastUrl?: string
  sourceUrl?: string
  // Dividend-specific fields
  dividendAmount?: number
  dividendCurrency?: string
  exDividendDate?: string
  paymentDate?: string
}

// Calendar events response
export interface CalendarEventsResponse {
  symbol: string
  companyName?: string
  events: CalendarEvent[]
  totalCount: number
  upcomingCount: number
}

// Document reference
export interface CompanyDocument {
  id: string
  documentType: string
  reportPeriod?: string
  title?: string
  publishDate?: string
  language?: string
  sourceUrl?: string
  hasLocalFile: boolean
  hasExtractedText: boolean
  pageCount?: number
  fileSizeMb?: number
}

// Documents response
export interface DocumentsResponse {
  symbol: string
  companyName?: string
  documents: CompanyDocument[]
  totalCount: number
  downloadedCount: number
  extractedCount: number
}

// Tab options for the detail page
export type CompanyTab = 'overview' | 'financials' | 'documents' | 'events'

// Time range options for price chart
export type PriceTimeRange = '1M' | '3M' | '6M' | '1Y' | '3Y' | '5Y' | 'MAX'

// Dimension detail with full metadata for deep dive
export interface DimensionDetail {
  dimensionCode: string
  score: number | null
  confidence: number | null
  dataQuality: number | null
  scoreLow: number | null
  scoreHigh: number | null
  metadata: DimensionMetadata
}

// Metadata structure varies by dimension
export interface DimensionMetadata {
  // Component scores (common for many dimensions)
  componentScores?: {
    raw?: number
    peer?: number
    trend?: number
    stability?: number
  }
  // Metrics breakdown (e.g., for value dimension)
  metrics?: Record<string, MetricDetail>
  // Context/interpretation
  [key: string]: unknown
}

// Individual metric detail
export interface MetricDetail {
  current?: number
  rawScore?: number
  sectorPercentile?: number
  historicalPercentile?: number
  minHistorical?: number
  maxHistorical?: number
  trend?: string
}

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

// Historical score point
export interface HistoricalScorePoint {
  scoreDate: string
  score: number
  dimensionCode?: string
}

// Historical scores response
export interface HistoricalScoresResponse {
  companyId: string
  symbol: string
  companyName: string
  fatPitchScores: HistoricalScorePoint[]
  dimensionScores: Record<string, HistoricalScorePoint[]>
}

// Anomaly data point
export interface AnomalyPoint {
  date: string
  anomalyScore: number  // 0-100, higher = more anomalous
  sectionType?: string
  similarityToPrior?: number
  year?: number
}

// Anomaly response
export interface AnomalyResponse {
  companyId: string
  symbol: string
  companyName: string
  anomalies: AnomalyPoint[]
  avgAnomalyScore: number
  maxAnomalyScore: number
  anomalyCount: number
}
