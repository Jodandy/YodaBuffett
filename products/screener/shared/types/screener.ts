// Shared TypeScript types for YodaBuffett Screener
// Used by both frontend and backend (via code generation)

export interface Company {
  id: string;
  symbol: string;
  name: string;
  market: string;
  sector?: string;
  industry?: string;
  market_cap?: number;
  currency: string;
}

export interface MetricDefinition {
  id: string;
  name: string;
  description: string;
  category: 'fundamental' | 'technical' | 'derived';
  dataType: 'number' | 'percentage' | 'ratio' | 'currency';
  unit?: string;
  isRelative?: boolean; // Can be compared to other metrics
}

export interface QueryCondition {
  id: string;
  leftOperand: string;  // metric ID or another metric ID for relative comparisons
  operator: ComparisonOperator;
  rightOperand: string | number; // value or metric ID for relative comparisons
  isRelative: boolean; // true for X > Y comparisons
}

export interface QueryGroup {
  id: string;
  conditions: QueryCondition[];
  logicalOperator: 'AND' | 'OR';
}

export interface ScreenerQuery {
  id?: string;
  name?: string;
  description?: string;
  groups: QueryGroup[];
  groupLogic: 'AND' | 'OR'; // How to combine groups
  asOfDate?: string; // For point-in-time screening
  columns: string[]; // Metric IDs to display
  includeForwardReturns?: ForwardReturnPeriod[];
}

export interface ScreenerResult {
  company: Company;
  values: Record<string, number | string | null>;
  forwardReturns?: Record<string, number>;
  rank?: number;
}

export interface ScreenerResponse {
  query: ScreenerQuery;
  results: ScreenerResult[];
  summary: ResultSummary;
  executionTime: number;
  asOfDate: string;
  totalMatches: number;
}

export interface ResultSummary {
  count: number;
  averages: Record<string, number>;
  medians: Record<string, number>;
  winRates?: Record<string, number>; // For forward returns
  sharpeRatios?: Record<string, number>; // For forward returns
}

export interface BacktestRequest {
  query: Omit<ScreenerQuery, 'asOfDate'>;
  startDate: string;
  endDate: string;
  frequency: 'daily' | 'weekly' | 'monthly';
  forwardPeriods: ForwardReturnPeriod[];
}

export interface BacktestResult {
  date: string;
  matches: number;
  avgReturn: Record<string, number>; // keyed by forward period
  winRate: Record<string, number>;
  sharpeRatio: Record<string, number>;
  topPerformers: ScreenerResult[];
}

export interface BacktestResponse {
  query: ScreenerQuery;
  results: BacktestResult[];
  summary: BacktestSummary;
  totalExecutionTime: number;
}

export interface BacktestSummary {
  totalSignals: number;
  avgReturns: Record<string, number>;
  winRates: Record<string, number>;
  sharpeRatios: Record<string, number>;
  bestMonth: { date: string; return: number };
  worstMonth: { date: string; return: number };
  maxDrawdown: number;
}

export type ComparisonOperator = 
  | '>' | '>=' | '<' | '<=' | '=' | '!='
  | 'between' | 'in' | 'not_in';

export type ForwardReturnPeriod = '1W' | '1M' | '3M' | '6M' | '1Y' | '2Y';

export interface SavedQuery {
  id: string;
  name: string;
  description?: string;
  query: ScreenerQuery;
  createdAt: string;
  updatedAt: string;
  isPublic: boolean;
  tags: string[];
}

// API Response types
export interface ApiResponse<T> {
  data: T;
  success: boolean;
  message?: string;
  errors?: string[];
}

export interface ErrorResponse {
  success: false;
  message: string;
  errors: string[];
  code?: string;
}

// Pagination
export interface PaginationParams {
  page: number;
  limit: number;
}

export interface PaginatedResponse<T> {
  data: T[];
  pagination: {
    page: number;
    limit: number;
    total: number;
    totalPages: number;
    hasNext: boolean;
    hasPrev: boolean;
  };
}

// Export/Import types
export interface ExportRequest {
  format: 'csv' | 'xlsx' | 'json';
  includeMetadata: boolean;
}

export interface ExportResponse {
  downloadUrl: string;
  filename: string;
  expiresAt: string;
}