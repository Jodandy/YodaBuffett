/**
 * Quality Screener Types
 * Standalone - no Fat Pitch dependencies
 */

export interface QualityCandidate {
  ticker: string;
  company_name: string;
  tier: number;
  tier_description: string;
  business_model: string;
  business_model_reason: string;
  size_category: string;
  cash_quality: string;
  is_profitable: boolean;
  quality_score: number;
  market_cap: number;
  gross_margin: number | null;
  operating_margin: number | null;
  net_margin: number | null;
  roic: number | null;
  roe: number | null;
  revenue_cagr: number | null;
  ocf_to_ni: number | null;
  fcf_to_ni: number | null;
  fcf_yield: number | null;
  capex_to_revenue: number | null;
  capex_to_da: number | null;
  negative_working_capital: boolean;
  receivables_vs_revenue: number | null;
  gross_margin_trend: number | null;
  share_dilution: number | null;
  net_cash: boolean;
  net_debt_to_ebitda: number | null;
  pe_ratio: number | null;
  ev_to_ebitda: number | null;
  reasons: string[];
  concerns: string[];
}

export interface ScreenerSummary {
  total_companies: number;
  by_tier: Record<string, number>;
  by_business_model: Record<string, number>;
  by_size: Record<string, number>;
  by_cash_quality: Record<string, number>;
  profitable_count: number;
  unprofitable_count: number;
}

export interface CategoryOption {
  value: string;
  label: string;
  description: string;
}

export interface CategoryOptions {
  tiers: CategoryOption[];
  business_models: CategoryOption[];
  size_categories: CategoryOption[];
  cash_qualities: CategoryOption[];
}

export interface ScreenerFilters {
  tiers?: number[];
  business_models?: string[];
  size_categories?: string[];
  cash_qualities?: string[];
  profitable_only?: boolean;
  min_market_cap?: number;
  max_market_cap?: number;
}

export interface ScreenerResponse {
  candidates: QualityCandidate[];
  summary: ScreenerSummary;
  score_date: string;
  filters_applied: Record<string, unknown>;
}

// Business model colors
export const BUSINESS_MODEL_COLORS: Record<string, string> = {
  'Cash Cow': 'bg-green-100 text-green-800 border-green-200',
  'Compounder': 'bg-blue-100 text-blue-800 border-blue-200',
  'Caution': 'bg-yellow-100 text-yellow-800 border-yellow-200',
  'Red Flag': 'bg-red-100 text-red-800 border-red-200',
  'Unclear': 'bg-gray-100 text-gray-800 border-gray-200',
  'Unknown': 'bg-gray-100 text-gray-600 border-gray-200',
};

// Size category colors
export const SIZE_COLORS: Record<string, string> = {
  'Micro': 'bg-purple-100 text-purple-800',
  'Small': 'bg-indigo-100 text-indigo-800',
  'Mid': 'bg-cyan-100 text-cyan-800',
  'Large': 'bg-teal-100 text-teal-800',
  'Mega': 'bg-emerald-100 text-emerald-800',
};

// Cash quality colors
export const CASH_QUALITY_COLORS: Record<string, string> = {
  'Excellent': 'text-green-600',
  'Good': 'text-green-500',
  'Moderate': 'text-yellow-600',
  'Weak': 'text-orange-500',
  'Poor': 'text-red-500',
  'Unknown': 'text-gray-400',
};

// Tier colors
export const TIER_COLORS: Record<number, string> = {
  1: 'bg-green-500',
  2: 'bg-green-400',
  3: 'bg-yellow-400',
  4: 'bg-orange-400',
  5: 'bg-red-400',
};
