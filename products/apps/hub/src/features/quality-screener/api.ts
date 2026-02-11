/**
 * Quality Screener API Client
 * Standalone - no Fat Pitch dependencies
 */

import type {
  ScreenerResponse,
  CategoryOptions,
  QualityCandidate,
  ScreenerFilters,
} from './types';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v1';

/**
 * Fetch available filter categories
 */
export async function fetchCategories(): Promise<CategoryOptions> {
  const response = await fetch(`${API_BASE}/quality-screener/categories`);
  if (!response.ok) {
    throw new Error('Failed to fetch categories');
  }
  return response.json();
}

/**
 * Fetch screened companies with optional filters
 */
export async function fetchCompanies(
  filters?: ScreenerFilters,
  scoreDate?: string,
  limit?: number
): Promise<ScreenerResponse> {
  const params = new URLSearchParams();

  if (scoreDate) {
    params.append('score_date', scoreDate);
  }

  if (limit) {
    params.append('limit', limit.toString());
  }

  if (filters?.tiers) {
    filters.tiers.forEach((t) => params.append('tier', t.toString()));
  }

  if (filters?.business_models) {
    filters.business_models.forEach((m) => params.append('model', m));
  }

  if (filters?.size_categories) {
    filters.size_categories.forEach((s) => params.append('size', s));
  }

  if (filters?.cash_qualities) {
    filters.cash_qualities.forEach((c) => params.append('cash', c));
  }

  if (filters?.profitable_only) {
    params.append('profitable', 'true');
  }

  if (filters?.min_market_cap) {
    params.append('min_cap', filters.min_market_cap.toString());
  }

  if (filters?.max_market_cap) {
    params.append('max_cap', filters.max_market_cap.toString());
  }

  const url = `${API_BASE}/quality-screener/companies?${params.toString()}`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error('Failed to fetch companies');
  }

  return response.json();
}

/**
 * Fetch a single company's quality analysis
 */
export async function fetchCompany(
  ticker: string,
  scoreDate?: string
): Promise<QualityCandidate> {
  const params = new URLSearchParams();
  if (scoreDate) {
    params.append('score_date', scoreDate);
  }

  const url = `${API_BASE}/quality-screener/companies/${ticker}?${params.toString()}`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Failed to fetch company ${ticker}`);
  }

  return response.json();
}

/**
 * Fetch summary statistics (no filters)
 */
export async function fetchSummary(scoreDate?: string): Promise<ScreenerResponse['summary']> {
  const params = new URLSearchParams();
  if (scoreDate) {
    params.append('score_date', scoreDate);
  }

  const url = `${API_BASE}/quality-screener/summary?${params.toString()}`;
  const response = await fetch(url);

  if (!response.ok) {
    throw new Error('Failed to fetch summary');
  }

  return response.json();
}
