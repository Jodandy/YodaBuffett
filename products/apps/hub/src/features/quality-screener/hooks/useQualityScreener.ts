/**
 * Quality Screener Hooks
 * React Query hooks for the quality screener
 */

import { useQuery } from '@tanstack/react-query';
import { fetchCategories, fetchCompanies, fetchCompany, fetchSummary } from '../api';
import type { ScreenerFilters } from '../types';

/**
 * Fetch available filter categories
 */
export function useCategories() {
  return useQuery({
    queryKey: ['quality-screener', 'categories'],
    queryFn: fetchCategories,
    staleTime: 1000 * 60 * 60, // 1 hour - categories don't change often
  });
}

/**
 * Fetch screened companies with filters
 */
export function useCompanies(
  filters?: ScreenerFilters,
  scoreDate?: string,
  limit?: number
) {
  return useQuery({
    queryKey: ['quality-screener', 'companies', filters, scoreDate, limit],
    queryFn: () => fetchCompanies(filters, scoreDate, limit),
    staleTime: 1000 * 60 * 5, // 5 minutes
  });
}

/**
 * Fetch a single company's quality analysis
 */
export function useCompany(ticker: string, scoreDate?: string) {
  return useQuery({
    queryKey: ['quality-screener', 'company', ticker, scoreDate],
    queryFn: () => fetchCompany(ticker, scoreDate),
    enabled: !!ticker,
    staleTime: 1000 * 60 * 5,
  });
}

/**
 * Fetch summary statistics
 */
export function useSummary(scoreDate?: string) {
  return useQuery({
    queryKey: ['quality-screener', 'summary', scoreDate],
    queryFn: () => fetchSummary(scoreDate),
    staleTime: 1000 * 60 * 5,
  });
}
