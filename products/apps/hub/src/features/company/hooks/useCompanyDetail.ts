/**
 * Company Detail Hooks
 * React Query hooks for fetching company data
 */

import { useQuery } from '@tanstack/react-query'
import {
  fetchCompanyBySymbol,
  fetchCompanyById,
  fetchNordicCompany,
  fetchPriceHistory,
  fetchFinancials,
  fetchCalendarEvents,
  fetchDocuments,
} from '../api'
import type { PriceTimeRange } from '../types'

// Query keys
export const companyKeys = {
  all: ['company'] as const,
  detail: (symbol: string) => [...companyKeys.all, 'detail', symbol] as const,
  byId: (id: string) => [...companyKeys.all, 'byId', id] as const,
  nordic: (id: string) => [...companyKeys.all, 'nordic', id] as const,
  prices: (symbol: string, range: PriceTimeRange) => [...companyKeys.all, 'prices', symbol, range] as const,
  financials: (symbol: string) => [...companyKeys.all, 'financials', symbol] as const,
  events: (symbol: string) => [...companyKeys.all, 'events', symbol] as const,
  documents: (symbol: string) => [...companyKeys.all, 'documents', symbol] as const,
}

// Map time range to days
const rangeToDays: Record<PriceTimeRange, number> = {
  '1M': 30,
  '3M': 90,
  '6M': 180,
  '1Y': 365,
  '3Y': 1095,
  '5Y': 1825,
  'MAX': 7300, // ~20 years
}

/**
 * Fetch company detail by symbol
 */
export function useCompanyDetail(symbol: string) {
  return useQuery({
    queryKey: companyKeys.detail(symbol),
    queryFn: () => fetchCompanyBySymbol(symbol),
    enabled: !!symbol,
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}

/**
 * Fetch company detail by ID
 */
export function useCompanyById(companyId: string) {
  return useQuery({
    queryKey: companyKeys.byId(companyId),
    queryFn: () => fetchCompanyById(companyId),
    enabled: !!companyId,
    staleTime: 5 * 60 * 1000,
  })
}

/**
 * Fetch Nordic company info
 */
export function useNordicCompany(companyId: string | undefined) {
  return useQuery({
    queryKey: companyKeys.nordic(companyId || ''),
    queryFn: () => fetchNordicCompany(companyId!),
    enabled: !!companyId,
    staleTime: 10 * 60 * 1000, // 10 minutes
  })
}

/**
 * Fetch price history
 */
export function usePriceHistory(symbol: string, range: PriceTimeRange = '1Y') {
  return useQuery({
    queryKey: companyKeys.prices(symbol, range),
    queryFn: () => fetchPriceHistory(symbol, rangeToDays[range]),
    enabled: !!symbol,
    staleTime: 5 * 60 * 1000,
  })
}

/**
 * Fetch financial data by symbol
 */
export function useFinancials(symbol: string | undefined) {
  return useQuery({
    queryKey: companyKeys.financials(symbol || ''),
    queryFn: () => fetchFinancials(symbol!),
    enabled: !!symbol,
    staleTime: 10 * 60 * 1000,
  })
}

/**
 * Fetch calendar events by symbol
 */
export function useCalendarEvents(symbol: string | undefined) {
  return useQuery({
    queryKey: companyKeys.events(symbol || ''),
    queryFn: () => fetchCalendarEvents(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60 * 1000,
  })
}

/**
 * Fetch company documents by symbol
 */
export function useDocuments(symbol: string | undefined) {
  return useQuery({
    queryKey: companyKeys.documents(symbol || ''),
    queryFn: () => fetchDocuments(symbol!),
    enabled: !!symbol,
    staleTime: 5 * 60 * 1000,
  })
}
