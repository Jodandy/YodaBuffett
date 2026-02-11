/**
 * Company API
 * API functions for fetching company data
 */

import { api, toCamelCase } from '@/services/api'
import type {
  CompanyDetail,
  NordicCompany,
  PriceHistory,
  FinancialData,
  CalendarEventsResponse,
  DocumentsResponse,
  DimensionDetail,
  WeightProfileListResponse,
  HistoricalScoresResponse,
  AnomalyResponse,
} from './types'

/**
 * Fetch company detail from Fat Pitch endpoint by symbol
 * Uses the dedicated symbol lookup endpoint for efficiency
 */
export async function fetchCompanyBySymbol(symbol: string): Promise<CompanyDetail | null> {
  try {
    const response = await api.get(`/fat-pitch/pitches/symbol/${encodeURIComponent(symbol)}`)
    return toCamelCase<CompanyDetail>(response.data)
  } catch (error) {
    // 404 means company not found or has insufficient data - this is expected for some companies
    if ((error as { response?: { status?: number } })?.response?.status === 404) {
      console.warn(`Company ${symbol} not found in Fat Pitch system`)
      return null
    }
    console.error('Failed to fetch company by symbol:', error)
    return null
  }
}

/**
 * Fetch company detail by company ID
 */
export async function fetchCompanyById(companyId: string): Promise<CompanyDetail> {
  const response = await api.get(`/fat-pitch/pitches/company/${companyId}`)
  return toCamelCase<CompanyDetail>(response.data)
}

/**
 * Fetch Nordic company info
 */
export async function fetchNordicCompany(companyId: string): Promise<NordicCompany | null> {
  try {
    const response = await api.get(`/nordic/companies/${companyId}`)
    return toCamelCase<NordicCompany>(response.data)
  } catch (error) {
    console.error('Failed to fetch Nordic company info:', error)
    return null
  }
}

/**
 * Fetch price history for a symbol
 */
export async function fetchPriceHistory(
  symbol: string,
  days: number = 365
): Promise<PriceHistory | null> {
  try {
    const response = await api.get(`/market-data/prices/${symbol}`, {
      params: { days }
    })
    return toCamelCase<PriceHistory>(response.data)
  } catch (error) {
    console.warn('Failed to fetch price history:', error)
    return null
  }
}

/**
 * Fetch financial statements for a symbol
 */
export async function fetchFinancials(symbol: string): Promise<FinancialData | null> {
  try {
    const response = await api.get(`/market-data/financials/${symbol}`)
    return toCamelCase<FinancialData>(response.data)
  } catch (error) {
    console.warn('Failed to fetch financials:', error)
    return null
  }
}

/**
 * Fetch calendar events for a symbol
 */
export async function fetchCalendarEvents(symbol: string): Promise<CalendarEventsResponse | null> {
  try {
    const response = await api.get(`/market-data/events/${symbol}`, {
      params: { limit: 50 }
    })
    return toCamelCase<CalendarEventsResponse>(response.data)
  } catch (error) {
    console.warn('Failed to fetch calendar events:', error)
    return null
  }
}

/**
 * Fetch documents for a symbol
 */
export async function fetchDocuments(symbol: string): Promise<DocumentsResponse | null> {
  try {
    const response = await api.get(`/market-data/documents/${symbol}`, {
      params: { limit: 50 }
    })
    return toCamelCase<DocumentsResponse>(response.data)
  } catch (error) {
    console.warn('Failed to fetch documents:', error)
    return null
  }
}

/**
 * Search companies by name or symbol
 * Note: Fetches all pitches (up to 2000) to ensure complete search coverage
 */
export async function searchCompanies(query: string): Promise<CompanyDetail[]> {
  try {
    // Fetch all pitches to ensure we can find any company by name/symbol
    // This is cached by React Query so subsequent searches are fast
    const response = await api.get('/fat-pitch/pitches', {
      params: { limit: 2000 }
    })
    const pitches = toCamelCase<CompanyDetail[]>(response.data)
    const q = query.toLowerCase()
    return pitches.filter(
      p => p.symbol?.toLowerCase().includes(q) ||
           p.companyName?.toLowerCase().includes(q)
    ).slice(0, 20)
  } catch (error) {
    console.error('Failed to search companies:', error)
    return []
  }
}

/**
 * Fetch dimension details with full metadata for a company
 */
export async function fetchDimensionDetails(companyId: string): Promise<DimensionDetail[]> {
  try {
    const response = await api.get(`/fat-pitch/dimensions/${companyId}`)
    return toCamelCase<DimensionDetail[]>(response.data)
  } catch (error) {
    console.warn('Failed to fetch dimension details:', error)
    return []
  }
}

/**
 * Fetch available weight profiles
 */
export async function fetchWeightProfiles(): Promise<WeightProfileListResponse> {
  try {
    const response = await api.get('/fat-pitch/weight-profiles')
    return toCamelCase<WeightProfileListResponse>(response.data)
  } catch (error) {
    console.warn('Failed to fetch weight profiles:', error)
    // Return default fallback
    return {
      profiles: [
        { name: 'optimal', description: 'Best predictor from backtesting', weights: {}, isDefault: true }
      ],
      defaultProfile: 'optimal'
    }
  }
}

/**
 * Fetch company by symbol with specific weight profile
 */
export async function fetchCompanyWithProfile(
  symbol: string,
  weightProfile?: string
): Promise<CompanyDetail | null> {
  try {
    const params = weightProfile ? { weight_profile: weightProfile } : {}
    const response = await api.get(`/fat-pitch/pitches/symbol/${encodeURIComponent(symbol)}`, { params })
    return toCamelCase<CompanyDetail>(response.data)
  } catch (error) {
    if ((error as { response?: { status?: number } })?.response?.status === 404) {
      console.warn(`Company ${symbol} not found in Fat Pitch system`)
      return null
    }
    console.error('Failed to fetch company with profile:', error)
    return null
  }
}

/**
 * Fetch historical scores for a company
 */
export async function fetchHistoricalScores(
  symbol: string,
  weightProfile?: string
): Promise<HistoricalScoresResponse | null> {
  try {
    const params = weightProfile ? { weight_profile: weightProfile } : {}
    const response = await api.get(`/fat-pitch/history/${encodeURIComponent(symbol)}`, { params })
    return toCamelCase<HistoricalScoresResponse>(response.data)
  } catch (error) {
    console.warn('Failed to fetch historical scores:', error)
    return null
  }
}

/**
 * Fetch temporal anomaly data for a company
 *
 * Anomalies measure how different a company's documents are from their historical patterns.
 * Higher anomaly score = more different from prior year's communication.
 *
 * Research shows:
 * - High anomalies (>=40) correlate with -3.63% avg 60d return
 * - Low anomalies (<40) correlate with +2.26% avg 60d return
 */
export async function fetchAnomalies(
  symbol: string,
  minYear: number = 2018
): Promise<AnomalyResponse | null> {
  try {
    const response = await api.get(`/fat-pitch/anomalies/${encodeURIComponent(symbol)}`, {
      params: { min_year: minYear }
    })
    return toCamelCase<AnomalyResponse>(response.data)
  } catch (error) {
    console.warn('Failed to fetch anomalies:', error)
    return null
  }
}
