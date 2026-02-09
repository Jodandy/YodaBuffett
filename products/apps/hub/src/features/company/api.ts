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
} from './types'

/**
 * Fetch company detail from Fat Pitch endpoint
 * Note: Fat Pitch uses company_id (UUID), so we first need to resolve symbol → id
 */
export async function fetchCompanyBySymbol(symbol: string): Promise<CompanyDetail | null> {
  try {
    // Fetch pitches in batches (API has 500 limit) and find by symbol
    // TODO: Add a dedicated /fat-pitch/pitches/symbol/{symbol} endpoint for efficiency
    let allPitches: CompanyDetail[] = []
    let offset = 0
    const batchSize = 500

    // Fetch up to 3 batches (1500 companies)
    for (let i = 0; i < 3; i++) {
      const response = await api.get('/fat-pitch/pitches', {
        params: { limit: batchSize, offset }
      })
      const pitches = toCamelCase<CompanyDetail[]>(response.data)
      allPitches = [...allPitches, ...pitches]

      // Check if we found the company
      const company = pitches.find(p => p.symbol === symbol)
      if (company) return company

      // If we got fewer than batchSize, we've fetched all
      if (pitches.length < batchSize) break

      offset += batchSize
    }

    // Final search through all fetched pitches
    return allPitches.find(p => p.symbol === symbol) || null
  } catch (error) {
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
 */
export async function searchCompanies(query: string): Promise<CompanyDetail[]> {
  try {
    const response = await api.get('/fat-pitch/pitches', {
      params: { limit: 500 }
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
