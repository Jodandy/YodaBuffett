/**
 * Portfolio API
 * API calls for portfolio and holdings management
 */

import { api, toCamelCase, toSnakeCase } from '../../../services/api'
import type {
  Portfolio,
  Holding,
  CreatePortfolioRequest,
  AddHoldingRequest,
} from '../types/portfolio'

// API response types (snake_case from backend)
interface PortfolioListResponse {
  id: string
  name: string
  description: string | null
  currency: string
  created_at: string
  holdings_count: number
  total_value: number | null
  total_gain_loss: number | null
  total_gain_loss_percent: number | null
}

export interface StockSearchResult {
  id: string
  symbol: string
  name: string
  sector: string | null
  country: string | null
}

export interface PortfolioListItem {
  id: string
  name: string
  description?: string
  currency: string
  createdAt: string
  holdingsCount: number
  totalValue?: number
  totalGainLoss?: number
  totalGainLossPercent?: number
}

// API functions
export async function fetchPortfolios(): Promise<PortfolioListItem[]> {
  const { data } = await api.get<PortfolioListResponse[]>('/portfolios')
  return data.map((p) => toCamelCase<PortfolioListItem>(p))
}

export async function fetchPortfolio(id: string): Promise<Portfolio> {
  const { data } = await api.get(`/portfolios/${id}`)
  return toCamelCase<Portfolio>(data)
}

export async function createPortfolio(
  request: CreatePortfolioRequest
): Promise<Portfolio> {
  const { data } = await api.post('/portfolios', toSnakeCase(request))
  return toCamelCase<Portfolio>(data)
}

export async function updatePortfolio(
  id: string,
  updates: Partial<CreatePortfolioRequest>
): Promise<Portfolio> {
  const { data } = await api.patch(`/portfolios/${id}`, toSnakeCase(updates))
  return toCamelCase<Portfolio>(data)
}

export async function deletePortfolio(id: string): Promise<void> {
  await api.delete(`/portfolios/${id}`)
}

export async function addHolding(
  portfolioId: string,
  request: AddHoldingRequest
): Promise<Holding> {
  const { data } = await api.post(
    `/portfolios/${portfolioId}/holdings`,
    toSnakeCase(request)
  )
  return toCamelCase<Holding>(data)
}

export async function updateHolding(
  holdingId: string,
  updates: Partial<AddHoldingRequest>
): Promise<Holding> {
  const { data } = await api.patch(`/portfolios/holdings/${holdingId}`, toSnakeCase(updates))
  return toCamelCase<Holding>(data)
}

export async function deleteHolding(holdingId: string): Promise<void> {
  await api.delete(`/portfolios/holdings/${holdingId}`)
}

export async function searchStocks(query: string): Promise<StockSearchResult[]> {
  if (!query || query.length < 1) return []
  const { data } = await api.get<StockSearchResult[]>('/portfolios/stocks/search', {
    params: { q: query, limit: 10 },
  })
  return data
}
