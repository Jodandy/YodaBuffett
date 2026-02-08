export interface Portfolio {
  id: string
  name: string
  description?: string
  currency: string
  isActive: boolean
  isDefault: boolean
  createdAt: string
  updatedAt: string
  holdings: Holding[]
  // Computed fields from API
  totalValue?: number
  totalCost?: number
  totalGainLoss?: number
  totalGainLossPercent?: number
}

export interface Holding {
  id: string
  portfolioId: string
  companyId?: string
  symbol: string
  companyName?: string
  quantity: number
  purchasePrice: number
  purchaseDate: string
  currency: string
  notes?: string
  createdAt: string
  updatedAt: string
  // Computed fields from API
  currentPrice?: number
  currentValue?: number
  costBasis?: number
  gainLoss?: number
  gainLossPercent?: number
}

export interface CreatePortfolioRequest {
  name: string
  description?: string
  currency?: string
}

export interface AddHoldingRequest {
  companyId?: string
  symbol: string
  companyName?: string
  quantity: number
  purchasePrice: number
  purchaseDate: string
  currency: string
  notes?: string
}
