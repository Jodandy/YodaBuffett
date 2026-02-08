/**
 * Portfolio Hooks
 * React Query hooks for portfolio data fetching and mutations
 */

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchPortfolios,
  fetchPortfolio,
  createPortfolio,
  updatePortfolio,
  deletePortfolio,
  addHolding,
  updateHolding,
  deleteHolding,
  searchStocks,
} from '../api/portfolio-api'
import type { CreatePortfolioRequest, AddHoldingRequest } from '../types/portfolio'

// Query keys
export const portfolioKeys = {
  all: ['portfolios'] as const,
  lists: () => [...portfolioKeys.all, 'list'] as const,
  list: () => portfolioKeys.lists(),
  details: () => [...portfolioKeys.all, 'detail'] as const,
  detail: (id: string) => [...portfolioKeys.details(), id] as const,
  stocks: (query: string) => ['stocks', 'search', query] as const,
}

// Fetch all portfolios
export function usePortfolios() {
  return useQuery({
    queryKey: portfolioKeys.list(),
    queryFn: fetchPortfolios,
  })
}

// Fetch single portfolio with holdings
export function usePortfolio(id: string | undefined) {
  return useQuery({
    queryKey: portfolioKeys.detail(id!),
    queryFn: () => fetchPortfolio(id!),
    enabled: !!id,
  })
}

// Create portfolio mutation
export function useCreatePortfolio() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (request: CreatePortfolioRequest) => createPortfolio(request),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: portfolioKeys.lists() })
    },
  })
}

// Update portfolio mutation
export function useUpdatePortfolio() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({ id, updates }: { id: string; updates: Partial<CreatePortfolioRequest> }) =>
      updatePortfolio(id, updates),
    onSuccess: (data) => {
      queryClient.invalidateQueries({ queryKey: portfolioKeys.lists() })
      queryClient.invalidateQueries({ queryKey: portfolioKeys.detail(data.id) })
    },
  })
}

// Delete portfolio mutation
export function useDeletePortfolio() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (id: string) => deletePortfolio(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: portfolioKeys.lists() })
    },
  })
}

// Add holding mutation
export function useAddHolding() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({
      portfolioId,
      request,
    }: {
      portfolioId: string
      request: AddHoldingRequest
    }) => addHolding(portfolioId, request),
    onSuccess: (_, { portfolioId }) => {
      queryClient.invalidateQueries({ queryKey: portfolioKeys.detail(portfolioId) })
      queryClient.invalidateQueries({ queryKey: portfolioKeys.lists() })
    },
  })
}

// Update holding mutation
export function useUpdateHolding() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({
      holdingId,
      portfolioId,
      updates,
    }: {
      holdingId: string
      portfolioId: string
      updates: Partial<AddHoldingRequest>
    }) => updateHolding(holdingId, updates),
    onSuccess: (_, { portfolioId }) => {
      queryClient.invalidateQueries({ queryKey: portfolioKeys.detail(portfolioId) })
      queryClient.invalidateQueries({ queryKey: portfolioKeys.lists() })
    },
  })
}

// Delete holding mutation
export function useDeleteHolding() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: ({
      holdingId,
      portfolioId,
    }: {
      holdingId: string
      portfolioId: string
    }) => deleteHolding(holdingId),
    onSuccess: (_, { portfolioId }) => {
      queryClient.invalidateQueries({ queryKey: portfolioKeys.detail(portfolioId) })
      queryClient.invalidateQueries({ queryKey: portfolioKeys.lists() })
    },
  })
}

// Stock search hook
export function useStockSearch(query: string) {
  return useQuery({
    queryKey: portfolioKeys.stocks(query),
    queryFn: () => searchStocks(query),
    enabled: query.length >= 1,
    staleTime: 60 * 1000, // Cache for 1 minute
  })
}
