/**
 * Watchlist React Query Hooks
 */

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  fetchWatchlists,
  fetchWatchlist,
  createWatchlist,
  updateWatchlist,
  deleteWatchlist,
  addCompaniesToWatchlist,
  removeCompanyFromWatchlist,
} from './api';
import type { CreateWatchlistRequest, AddCompaniesRequest } from './types';

const WATCHLISTS_KEY = ['watchlists'];

/**
 * Fetch all watchlists
 */
export function useWatchlists() {
  return useQuery({
    queryKey: WATCHLISTS_KEY,
    queryFn: fetchWatchlists,
  });
}

/**
 * Fetch a single watchlist with companies
 */
export function useWatchlist(id: string | undefined) {
  return useQuery({
    queryKey: [...WATCHLISTS_KEY, id],
    queryFn: () => fetchWatchlist(id!),
    enabled: !!id,
  });
}

/**
 * Create a new watchlist
 */
export function useCreateWatchlist() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (data: CreateWatchlistRequest) => createWatchlist(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: WATCHLISTS_KEY });
    },
  });
}

/**
 * Update a watchlist
 */
export function useUpdateWatchlist() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ id, data }: { id: string; data: Partial<CreateWatchlistRequest> }) =>
      updateWatchlist(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: WATCHLISTS_KEY });
    },
  });
}

/**
 * Delete a watchlist
 */
export function useDeleteWatchlist() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (id: string) => deleteWatchlist(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: WATCHLISTS_KEY });
    },
  });
}

/**
 * Add companies to a watchlist
 */
export function useAddCompanies() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ watchlistId, data }: { watchlistId: string; data: AddCompaniesRequest }) =>
      addCompaniesToWatchlist(watchlistId, data),
    onSuccess: (_, { watchlistId }) => {
      queryClient.invalidateQueries({ queryKey: WATCHLISTS_KEY });
      queryClient.invalidateQueries({ queryKey: [...WATCHLISTS_KEY, watchlistId] });
    },
  });
}

/**
 * Remove a company from a watchlist
 */
export function useRemoveCompany() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ watchlistId, ticker }: { watchlistId: string; ticker: string }) =>
      removeCompanyFromWatchlist(watchlistId, ticker),
    onSuccess: (_, { watchlistId }) => {
      queryClient.invalidateQueries({ queryKey: WATCHLISTS_KEY });
      queryClient.invalidateQueries({ queryKey: [...WATCHLISTS_KEY, watchlistId] });
    },
  });
}
