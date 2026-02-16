/**
 * Watchlist API Client
 */

import type {
  Watchlist,
  WatchlistDetail,
  CreateWatchlistRequest,
  AddCompaniesRequest,
  AddCompaniesResponse,
} from './types';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000/api/v1';

/**
 * Fetch all watchlists
 */
export async function fetchWatchlists(): Promise<Watchlist[]> {
  const response = await fetch(`${API_BASE}/watchlists`);
  if (!response.ok) {
    throw new Error('Failed to fetch watchlists');
  }
  return response.json();
}

/**
 * Fetch a single watchlist with companies
 */
export async function fetchWatchlist(id: string): Promise<WatchlistDetail> {
  const response = await fetch(`${API_BASE}/watchlists/${id}`);
  if (!response.ok) {
    throw new Error('Failed to fetch watchlist');
  }
  return response.json();
}

/**
 * Create a new watchlist
 */
export async function createWatchlist(data: CreateWatchlistRequest): Promise<Watchlist> {
  const response = await fetch(`${API_BASE}/watchlists`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || 'Failed to create watchlist');
  }
  return response.json();
}

/**
 * Update a watchlist
 */
export async function updateWatchlist(
  id: string,
  data: Partial<CreateWatchlistRequest>
): Promise<Watchlist> {
  const response = await fetch(`${API_BASE}/watchlists/${id}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
  if (!response.ok) {
    throw new Error('Failed to update watchlist');
  }
  return response.json();
}

/**
 * Delete a watchlist
 */
export async function deleteWatchlist(id: string): Promise<void> {
  const response = await fetch(`${API_BASE}/watchlists/${id}`, {
    method: 'DELETE',
  });
  if (!response.ok) {
    throw new Error('Failed to delete watchlist');
  }
}

/**
 * Add companies to a watchlist
 */
export async function addCompaniesToWatchlist(
  watchlistId: string,
  data: AddCompaniesRequest
): Promise<AddCompaniesResponse> {
  const response = await fetch(`${API_BASE}/watchlists/${watchlistId}/companies`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
  if (!response.ok) {
    throw new Error('Failed to add companies');
  }
  return response.json();
}

/**
 * Remove a company from a watchlist
 */
export async function removeCompanyFromWatchlist(
  watchlistId: string,
  ticker: string
): Promise<void> {
  const response = await fetch(`${API_BASE}/watchlists/${watchlistId}/companies/${ticker}`, {
    method: 'DELETE',
  });
  if (!response.ok) {
    throw new Error('Failed to remove company');
  }
}
