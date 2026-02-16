/**
 * Watchlist Types
 */

export interface Watchlist {
  id: string;
  name: string;
  description: string | null;
  created_at: string;
  updated_at: string;
  company_count: number;
}

export interface WatchlistItem {
  id: string;
  company_id: string;
  ticker: string;
  company_name: string;
  added_at: string;
  source: string | null;
  notes: string | null;
  current_price: number | null;
  price_when_added: number | null;
  return_pct: number | null;
}

export interface WatchlistDetail extends Omit<Watchlist, 'company_count'> {
  companies: WatchlistItem[];
}

export interface CreateWatchlistRequest {
  name: string;
  description?: string;
}

export interface AddCompaniesRequest {
  tickers: string[];
  source?: string;
}

export interface AddCompaniesResponse {
  added: number;
  skipped: number;
  not_found: string[];
}
