/**
 * Save to Watchlist Modal
 * Allows creating a new watchlist or adding to existing one
 */

import { useState } from 'react';
import { useWatchlists, useCreateWatchlist, useAddCompanies } from '../hooks';
import type { Watchlist } from '../types';

interface SaveToWatchlistModalProps {
  isOpen: boolean;
  onClose: () => void;
  tickers: string[];
  source?: string;
}

export function SaveToWatchlistModal({
  isOpen,
  onClose,
  tickers,
  source,
}: SaveToWatchlistModalProps) {
  const [mode, setMode] = useState<'select' | 'create'>('select');
  const [selectedWatchlist, setSelectedWatchlist] = useState<string>('');
  const [newName, setNewName] = useState('');
  const [newDescription, setNewDescription] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const { data: watchlists, isLoading } = useWatchlists();
  const createWatchlist = useCreateWatchlist();
  const addCompanies = useAddCompanies();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setSuccess(null);

    try {
      let watchlistId = selectedWatchlist;

      // Create new watchlist if needed
      if (mode === 'create') {
        if (!newName.trim()) {
          setError('Please enter a watchlist name');
          return;
        }
        const created = await createWatchlist.mutateAsync({
          name: newName.trim(),
          description: newDescription.trim() || undefined,
        });
        watchlistId = created.id;
      }

      if (!watchlistId) {
        setError('Please select or create a watchlist');
        return;
      }

      // Add companies
      const result = await addCompanies.mutateAsync({
        watchlistId,
        data: {
          tickers,
          source,
        },
      });

      if (result.added > 0) {
        const msg = `Added ${result.added} companies to watchlist` +
          (result.skipped > 0 ? ` (${result.skipped} already existed)` : '');
        setSuccess(msg);
        setTimeout(() => {
          onClose();
          // Reset state
          setMode('select');
          setSelectedWatchlist('');
          setNewName('');
          setNewDescription('');
          setSuccess(null);
        }, 1500);
      } else if (result.skipped > 0) {
        setError('All companies already in watchlist');
      } else {
        setError('No companies were added');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save');
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black bg-opacity-50 transition-opacity"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="flex min-h-full items-center justify-center p-4">
        <div className="relative bg-white rounded-lg shadow-xl max-w-md w-full p-6">
          {/* Header */}
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold text-gray-900">
              Save to Watchlist
            </h3>
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-gray-500"
            >
              <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          {/* Company count */}
          <p className="text-sm text-gray-500 mb-4">
            {tickers.length} {tickers.length === 1 ? 'company' : 'companies'} selected
          </p>

          {/* Mode toggle */}
          <div className="flex border border-gray-300 rounded-md overflow-hidden mb-4">
            <button
              type="button"
              onClick={() => setMode('select')}
              className={`flex-1 px-4 py-2 text-sm font-medium ${
                mode === 'select'
                  ? 'bg-indigo-500 text-white'
                  : 'bg-white text-gray-700 hover:bg-gray-50'
              }`}
            >
              Existing Watchlist
            </button>
            <button
              type="button"
              onClick={() => setMode('create')}
              className={`flex-1 px-4 py-2 text-sm font-medium ${
                mode === 'create'
                  ? 'bg-indigo-500 text-white'
                  : 'bg-white text-gray-700 hover:bg-gray-50'
              }`}
            >
              New Watchlist
            </button>
          </div>

          <form onSubmit={handleSubmit}>
            {mode === 'select' ? (
              // Select existing watchlist
              <div className="mb-4">
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Select Watchlist
                </label>
                {isLoading ? (
                  <p className="text-sm text-gray-500">Loading...</p>
                ) : watchlists && watchlists.length > 0 ? (
                  <select
                    value={selectedWatchlist}
                    onChange={(e) => setSelectedWatchlist(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-indigo-500 focus:border-indigo-500"
                  >
                    <option value="">Choose a watchlist...</option>
                    {watchlists.map((w: Watchlist) => (
                      <option key={w.id} value={w.id}>
                        {w.name} ({w.company_count} companies)
                      </option>
                    ))}
                  </select>
                ) : (
                  <p className="text-sm text-gray-500">
                    No watchlists yet.{' '}
                    <button
                      type="button"
                      onClick={() => setMode('create')}
                      className="text-indigo-600 hover:text-indigo-700"
                    >
                      Create one
                    </button>
                  </p>
                )}
              </div>
            ) : (
              // Create new watchlist
              <>
                <div className="mb-4">
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Watchlist Name
                  </label>
                  <input
                    type="text"
                    value={newName}
                    onChange={(e) => setNewName(e.target.value)}
                    placeholder="e.g., Quality Feb 2025"
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-indigo-500 focus:border-indigo-500"
                  />
                </div>
                <div className="mb-4">
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Description (optional)
                  </label>
                  <input
                    type="text"
                    value={newDescription}
                    onChange={(e) => setNewDescription(e.target.value)}
                    placeholder="e.g., Tier 2-3 + Good Cash + Cash Cow"
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-indigo-500 focus:border-indigo-500"
                  />
                </div>
              </>
            )}

            {/* Source info */}
            {source && (
              <p className="text-xs text-gray-400 mb-4">
                Source: {source}
              </p>
            )}

            {/* Error/Success messages */}
            {error && (
              <div className="mb-4 p-3 bg-red-50 text-red-700 rounded-md text-sm">
                {error}
              </div>
            )}
            {success && (
              <div className="mb-4 p-3 bg-green-50 text-green-700 rounded-md text-sm">
                {success}
              </div>
            )}

            {/* Actions */}
            <div className="flex justify-end gap-3">
              <button
                type="button"
                onClick={onClose}
                className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={createWatchlist.isPending || addCompanies.isPending}
                className="px-4 py-2 text-sm font-medium text-white bg-indigo-600 rounded-md hover:bg-indigo-700 disabled:opacity-50"
              >
                {createWatchlist.isPending || addCompanies.isPending
                  ? 'Saving...'
                  : 'Save'}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}
