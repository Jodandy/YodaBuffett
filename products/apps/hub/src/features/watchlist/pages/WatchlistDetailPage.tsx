/**
 * Watchlist Detail Page
 * Shows companies in a watchlist with prices/returns
 */

import { useState } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import {
  ArrowLeftIcon,
  TrashIcon,
  PencilIcon,
  ArrowTopRightOnSquareIcon,
} from '@heroicons/react/24/outline';
import {
  useWatchlist,
  useUpdateWatchlist,
  useDeleteWatchlist,
  useRemoveCompany,
} from '../hooks';
import type { WatchlistItem } from '../types';

export default function WatchlistDetailPage() {
  const { watchlistId } = useParams<{ watchlistId: string }>();
  const navigate = useNavigate();

  const [showEditModal, setShowEditModal] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [editName, setEditName] = useState('');
  const [editDescription, setEditDescription] = useState('');
  const [removeConfirm, setRemoveConfirm] = useState<string | null>(null);

  const { data: watchlist, isLoading, error } = useWatchlist(watchlistId);
  const updateMutation = useUpdateWatchlist();
  const deleteMutation = useDeleteWatchlist();
  const removeMutation = useRemoveCompany();

  const handleEdit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!watchlistId || !editName.trim()) return;

    try {
      await updateMutation.mutateAsync({
        id: watchlistId,
        data: {
          name: editName.trim(),
          description: editDescription.trim() || undefined,
        },
      });
      setShowEditModal(false);
    } catch (err) {
      console.error('Failed to update watchlist:', err);
    }
  };

  const handleDelete = async () => {
    if (!watchlistId) return;

    try {
      await deleteMutation.mutateAsync(watchlistId);
      navigate('/watchlist');
    } catch (err) {
      console.error('Failed to delete watchlist:', err);
    }
  };

  const handleRemoveCompany = async (ticker: string) => {
    if (!watchlistId) return;

    try {
      await removeMutation.mutateAsync({ watchlistId, ticker });
      setRemoveConfirm(null);
    } catch (err) {
      console.error('Failed to remove company:', err);
    }
  };

  const openEditModal = () => {
    if (watchlist) {
      setEditName(watchlist.name);
      setEditDescription(watchlist.description || '');
      setShowEditModal(true);
    }
  };

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString('sv-SE', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  };

  const formatPrice = (price: number | null) => {
    if (price === null) return '-';
    return price.toLocaleString('sv-SE', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  };

  const formatReturn = (returnPct: number | null) => {
    if (returnPct === null) return '-';
    const sign = returnPct >= 0 ? '+' : '';
    return `${sign}${returnPct.toFixed(1)}%`;
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
      </div>
    );
  }

  if (error || !watchlist) {
    return (
      <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-6 text-center">
        <p className="text-red-400">Failed to load watchlist</p>
        <p className="text-sm text-muted-foreground mt-2">
          {error instanceof Error ? error.message : 'Watchlist not found'}
        </p>
        <Link
          to="/watchlist"
          className="inline-flex items-center mt-4 text-blue-500 hover:text-blue-400"
        >
          <ArrowLeftIcon className="w-4 h-4 mr-2" />
          Back to watchlists
        </Link>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <Link
            to="/watchlist"
            className="inline-flex items-center text-muted-foreground hover:text-foreground mb-2"
          >
            <ArrowLeftIcon className="w-4 h-4 mr-1" />
            Watchlists
          </Link>
          <h1 className="text-3xl font-bold text-foreground">{watchlist.name}</h1>
          {watchlist.description && (
            <p className="text-muted-foreground mt-1">{watchlist.description}</p>
          )}
          <div className="flex items-center gap-4 mt-2 text-sm text-muted-foreground">
            <span>Created {formatDate(watchlist.created_at)}</span>
            <span>Updated {formatDate(watchlist.updated_at)}</span>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={openEditModal}
            className="inline-flex items-center px-3 py-2 text-foreground border border-border rounded-lg hover:bg-muted transition-colors"
          >
            <PencilIcon className="w-4 h-4 mr-2" />
            Edit
          </button>
          <button
            onClick={() => setShowDeleteConfirm(true)}
            className="inline-flex items-center px-3 py-2 text-red-500 border border-red-500/30 rounded-lg hover:bg-red-500/10 transition-colors"
          >
            <TrashIcon className="w-4 h-4 mr-2" />
            Delete
          </button>
        </div>
      </div>

      {/* Companies Table */}
      {watchlist.companies.length > 0 ? (
        <div className="bg-card border border-border rounded-lg overflow-hidden">
          <table className="w-full">
            <thead className="bg-muted/50">
              <tr>
                <th className="text-left px-4 py-3 text-sm font-medium text-muted-foreground">
                  Company
                </th>
                <th className="text-left px-4 py-3 text-sm font-medium text-muted-foreground">
                  Added
                </th>
                <th className="text-right px-4 py-3 text-sm font-medium text-muted-foreground">
                  Price When Added
                </th>
                <th className="text-right px-4 py-3 text-sm font-medium text-muted-foreground">
                  Current Price
                </th>
                <th className="text-right px-4 py-3 text-sm font-medium text-muted-foreground">
                  Return
                </th>
                <th className="text-left px-4 py-3 text-sm font-medium text-muted-foreground">
                  Source
                </th>
                <th className="w-10"></th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {watchlist.companies.map((item: WatchlistItem) => (
                <tr key={item.id} className="hover:bg-muted/30 transition-colors">
                  <td className="px-4 py-3">
                    <Link
                      to={`/company/${item.ticker}`}
                      className="flex items-center gap-2 hover:text-blue-500"
                    >
                      <div>
                        <div className="font-medium text-foreground">
                          {item.ticker}
                        </div>
                        <div className="text-sm text-muted-foreground">
                          {item.company_name}
                        </div>
                      </div>
                      <ArrowTopRightOnSquareIcon className="w-4 h-4 text-muted-foreground" />
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-sm text-muted-foreground">
                    {formatDate(item.added_at)}
                  </td>
                  <td className="px-4 py-3 text-right text-foreground">
                    {formatPrice(item.price_when_added)}
                  </td>
                  <td className="px-4 py-3 text-right text-foreground">
                    {formatPrice(item.current_price)}
                  </td>
                  <td className="px-4 py-3 text-right">
                    <span
                      className={
                        item.return_pct !== null && item.return_pct >= 0
                          ? 'text-green-500'
                          : 'text-red-500'
                      }
                    >
                      {formatReturn(item.return_pct)}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    {item.source && (
                      <span className="text-xs text-muted-foreground bg-muted px-2 py-1 rounded">
                        {item.source.length > 40
                          ? item.source.substring(0, 40) + '...'
                          : item.source}
                      </span>
                    )}
                  </td>
                  <td className="px-4 py-3">
                    <button
                      onClick={() => setRemoveConfirm(item.ticker)}
                      className="p-1 text-muted-foreground hover:text-red-500 transition-colors"
                      title="Remove from watchlist"
                    >
                      <TrashIcon className="w-4 h-4" />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          {/* Summary Row */}
          <div className="bg-muted/30 px-4 py-3 border-t border-border">
            <div className="flex justify-between text-sm">
              <span className="text-muted-foreground">
                {watchlist.companies.length} companies
              </span>
              <span className="text-muted-foreground">
                Avg Return:{' '}
                <span
                  className={
                    calculateAvgReturn(watchlist.companies) >= 0
                      ? 'text-green-500'
                      : 'text-red-500'
                  }
                >
                  {formatReturn(calculateAvgReturn(watchlist.companies))}
                </span>
              </span>
            </div>
          </div>
        </div>
      ) : (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <p className="text-muted-foreground mb-4">No companies in this watchlist yet.</p>
          <Link
            to="/quality"
            className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            Go to Quality Screener
          </Link>
        </div>
      )}

      {/* Edit Modal */}
      {showEditModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-card border border-border rounded-lg p-6 w-full max-w-md">
            <h2 className="text-xl font-semibold text-foreground mb-4">
              Edit Watchlist
            </h2>
            <form onSubmit={handleEdit} className="space-y-4">
              <div>
                <label
                  htmlFor="edit-name"
                  className="block text-sm font-medium text-foreground mb-1"
                >
                  Name
                </label>
                <input
                  type="text"
                  id="edit-name"
                  value={editName}
                  onChange={(e) => setEditName(e.target.value)}
                  className="w-full px-3 py-2 bg-background border border-border rounded-lg text-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
                  autoFocus
                />
              </div>
              <div>
                <label
                  htmlFor="edit-description"
                  className="block text-sm font-medium text-foreground mb-1"
                >
                  Description (optional)
                </label>
                <textarea
                  id="edit-description"
                  value={editDescription}
                  onChange={(e) => setEditDescription(e.target.value)}
                  rows={3}
                  className="w-full px-3 py-2 bg-background border border-border rounded-lg text-foreground focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none"
                />
              </div>
              <div className="flex space-x-3 pt-2">
                <button
                  type="button"
                  onClick={() => setShowEditModal(false)}
                  className="flex-1 py-2 bg-muted text-foreground rounded-lg hover:bg-muted/80 transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={!editName.trim() || updateMutation.isPending}
                  className="flex-1 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50"
                >
                  {updateMutation.isPending ? 'Saving...' : 'Save'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Delete Confirmation Modal */}
      {showDeleteConfirm && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-card border border-border rounded-lg p-6 w-full max-w-md">
            <h2 className="text-xl font-semibold text-foreground mb-4">
              Delete Watchlist?
            </h2>
            <p className="text-muted-foreground mb-6">
              This will permanently delete "{watchlist.name}" and remove all companies
              from it. This action cannot be undone.
            </p>
            <div className="flex space-x-3">
              <button
                type="button"
                onClick={() => setShowDeleteConfirm(false)}
                className="flex-1 py-2 bg-muted text-foreground rounded-lg hover:bg-muted/80 transition-colors"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleDelete}
                disabled={deleteMutation.isPending}
                className="flex-1 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors disabled:opacity-50"
              >
                {deleteMutation.isPending ? 'Deleting...' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Remove Company Confirmation */}
      {removeConfirm && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-card border border-border rounded-lg p-6 w-full max-w-md">
            <h2 className="text-xl font-semibold text-foreground mb-4">
              Remove Company?
            </h2>
            <p className="text-muted-foreground mb-6">
              Remove {removeConfirm} from this watchlist?
            </p>
            <div className="flex space-x-3">
              <button
                type="button"
                onClick={() => setRemoveConfirm(null)}
                className="flex-1 py-2 bg-muted text-foreground rounded-lg hover:bg-muted/80 transition-colors"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => handleRemoveCompany(removeConfirm)}
                disabled={removeMutation.isPending}
                className="flex-1 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors disabled:opacity-50"
              >
                {removeMutation.isPending ? 'Removing...' : 'Remove'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function calculateAvgReturn(companies: WatchlistItem[]): number {
  const withReturns = companies.filter((c) => c.return_pct !== null);
  if (withReturns.length === 0) return 0;
  const sum = withReturns.reduce((acc, c) => acc + (c.return_pct || 0), 0);
  return sum / withReturns.length;
}
