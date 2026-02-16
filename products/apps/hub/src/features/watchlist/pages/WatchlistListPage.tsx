/**
 * Watchlist List Page
 * Shows all watchlists with ability to create/delete
 */

import { useState } from 'react';
import { Link } from 'react-router-dom';
import { PlusIcon, EyeIcon, TrashIcon } from '@heroicons/react/24/outline';
import { useWatchlists, useCreateWatchlist, useDeleteWatchlist } from '../hooks';
import type { Watchlist } from '../types';

export default function WatchlistListPage() {
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState<string | null>(null);
  const [newName, setNewName] = useState('');
  const [newDescription, setNewDescription] = useState('');

  const { data: watchlists = [], isLoading, error } = useWatchlists();
  const createMutation = useCreateWatchlist();
  const deleteMutation = useDeleteWatchlist();

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newName.trim()) return;

    try {
      await createMutation.mutateAsync({
        name: newName.trim(),
        description: newDescription.trim() || undefined,
      });
      setShowCreateModal(false);
      setNewName('');
      setNewDescription('');
    } catch (err) {
      console.error('Failed to create watchlist:', err);
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await deleteMutation.mutateAsync(id);
      setShowDeleteConfirm(null);
    } catch (err) {
      console.error('Failed to delete watchlist:', err);
    }
  };

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString('sv-SE', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
    });
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-6 text-center">
        <p className="text-red-400">Failed to load watchlists</p>
        <p className="text-sm text-muted-foreground mt-2">
          {error instanceof Error ? error.message : 'Unknown error'}
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-3xl font-bold text-foreground">Watchlists</h1>
          <p className="text-muted-foreground mt-2">
            Track companies from your screener searches
          </p>
        </div>
        <button
          onClick={() => setShowCreateModal(true)}
          className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          <PlusIcon className="w-5 h-5 mr-2" />
          New Watchlist
        </button>
      </div>

      {/* Watchlist Grid */}
      {watchlists.length > 0 ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {watchlists.map((watchlist: Watchlist) => (
            <div
              key={watchlist.id}
              className="bg-card border border-border rounded-lg p-6 hover:border-blue-500 transition-colors relative group"
            >
              <Link to={`/watchlist/${watchlist.id}`} className="block">
                <div className="flex items-center space-x-3 mb-4">
                  <div className="p-2 bg-green-500/10 rounded-lg">
                    <EyeIcon className="w-6 h-6 text-green-500" />
                  </div>
                  <h3 className="text-lg font-semibold text-foreground">
                    {watchlist.name}
                  </h3>
                </div>
                {watchlist.description && (
                  <p className="text-sm text-muted-foreground mb-4 line-clamp-2">
                    {watchlist.description}
                  </p>
                )}
                <div className="space-y-2">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Companies</span>
                    <span className="text-foreground font-medium">
                      {watchlist.company_count}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Created</span>
                    <span className="text-foreground text-sm">
                      {formatDate(watchlist.created_at)}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Updated</span>
                    <span className="text-foreground text-sm">
                      {formatDate(watchlist.updated_at)}
                    </span>
                  </div>
                </div>
              </Link>

              {/* Delete button */}
              <button
                onClick={(e) => {
                  e.preventDefault();
                  setShowDeleteConfirm(watchlist.id);
                }}
                className="absolute top-4 right-4 p-2 text-muted-foreground hover:text-red-500 opacity-0 group-hover:opacity-100 transition-opacity"
                title="Delete watchlist"
              >
                <TrashIcon className="w-5 h-5" />
              </button>
            </div>
          ))}
        </div>
      ) : (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <EyeIcon className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-foreground mb-2">
            No watchlists yet
          </h2>
          <p className="text-muted-foreground mb-6">
            Create a watchlist or save companies from the Quality Screener.
          </p>
          <button
            onClick={() => setShowCreateModal(true)}
            className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            <PlusIcon className="w-5 h-5 mr-2" />
            Create Watchlist
          </button>
        </div>
      )}

      {/* Create Watchlist Modal */}
      {showCreateModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-card border border-border rounded-lg p-6 w-full max-w-md">
            <h2 className="text-xl font-semibold text-foreground mb-4">
              Create Watchlist
            </h2>
            <form onSubmit={handleCreate} className="space-y-4">
              <div>
                <label
                  htmlFor="name"
                  className="block text-sm font-medium text-foreground mb-1"
                >
                  Name
                </label>
                <input
                  type="text"
                  id="name"
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                  placeholder="e.g., Q1 2025 Picks"
                  className="w-full px-3 py-2 bg-background border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
                  autoFocus
                />
              </div>
              <div>
                <label
                  htmlFor="description"
                  className="block text-sm font-medium text-foreground mb-1"
                >
                  Description (optional)
                </label>
                <textarea
                  id="description"
                  value={newDescription}
                  onChange={(e) => setNewDescription(e.target.value)}
                  placeholder="e.g., Tier 1-2 quality, Cash Cow business model"
                  rows={3}
                  className="w-full px-3 py-2 bg-background border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none"
                />
              </div>
              <div className="flex space-x-3 pt-2">
                <button
                  type="button"
                  onClick={() => {
                    setShowCreateModal(false);
                    setNewName('');
                    setNewDescription('');
                  }}
                  className="flex-1 py-2 bg-muted text-foreground rounded-lg hover:bg-muted/80 transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={!newName.trim() || createMutation.isPending}
                  className="flex-1 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {createMutation.isPending ? 'Creating...' : 'Create'}
                </button>
              </div>
              {createMutation.isError && (
                <p className="text-sm text-red-400 text-center">
                  Failed to create watchlist. Please try again.
                </p>
              )}
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
              This will permanently delete the watchlist and remove all companies from it.
              This action cannot be undone.
            </p>
            <div className="flex space-x-3">
              <button
                type="button"
                onClick={() => setShowDeleteConfirm(null)}
                className="flex-1 py-2 bg-muted text-foreground rounded-lg hover:bg-muted/80 transition-colors"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => handleDelete(showDeleteConfirm)}
                disabled={deleteMutation.isPending}
                className="flex-1 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors disabled:opacity-50"
              >
                {deleteMutation.isPending ? 'Deleting...' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
