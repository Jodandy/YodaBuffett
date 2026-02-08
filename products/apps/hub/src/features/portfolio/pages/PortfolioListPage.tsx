import { useState } from 'react'
import { Link } from 'react-router-dom'
import { PlusIcon, BriefcaseIcon } from '@heroicons/react/24/outline'
import { usePortfolios, useCreatePortfolio } from '../hooks/usePortfolios'

export default function PortfolioListPage() {
  const [showCreateModal, setShowCreateModal] = useState(false)
  const [newPortfolioName, setNewPortfolioName] = useState('')
  const [newPortfolioDescription, setNewPortfolioDescription] = useState('')

  const { data: portfolios = [], isLoading, error } = usePortfolios()
  const createMutation = useCreatePortfolio()

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!newPortfolioName.trim()) return

    try {
      await createMutation.mutateAsync({
        name: newPortfolioName.trim(),
        description: newPortfolioDescription.trim() || undefined,
        currency: 'SEK',
      })
      setShowCreateModal(false)
      setNewPortfolioName('')
      setNewPortfolioDescription('')
    } catch (err) {
      console.error('Failed to create portfolio:', err)
    }
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-6 text-center">
        <p className="text-red-400">Failed to load portfolios</p>
        <p className="text-sm text-muted-foreground mt-2">
          {error instanceof Error ? error.message : 'Unknown error'}
        </p>
      </div>
    )
  }

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-3xl font-bold text-foreground">Portfolios</h1>
          <p className="text-muted-foreground mt-2">
            Manage your investment portfolios
          </p>
        </div>
        <button
          onClick={() => setShowCreateModal(true)}
          className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          <PlusIcon className="w-5 h-5 mr-2" />
          New Portfolio
        </button>
      </div>

      {/* Portfolio Grid */}
      {portfolios.length > 0 ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {portfolios.map((portfolio) => (
            <Link
              key={portfolio.id}
              to={`/portfolios/${portfolio.id}`}
              className="bg-card border border-border rounded-lg p-6 hover:border-blue-500 transition-colors"
            >
              <div className="flex items-center space-x-3 mb-4">
                <div className="p-2 bg-blue-500/10 rounded-lg">
                  <BriefcaseIcon className="w-6 h-6 text-blue-500" />
                </div>
                <h3 className="text-lg font-semibold text-foreground">
                  {portfolio.name}
                </h3>
              </div>
              {portfolio.description && (
                <p className="text-sm text-muted-foreground mb-4 line-clamp-2">
                  {portfolio.description}
                </p>
              )}
              <div className="space-y-2">
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Total Value</span>
                  <span className="text-foreground font-medium">
                    {portfolio.totalValue != null
                      ? `${portfolio.totalValue.toLocaleString('sv-SE')} ${portfolio.currency}`
                      : '-'}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Gain/Loss</span>
                  <span
                    className={
                      portfolio.totalGainLoss != null && portfolio.totalGainLoss >= 0
                        ? 'text-green-500'
                        : 'text-red-500'
                    }
                  >
                    {portfolio.totalGainLoss != null && portfolio.totalGainLossPercent != null
                      ? `${portfolio.totalGainLoss >= 0 ? '+' : ''}${portfolio.totalGainLoss.toLocaleString('sv-SE')} (${portfolio.totalGainLossPercent.toFixed(1)}%)`
                      : '-'}
                  </span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Holdings</span>
                  <span className="text-foreground">{portfolio.holdingsCount}</span>
                </div>
              </div>
            </Link>
          ))}
        </div>
      ) : (
        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <BriefcaseIcon className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-foreground mb-2">
            No portfolios yet
          </h2>
          <p className="text-muted-foreground mb-6">
            Create your first portfolio to start tracking investments.
          </p>
          <button
            onClick={() => setShowCreateModal(true)}
            className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            <PlusIcon className="w-5 h-5 mr-2" />
            Create Portfolio
          </button>
        </div>
      )}

      {/* Create Portfolio Modal */}
      {showCreateModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-card border border-border rounded-lg p-6 w-full max-w-md">
            <h2 className="text-xl font-semibold text-foreground mb-4">
              Create Portfolio
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
                  value={newPortfolioName}
                  onChange={(e) => setNewPortfolioName(e.target.value)}
                  placeholder="My Portfolio"
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
                  value={newPortfolioDescription}
                  onChange={(e) => setNewPortfolioDescription(e.target.value)}
                  placeholder="A brief description..."
                  rows={3}
                  className="w-full px-3 py-2 bg-background border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none"
                />
              </div>
              <div className="flex space-x-3 pt-2">
                <button
                  type="button"
                  onClick={() => {
                    setShowCreateModal(false)
                    setNewPortfolioName('')
                    setNewPortfolioDescription('')
                  }}
                  className="flex-1 py-2 bg-muted text-foreground rounded-lg hover:bg-muted/80 transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={!newPortfolioName.trim() || createMutation.isPending}
                  className="flex-1 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {createMutation.isPending ? 'Creating...' : 'Create'}
                </button>
              </div>
              {createMutation.isError && (
                <p className="text-sm text-red-400 text-center">
                  Failed to create portfolio. Please try again.
                </p>
              )}
            </form>
          </div>
        </div>
      )}
    </div>
  )
}
