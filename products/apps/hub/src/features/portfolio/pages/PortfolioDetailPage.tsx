import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import {
  ArrowLeftIcon,
  PlusIcon,
  BriefcaseIcon,
  TrashIcon,
  MagnifyingGlassIcon,
} from '@heroicons/react/24/outline'
import {
  usePortfolio,
  useAddHolding,
  useDeleteHolding,
  useStockSearch,
} from '../hooks/usePortfolios'
import type { AddHoldingRequest } from '../types/portfolio'

export default function PortfolioDetailPage() {
  const { portfolioId } = useParams<{ portfolioId: string }>()
  const { data: portfolio, isLoading, error } = usePortfolio(portfolioId)
  const addHoldingMutation = useAddHolding()
  const deleteHoldingMutation = useDeleteHolding()

  const [showAddModal, setShowAddModal] = useState(false)
  const [searchQuery, setSearchQuery] = useState('')
  const [selectedStock, setSelectedStock] = useState<{
    id: string
    symbol: string
    name: string
  } | null>(null)
  const [quantity, setQuantity] = useState('')
  const [purchasePrice, setPurchasePrice] = useState('')
  const [purchaseDate, setPurchaseDate] = useState(
    new Date().toISOString().split('T')[0]
  )

  const { data: searchResults = [] } = useStockSearch(searchQuery)

  const handleAddHolding = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!portfolioId || !selectedStock || !quantity || !purchasePrice) return

    const request: AddHoldingRequest = {
      companyId: selectedStock.id,
      symbol: selectedStock.symbol,
      companyName: selectedStock.name,
      quantity: parseFloat(quantity),
      purchasePrice: parseFloat(purchasePrice),
      purchaseDate: purchaseDate,
      currency: 'SEK',
    }

    try {
      await addHoldingMutation.mutateAsync({
        portfolioId,
        request,
      })
      resetAddForm()
    } catch (err) {
      console.error('Failed to add holding:', err)
    }
  }

  const handleDeleteHolding = async (holdingId: string) => {
    if (!portfolioId) return
    if (!confirm('Are you sure you want to delete this holding?')) return

    try {
      await deleteHoldingMutation.mutateAsync({
        holdingId,
        portfolioId,
      })
    } catch (err) {
      console.error('Failed to delete holding:', err)
    }
  }

  const resetAddForm = () => {
    setShowAddModal(false)
    setSearchQuery('')
    setSelectedStock(null)
    setQuantity('')
    setPurchasePrice('')
    setPurchaseDate(new Date().toISOString().split('T')[0])
  }

  if (isLoading) {
    return (
      <div className="space-y-8">
        <Link
          to="/portfolios"
          className="inline-flex items-center text-muted-foreground hover:text-foreground transition-colors"
        >
          <ArrowLeftIcon className="w-4 h-4 mr-2" />
          Back to Portfolios
        </Link>
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
        </div>
      </div>
    )
  }

  if (error || !portfolio) {
    return (
      <div className="space-y-8">
        <Link
          to="/portfolios"
          className="inline-flex items-center text-muted-foreground hover:text-foreground transition-colors"
        >
          <ArrowLeftIcon className="w-4 h-4 mr-2" />
          Back to Portfolios
        </Link>

        <div className="bg-card border border-border rounded-lg p-12 text-center">
          <BriefcaseIcon className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
          <h2 className="text-xl font-semibold text-foreground mb-2">
            Portfolio not found
          </h2>
          <p className="text-muted-foreground">
            {error instanceof Error ? error.message : 'Unable to load portfolio'}
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-8">
      {/* Back link */}
      <Link
        to="/portfolios"
        className="inline-flex items-center text-muted-foreground hover:text-foreground transition-colors"
      >
        <ArrowLeftIcon className="w-4 h-4 mr-2" />
        Back to Portfolios
      </Link>

      {/* Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-3xl font-bold text-foreground">{portfolio.name}</h1>
          {portfolio.description && (
            <p className="text-muted-foreground mt-2">{portfolio.description}</p>
          )}
        </div>
        <button
          onClick={() => setShowAddModal(true)}
          className="inline-flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          <PlusIcon className="w-5 h-5 mr-2" />
          Add Holding
        </button>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <div className="bg-card border border-border rounded-lg p-6">
          <p className="text-sm text-muted-foreground">Total Value</p>
          <p className="text-2xl font-bold text-foreground">
            {portfolio.totalValue != null
              ? `${portfolio.totalValue.toLocaleString('sv-SE')} ${portfolio.currency}`
              : '-'}
          </p>
        </div>
        <div className="bg-card border border-border rounded-lg p-6">
          <p className="text-sm text-muted-foreground">Total Cost</p>
          <p className="text-2xl font-bold text-foreground">
            {portfolio.totalCost != null
              ? `${portfolio.totalCost.toLocaleString('sv-SE')} ${portfolio.currency}`
              : '-'}
          </p>
        </div>
        <div className="bg-card border border-border rounded-lg p-6">
          <p className="text-sm text-muted-foreground">Gain/Loss</p>
          <p
            className={`text-2xl font-bold ${
              portfolio.totalGainLoss != null && portfolio.totalGainLoss >= 0
                ? 'text-green-500'
                : 'text-red-500'
            }`}
          >
            {portfolio.totalGainLoss != null && portfolio.totalGainLossPercent != null
              ? `${portfolio.totalGainLoss >= 0 ? '+' : ''}${portfolio.totalGainLoss.toLocaleString('sv-SE')} (${portfolio.totalGainLossPercent.toFixed(1)}%)`
              : '-'}
          </p>
        </div>
        <div className="bg-card border border-border rounded-lg p-6">
          <p className="text-sm text-muted-foreground">Holdings</p>
          <p className="text-2xl font-bold text-foreground">
            {portfolio.holdings.length}
          </p>
        </div>
      </div>

      {/* Holdings Table */}
      <div className="bg-card border border-border rounded-lg">
        <div className="p-6 border-b border-border">
          <h2 className="text-lg font-semibold text-foreground">Holdings</h2>
        </div>
        {portfolio.holdings.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-muted/50">
                <tr>
                  <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                    Symbol
                  </th>
                  <th className="text-left p-4 text-sm font-medium text-muted-foreground">
                    Company
                  </th>
                  <th className="text-right p-4 text-sm font-medium text-muted-foreground">
                    Quantity
                  </th>
                  <th className="text-right p-4 text-sm font-medium text-muted-foreground">
                    Purchase Price
                  </th>
                  <th className="text-right p-4 text-sm font-medium text-muted-foreground">
                    Current Price
                  </th>
                  <th className="text-right p-4 text-sm font-medium text-muted-foreground">
                    Value
                  </th>
                  <th className="text-right p-4 text-sm font-medium text-muted-foreground">
                    Gain/Loss
                  </th>
                  <th className="p-4"></th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {portfolio.holdings.map((holding) => (
                  <tr key={holding.id} className="hover:bg-muted/30">
                    <td className="p-4 font-medium text-foreground">
                      {holding.symbol}
                    </td>
                    <td className="p-4 text-muted-foreground">
                      {holding.companyName || '-'}
                    </td>
                    <td className="p-4 text-right text-foreground">
                      {holding.quantity.toLocaleString('sv-SE')}
                    </td>
                    <td className="p-4 text-right text-foreground">
                      {holding.purchasePrice.toLocaleString('sv-SE', {
                        minimumFractionDigits: 2,
                      })}
                    </td>
                    <td className="p-4 text-right text-foreground">
                      {holding.currentPrice != null
                        ? holding.currentPrice.toLocaleString('sv-SE', {
                            minimumFractionDigits: 2,
                          })
                        : '-'}
                    </td>
                    <td className="p-4 text-right text-foreground font-medium">
                      {holding.currentValue != null
                        ? holding.currentValue.toLocaleString('sv-SE', {
                            minimumFractionDigits: 2,
                          })
                        : '-'}
                    </td>
                    <td
                      className={`p-4 text-right font-medium ${
                        holding.gainLoss != null && holding.gainLoss >= 0
                          ? 'text-green-500'
                          : 'text-red-500'
                      }`}
                    >
                      {holding.gainLoss != null && holding.gainLossPercent != null
                        ? `${holding.gainLoss >= 0 ? '+' : ''}${holding.gainLoss.toLocaleString('sv-SE', { minimumFractionDigits: 2 })} (${holding.gainLossPercent.toFixed(1)}%)`
                        : '-'}
                    </td>
                    <td className="p-4">
                      <button
                        onClick={() => handleDeleteHolding(holding.id)}
                        className="p-1 text-muted-foreground hover:text-red-500 transition-colors"
                        title="Delete holding"
                      >
                        <TrashIcon className="w-4 h-4" />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="p-12 text-center">
            <p className="text-muted-foreground">
              No holdings yet. Add your first position to get started.
            </p>
          </div>
        )}
      </div>

      {/* Add Holding Modal */}
      {showAddModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-card border border-border rounded-lg p-6 w-full max-w-md">
            <h2 className="text-xl font-semibold text-foreground mb-4">
              Add Holding
            </h2>
            <form onSubmit={handleAddHolding} className="space-y-4">
              {/* Stock Search */}
              <div>
                <label className="block text-sm font-medium text-foreground mb-1">
                  Stock
                </label>
                {selectedStock ? (
                  <div className="flex items-center justify-between p-3 bg-blue-500/10 border border-blue-500/30 rounded-lg">
                    <div>
                      <span className="font-medium text-foreground">
                        {selectedStock.symbol}
                      </span>
                      <span className="text-muted-foreground ml-2">
                        {selectedStock.name}
                      </span>
                    </div>
                    <button
                      type="button"
                      onClick={() => setSelectedStock(null)}
                      className="text-muted-foreground hover:text-foreground"
                    >
                      Change
                    </button>
                  </div>
                ) : (
                  <div className="relative">
                    <MagnifyingGlassIcon className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                    <input
                      type="text"
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      placeholder="Search stocks..."
                      className="w-full pl-9 pr-3 py-2 bg-background border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
                      autoFocus
                    />
                    {searchResults.length > 0 && searchQuery.length >= 1 && (
                      <div className="absolute top-full left-0 right-0 mt-1 bg-card border border-border rounded-lg shadow-lg max-h-60 overflow-y-auto z-10">
                        {searchResults.map((stock) => (
                          <button
                            key={stock.id}
                            type="button"
                            onClick={() => {
                              setSelectedStock(stock)
                              setSearchQuery('')
                            }}
                            className="w-full text-left px-3 py-2 hover:bg-muted/50 transition-colors"
                          >
                            <span className="font-medium text-foreground">
                              {stock.symbol}
                            </span>
                            <span className="text-muted-foreground ml-2">
                              {stock.name}
                            </span>
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* Quantity */}
              <div>
                <label className="block text-sm font-medium text-foreground mb-1">
                  Quantity
                </label>
                <input
                  type="number"
                  value={quantity}
                  onChange={(e) => setQuantity(e.target.value)}
                  placeholder="100"
                  min="0.0001"
                  step="any"
                  className="w-full px-3 py-2 bg-background border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              {/* Purchase Price */}
              <div>
                <label className="block text-sm font-medium text-foreground mb-1">
                  Purchase Price (SEK)
                </label>
                <input
                  type="number"
                  value={purchasePrice}
                  onChange={(e) => setPurchasePrice(e.target.value)}
                  placeholder="100.00"
                  min="0"
                  step="0.01"
                  className="w-full px-3 py-2 bg-background border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              {/* Purchase Date */}
              <div>
                <label className="block text-sm font-medium text-foreground mb-1">
                  Purchase Date
                </label>
                <input
                  type="date"
                  value={purchaseDate}
                  onChange={(e) => setPurchaseDate(e.target.value)}
                  className="w-full px-3 py-2 bg-background border border-border rounded-lg text-foreground focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              {/* Actions */}
              <div className="flex space-x-3 pt-2">
                <button
                  type="button"
                  onClick={resetAddForm}
                  className="flex-1 py-2 bg-muted text-foreground rounded-lg hover:bg-muted/80 transition-colors"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={
                    !selectedStock ||
                    !quantity ||
                    !purchasePrice ||
                    addHoldingMutation.isPending
                  }
                  className="flex-1 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {addHoldingMutation.isPending ? 'Adding...' : 'Add'}
                </button>
              </div>
              {addHoldingMutation.isError && (
                <p className="text-sm text-red-400 text-center">
                  Failed to add holding. Please try again.
                </p>
              )}
            </form>
          </div>
        </div>
      )}
    </div>
  )
}
