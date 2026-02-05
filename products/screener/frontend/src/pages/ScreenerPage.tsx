import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import QueryBuilder from '@/components/screener/QueryBuilder'
import ResultsDisplay from '@/components/screener/ResultsDisplay'
import { screenerApi, metricsApi } from '@/services/api'
import type { ScreenerQuery, ScreenerResponse, MetricDefinition } from '@/types/screener'
import { PlayIcon, DocumentArrowDownIcon, BookmarkIcon, MagnifyingGlassIcon } from '@heroicons/react/24/outline'
import toast from 'react-hot-toast'

const defaultQuery: ScreenerQuery = {
  groups: [],
  groupLogic: 'AND',
  columns: ['pe_ratio', 'pb_ratio', 'market_cap', 'roe'],
  includeForwardReturns: ['1M', '3M', '1Y'],
}

export default function ScreenerPage() {
  const [query, setQuery] = useState<ScreenerQuery>(defaultQuery)
  const [results, setResults] = useState<ScreenerResponse | null>(null)
  const [isExecuting, setIsExecuting] = useState(false)

  // Fetch available metrics
  const { data: metrics = [], isLoading: metricsLoading } = useQuery({
    queryKey: ['metrics', 'available'],
    queryFn: () => metricsApi.getAvailableMetrics(),
    staleTime: 10 * 60 * 1000, // 10 minutes
  })

  // Execute screening query
  const executeQuery = async () => {
    if (query.groups.length === 0) {
      toast.error('Please add at least one screening condition')
      return
    }

    setIsExecuting(true)
    try {
      const response = await screenerApi.runScreen(query)
      setResults(response)
      toast.success(`Found ${response.totalMatches} matching companies`)
    } catch (error: any) {
      toast.error(`Screening failed: ${error.response?.data?.detail || error.message}`)
      console.error('Screening error:', error)
    } finally {
      setIsExecuting(false)
    }
  }

  // Save current query
  const saveQuery = async () => {
    const name = prompt('Enter a name for this query:')
    if (!name) return

    try {
      await screenerApi.saveQuery({
        name,
        query,
        isPublic: false,
        tags: [],
      })
      toast.success('Query saved successfully')
    } catch (error: any) {
      toast.error(`Failed to save query: ${error.response?.data?.detail || error.message}`)
    }
  }

  // Export results
  const exportResults = async (format: 'csv' | 'xlsx' | 'json') => {
    if (!results) {
      toast.error('No results to export')
      return
    }

    try {
      const exportResponse = await screenerApi.exportResults(query, {
        format,
        includeMetadata: true,
      })
      
      // Create download link
      const link = document.createElement('a')
      link.href = exportResponse.downloadUrl
      link.download = exportResponse.filename
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      
      toast.success(`Export started: ${exportResponse.filename}`)
    } catch (error: any) {
      toast.error(`Export failed: ${error.response?.data?.detail || error.message}`)
    }
  }

  return (
    <div className="space-y-8">
      {/* Page header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-3xl font-bold text-foreground">Stock Screener</h1>
          <p className="text-muted-foreground mt-2">
            Screen {metrics.length > 0 ? `787 Nordic companies` : 'companies'} using fundamental and technical metrics with point-in-time data
          </p>
        </div>

        <div className="flex items-center space-x-3">
          {results && (
            <>
              <div className="flex items-center space-x-2">
                <button
                  onClick={() => exportResults('csv')}
                  className="btn-outline flex items-center space-x-2"
                >
                  <DocumentArrowDownIcon className="w-4 h-4" />
                  <span>CSV</span>
                </button>
                <button
                  onClick={() => exportResults('xlsx')}
                  className="btn-outline flex items-center space-x-2"
                >
                  <DocumentArrowDownIcon className="w-4 h-4" />
                  <span>Excel</span>
                </button>
              </div>
              
              <button
                onClick={saveQuery}
                className="btn-secondary flex items-center space-x-2"
              >
                <BookmarkIcon className="w-4 h-4" />
                <span>Save Query</span>
              </button>
            </>
          )}

          <button
            onClick={executeQuery}
            disabled={isExecuting || query.groups.length === 0}
            className="btn-primary flex items-center space-x-2"
          >
            <PlayIcon className="w-4 h-4" />
            <span>{isExecuting ? 'Running...' : 'Run Screen'}</span>
          </button>
        </div>
      </div>

      {/* Query builder section */}
      <div className="card">
        <div className="card-header">
          <h2 className="card-title">Screening Conditions</h2>
          <p className="card-description">
            Build complex queries using fundamental and technical metrics. 
            Use relative comparisons and advanced logic combinations.
          </p>
        </div>
        <div className="card-content">
          {metricsLoading ? (
            <div className="flex items-center justify-center py-12">
              <div className="loading-pulse text-muted-foreground">Loading metrics...</div>
            </div>
          ) : (
            <QueryBuilder
              query={query}
              onChange={setQuery}
              availableMetrics={metrics}
            />
          )}
        </div>
      </div>

      {/* Results section */}
      {results && (
        <div className="card">
          <div className="card-header">
            <div className="flex justify-between items-center">
              <div>
                <h2 className="card-title">
                  Screening Results ({results.totalMatches} companies)
                </h2>
                <p className="card-description">
                  Executed in {results.executionTime.toFixed(2)}s • 
                  As of {new Date(results.asOfDate).toLocaleDateString()}
                  {results.query.includeForwardReturns && (
                    <> • Forward returns: {results.query.includeForwardReturns.join(', ')}</>
                  )}
                </p>
              </div>
              
              {results.summary && (
                <div className="text-right text-sm text-muted-foreground">
                  <div>Avg Market Cap: {Object.values(results.summary.averages)[0]?.toLocaleString() || 'N/A'}</div>
                  {results.summary.winRates && (
                    <div>Avg Win Rate (1M): {(results.summary.winRates['1M'] * 100).toFixed(1)}%</div>
                  )}
                </div>
              )}
            </div>
          </div>
          <div className="card-content">
            <ResultsDisplay
              response={results}
              onExport={exportResults}
            />
          </div>
        </div>
      )}

      {/* Empty state */}
      {!results && !isExecuting && query.groups.length === 0 && (
        <div className="card">
          <div className="card-content py-16">
            <div className="text-center">
              <MagnifyingGlassIcon className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
              <h3 className="text-lg font-semibold text-foreground mb-2">
                Ready to screen companies?
              </h3>
              <p className="text-muted-foreground mb-6 max-w-md mx-auto">
                Add screening conditions using the query builder above, then click "Run Screen" 
                to find companies matching your criteria.
              </p>
              <div className="flex justify-center space-x-4 text-sm text-muted-foreground">
                <div className="flex items-center space-x-1">
                  <span className="w-2 h-2 bg-yb-blue rounded-full"></span>
                  <span>Point-in-time data</span>
                </div>
                <div className="flex items-center space-x-1">
                  <span className="w-2 h-2 bg-yb-green rounded-full"></span>
                  <span>Forward returns</span>
                </div>
                <div className="flex items-center space-x-1">
                  <span className="w-2 h-2 bg-yb-orange rounded-full"></span>
                  <span>Complex logic</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}