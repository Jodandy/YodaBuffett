import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import QueryBuilder from '@/components/screener/QueryBuilder'
import BacktestResults from '@/components/backtest/BacktestResults'
import BacktestSettings from '@/components/backtest/BacktestSettings'
import { backtestApi, metricsApi } from '@/services/api'
import type { BacktestRequest, BacktestResponse, ScreenerQuery } from '@/types/screener'
import { PlayIcon, ClockIcon, ChartBarIcon } from '@heroicons/react/24/outline'
import toast from 'react-hot-toast'

const defaultQuery: ScreenerQuery = {
  groups: [],
  groupLogic: 'AND',
  columns: ['pe_ratio', 'pb_ratio', 'market_cap', 'roe'],
  includeForwardReturns: ['1M', '3M', '1Y'],
}

const defaultRequest: BacktestRequest = {
  query: defaultQuery,
  startDate: '2022-01-01',
  endDate: '2024-12-01',
  frequency: 'monthly',
  forwardPeriods: ['1M', '3M', '1Y'],
}

export default function BacktestPage() {
  const [backtestRequest, setBacktestRequest] = useState<BacktestRequest>(defaultRequest)
  const [results, setResults] = useState<BacktestResponse | null>(null)
  const [isExecuting, setIsExecuting] = useState(false)
  const [executionProgress, setExecutionProgress] = useState(0)

  // Fetch available metrics
  const { data: metrics = [], isLoading: metricsLoading } = useQuery({
    queryKey: ['metrics', 'available'],
    queryFn: () => metricsApi.getAvailableMetrics(),
    staleTime: 10 * 60 * 1000,
  })

  // Update query within backtest request
  const updateQuery = (query: ScreenerQuery) => {
    setBacktestRequest(prev => ({
      ...prev,
      query: {
        ...query,
        includeForwardReturns: prev.forwardPeriods, // Sync with backtest settings
      }
    }))
  }

  // Update backtest settings
  const updateSettings = (settings: Partial<BacktestRequest>) => {
    setBacktestRequest(prev => ({
      ...prev,
      ...settings,
      query: {
        ...prev.query,
        includeForwardReturns: settings.forwardPeriods || prev.forwardPeriods,
      }
    }))
  }

  // Execute backtest
  const executeBacktest = async (quick: boolean = false) => {
    if (backtestRequest.query.groups.length === 0) {
      toast.error('Please add at least one screening condition')
      return
    }

    setIsExecuting(true)
    setExecutionProgress(0)
    
    try {
      let response: BacktestResponse
      
      if (quick) {
        // Quick backtest for rapid feedback
        toast.info('Running quick backtest (12 periods max)...')
        response = await backtestApi.quickBacktest(backtestRequest, 12)
      } else {
        // Full backtest
        const startDate = new Date(backtestRequest.startDate)
        const endDate = new Date(backtestRequest.endDate)
        const monthsDiff = (endDate.getFullYear() - startDate.getFullYear()) * 12 + 
                          (endDate.getMonth() - startDate.getMonth())
        
        toast.info(`Running full backtest (${monthsDiff} periods)...`)
        
        // Simulate progress for long-running backtests
        const progressInterval = setInterval(() => {
          setExecutionProgress(prev => Math.min(prev + 10, 90))
        }, 500)
        
        response = await backtestApi.runBacktest(backtestRequest)
        clearInterval(progressInterval)
      }
      
      setExecutionProgress(100)
      setResults(response)
      
      const avgReturn = Object.values(response.summary.avgReturns)[0] || 0
      const totalSignals = response.summary.totalSignals
      
      toast.success(
        `Backtest complete: ${totalSignals} total signals, ` +
        `${avgReturn.toFixed(2)}% avg return`
      )
      
    } catch (error: any) {
      toast.error(`Backtest failed: ${error.response?.data?.detail || error.message}`)
      console.error('Backtest error:', error)
    } finally {
      setIsExecuting(false)
      setExecutionProgress(0)
    }
  }

  // Calculate estimated execution time
  const getEstimatedTime = () => {
    const startDate = new Date(backtestRequest.startDate)
    const endDate = new Date(backtestRequest.endDate)
    const months = (endDate.getFullYear() - startDate.getFullYear()) * 12 + 
                   (endDate.getMonth() - startDate.getMonth())
    
    const complexityScore = backtestRequest.query.groups.length * 2 + 
                           backtestRequest.query.groups.reduce((acc, g) => acc + g.conditions.length, 0)
    
    const estimatedSeconds = (months * complexityScore * 0.1) + 5
    return Math.min(estimatedSeconds, 300) // Cap at 5 minutes
  }

  return (
    <div className="space-y-8">
      {/* Page header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-3xl font-bold text-foreground">Strategy Backtest</h1>
          <p className="text-muted-foreground mt-2">
            Test your screening strategies on historical data with point-in-time analysis 
            and forward return calculation
          </p>
        </div>

        <div className="flex items-center space-x-3">
          <div className="text-right text-sm text-muted-foreground">
            <div className="flex items-center space-x-1">
              <ClockIcon className="w-4 h-4" />
              <span>Est. {Math.ceil(getEstimatedTime())}s</span>
            </div>
            <div>
              {new Date(backtestRequest.startDate).toLocaleDateString()} - {' '}
              {new Date(backtestRequest.endDate).toLocaleDateString()}
            </div>
          </div>

          <button
            onClick={() => executeBacktest(true)}
            disabled={isExecuting || backtestRequest.query.groups.length === 0}
            className="btn-secondary flex items-center space-x-2"
          >
            <ChartBarIcon className="w-4 h-4" />
            <span>Quick Test</span>
          </button>

          <button
            onClick={() => executeBacktest(false)}
            disabled={isExecuting || backtestRequest.query.groups.length === 0}
            className="btn-primary flex items-center space-x-2"
          >
            <PlayIcon className="w-4 h-4" />
            <span>{isExecuting ? 'Running...' : 'Full Backtest'}</span>
          </button>
        </div>
      </div>

      {/* Execution progress */}
      {isExecuting && (
        <div className="card">
          <div className="card-content py-6">
            <div className="flex items-center space-x-4">
              <div className="flex-1">
                <div className="flex justify-between text-sm text-muted-foreground mb-2">
                  <span>Executing backtest...</span>
                  <span>{executionProgress}%</span>
                </div>
                <div className="w-full bg-muted rounded-full h-2">
                  <div 
                    className="bg-primary h-2 rounded-full transition-all duration-300"
                    style={{ width: `${executionProgress}%` }}
                  />
                </div>
              </div>
              <div className="loading-pulse">
                <ChartBarIcon className="w-6 h-6 text-primary" />
              </div>
            </div>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Query builder */}
        <div className="lg:col-span-2">
          <div className="card">
            <div className="card-header">
              <h2 className="card-title">Screening Strategy</h2>
              <p className="card-description">
                Define the screening conditions for your backtest strategy
              </p>
            </div>
            <div className="card-content">
              {metricsLoading ? (
                <div className="flex items-center justify-center py-12">
                  <div className="loading-pulse text-muted-foreground">Loading metrics...</div>
                </div>
              ) : (
                <QueryBuilder
                  query={backtestRequest.query}
                  onChange={updateQuery}
                  availableMetrics={metrics}
                />
              )}
            </div>
          </div>
        </div>

        {/* Backtest settings */}
        <div className="lg:col-span-1">
          <BacktestSettings
            request={backtestRequest}
            onChange={updateSettings}
            estimatedTime={getEstimatedTime()}
          />
        </div>
      </div>

      {/* Results */}
      {results && (
        <div className="card">
          <div className="card-header">
            <div className="flex justify-between items-center">
              <div>
                <h2 className="card-title">Backtest Results</h2>
                <p className="card-description">
                  {results.results.length} periods tested • 
                  {results.summary.totalSignals} total signals • 
                  Executed in {results.totalExecutionTime.toFixed(1)}s
                </p>
              </div>
              
              <div className="text-right">
                <div className="text-2xl font-bold text-foreground">
                  {Object.values(results.summary.avgReturns)[0]?.toFixed(2)}%
                </div>
                <div className="text-sm text-muted-foreground">Avg Return</div>
              </div>
            </div>
          </div>
          <div className="card-content">
            <BacktestResults response={results} />
          </div>
        </div>
      )}

      {/* Empty state */}
      {!results && !isExecuting && backtestRequest.query.groups.length === 0 && (
        <div className="card">
          <div className="card-content py-16">
            <div className="text-center">
              <ChartBarIcon className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
              <h3 className="text-lg font-semibold text-foreground mb-2">
                Ready to backtest your strategy?
              </h3>
              <p className="text-muted-foreground mb-6 max-w-md mx-auto">
                Build a screening strategy and test it on historical data to see how it would 
                have performed. Get detailed performance metrics and forward returns.
              </p>
              <div className="flex justify-center space-x-6 text-sm text-muted-foreground">
                <div className="text-center">
                  <div className="w-8 h-8 bg-yb-blue/20 rounded-lg flex items-center justify-center mx-auto mb-2">
                    <span className="text-yb-blue font-semibold">1</span>
                  </div>
                  <span>Define Strategy</span>
                </div>
                <div className="text-center">
                  <div className="w-8 h-8 bg-yb-green/20 rounded-lg flex items-center justify-center mx-auto mb-2">
                    <span className="text-yb-green font-semibold">2</span>
                  </div>
                  <span>Set Time Period</span>
                </div>
                <div className="text-center">
                  <div className="w-8 h-8 bg-yb-orange/20 rounded-lg flex items-center justify-center mx-auto mb-2">
                    <span className="text-yb-orange font-semibold">3</span>
                  </div>
                  <span>Run Backtest</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}