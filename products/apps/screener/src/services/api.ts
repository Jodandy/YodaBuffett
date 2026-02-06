import { createApiClient } from '@yodabuffett/api-client'
import type {
  ScreenerQuery,
  ScreenerResponse,
  BacktestRequest,
  BacktestResponse,
  MetricDefinition,
  SavedQuery,
  SaveQueryRequest,
  ApiResponse,
  ExportRequest,
  ExportResponse,
} from '@/types/screener'

// Re-export error utilities from @yodabuffett/api-client
export {
  isNetworkError,
  isClientError,
  isServerError,
  getErrorMessage,
  formatValidationErrors,
} from '@yodabuffett/api-client'

// Create the API client using the shared factory
const api = createApiClient({
  baseURL: import.meta.env.VITE_API_BASE_URL || '/api/v1',
  timeout: 30000,
  onUnauthorized: () => {
    console.warn('Unauthorized access detected')
  },
  onServerError: () => {
    console.error('Server error detected')
  },
})

// Screener API endpoints
export const screenerApi = {
  runScreen: async (query: ScreenerQuery): Promise<ScreenerResponse> => {
    const response = await api.post('/screener/run', query)
    return response.data
  },

  validateQuery: async (query: ScreenerQuery): Promise<ApiResponse> => {
    const response = await api.get('/screener/validate', { data: query })
    return response.data
  },

  exportResults: async (
    query: ScreenerQuery,
    exportRequest: ExportRequest
  ): Promise<ExportResponse> => {
    const response = await api.post('/screener/export', {
      query,
      ...exportRequest,
    })
    return response.data
  },

  saveQuery: async (request: SaveQueryRequest): Promise<SavedQuery> => {
    const response = await api.post('/screener/save', request)
    return response.data
  },

  getSavedQueries: async (params?: {
    includePublic?: boolean
    tags?: string[]
    limit?: number
    offset?: number
  }): Promise<SavedQuery[]> => {
    const response = await api.get('/screener/saved', { params })
    return response.data
  },

  getSavedQuery: async (queryId: string): Promise<SavedQuery> => {
    const response = await api.get(`/screener/saved/${queryId}`)
    return response.data
  },

  updateSavedQuery: async (
    queryId: string,
    request: SaveQueryRequest
  ): Promise<SavedQuery> => {
    const response = await api.put(`/screener/saved/${queryId}`, request)
    return response.data
  },

  deleteSavedQuery: async (queryId: string): Promise<void> => {
    await api.delete(`/screener/saved/${queryId}`)
  },
}

// Backtest API endpoints
export const backtestApi = {
  runBacktest: async (request: BacktestRequest): Promise<BacktestResponse> => {
    const response = await api.post('/backtest/run', request)
    return response.data
  },

  quickBacktest: async (
    request: BacktestRequest,
    maxPeriods: number = 12
  ): Promise<BacktestResponse> => {
    const response = await api.post(
      `/backtest/quick?max_periods=${maxPeriods}`,
      request
    )
    return response.data
  },

  compareStrategies: async (
    strategy1: BacktestRequest,
    strategy2: BacktestRequest
  ): Promise<ApiResponse> => {
    const response = await api.post('/backtest/compare', {
      strategy_1: strategy1,
      strategy_2: strategy2,
    })
    return response.data
  },

  getSavedBacktests: async (params?: {
    queryId?: string
    limit?: number
    offset?: number
  }): Promise<unknown[]> => {
    const response = await api.get('/backtest/saved', { params })
    return response.data
  },

  getBacktestResult: async (backtestId: string): Promise<BacktestResponse> => {
    const response = await api.get(`/backtest/${backtestId}`)
    return response.data
  },

  getBenchmarks: async (): Promise<ApiResponse> => {
    const response = await api.get('/backtest/performance/benchmarks')
    return response.data
  },

  optimizeStrategy: async (
    baseQuery: BacktestRequest,
    optimizationParams: unknown
  ): Promise<ApiResponse> => {
    const response = await api.post('/backtest/optimize', {
      base_query: baseQuery,
      optimization_params: optimizationParams,
    })
    return response.data
  },

  getPeriodAnalytics: async (
    backtestId: string,
    periodType: 'monthly' | 'quarterly' | 'yearly' = 'monthly'
  ): Promise<ApiResponse> => {
    const response = await api.get(`/backtest/analytics/periods`, {
      params: { backtest_id: backtestId, period_type: periodType },
    })
    return response.data
  },
}

// Metrics API endpoints
export const metricsApi = {
  getAvailableMetrics: async (params?: {
    category?: string
    dataType?: string
    relativeOnly?: boolean
  }): Promise<MetricDefinition[]> => {
    const response = await api.get('/metrics/available', { params })
    return response.data
  },

  getCategories: async (): Promise<ApiResponse> => {
    const response = await api.get('/metrics/categories')
    return response.data
  },

  getOperators: async (): Promise<ApiResponse> => {
    const response = await api.get('/metrics/operators')
    return response.data
  },

  getMetricDefinition: async (metricId: string): Promise<MetricDefinition> => {
    const response = await api.get(`/metrics/${metricId}`)
    return response.data
  },

  getExampleQueries: async (): Promise<ApiResponse> => {
    const response = await api.get('/metrics/examples/queries')
    return response.data
  },

  getValidationRules: async (): Promise<ApiResponse> => {
    const response = await api.get('/metrics/validation/rules')
    return response.data
  },
}

export default api
