import axios from 'axios'
import type {
  ScreenerQuery,
  ScreenerResponse,
  BacktestRequest,
  BacktestResponse,
  MetricDefinition,
  SavedQuery,
  SaveQueryRequest,
  ExportRequest,
  ExportResponse,
  ApiResponse
} from '@/types/screener'

// Create axios instance with base configuration
const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || '/api/v1',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Request interceptor for logging and auth
api.interceptors.request.use(
  (config) => {
    console.log(`API Request: ${config.method?.toUpperCase()} ${config.url}`)
    // Add auth token here when implemented
    // config.headers.Authorization = `Bearer ${getAuthToken()}`
    return config
  },
  (error) => {
    console.error('API Request Error:', error)
    return Promise.reject(error)
  }
)

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => {
    console.log(`API Response: ${response.status} ${response.config.url}`)
    return response
  },
  (error) => {
    console.error('API Response Error:', error.response?.data || error.message)
    
    if (error.response?.status === 401) {
      // Handle unauthorized - redirect to login
      console.warn('Unauthorized access detected')
    } else if (error.response?.status >= 500) {
      // Handle server errors
      console.error('Server error detected')
    }
    
    return Promise.reject(error)
  }
)

// Screener API endpoints
export const screenerApi = {
  // Execute screening query
  runScreen: async (query: ScreenerQuery): Promise<ScreenerResponse> => {
    const response = await api.post('/screener/run', query)
    return response.data
  },

  // Validate query without executing
  validateQuery: async (query: ScreenerQuery): Promise<ApiResponse> => {
    const response = await api.get('/screener/validate', { data: query })
    return response.data
  },

  // Export screening results
  exportResults: async (query: ScreenerQuery, exportRequest: ExportRequest): Promise<ExportResponse> => {
    const response = await api.post('/screener/export', { query, ...exportRequest })
    return response.data
  },

  // Save query
  saveQuery: async (request: SaveQueryRequest): Promise<SavedQuery> => {
    const response = await api.post('/screener/save', request)
    return response.data
  },

  // Get saved queries
  getSavedQueries: async (params?: {
    includePublic?: boolean
    tags?: string[]
    limit?: number
    offset?: number
  }): Promise<SavedQuery[]> => {
    const response = await api.get('/screener/saved', { params })
    return response.data
  },

  // Get specific saved query
  getSavedQuery: async (queryId: string): Promise<SavedQuery> => {
    const response = await api.get(`/screener/saved/${queryId}`)
    return response.data
  },

  // Update saved query
  updateSavedQuery: async (queryId: string, request: SaveQueryRequest): Promise<SavedQuery> => {
    const response = await api.put(`/screener/saved/${queryId}`, request)
    return response.data
  },

  // Delete saved query
  deleteSavedQuery: async (queryId: string): Promise<void> => {
    await api.delete(`/screener/saved/${queryId}`)
  },
}

// Backtest API endpoints
export const backtestApi = {
  // Run full backtest
  runBacktest: async (request: BacktestRequest): Promise<BacktestResponse> => {
    const response = await api.post('/backtest/run', request)
    return response.data
  },

  // Run quick backtest (limited periods)
  quickBacktest: async (request: BacktestRequest, maxPeriods: number = 12): Promise<BacktestResponse> => {
    const response = await api.post(`/backtest/quick?max_periods=${maxPeriods}`, request)
    return response.data
  },

  // Compare two strategies
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

  // Get saved backtests
  getSavedBacktests: async (params?: {
    queryId?: string
    limit?: number
    offset?: number
  }): Promise<any[]> => {
    const response = await api.get('/backtest/saved', { params })
    return response.data
  },

  // Get specific backtest result
  getBacktestResult: async (backtestId: string): Promise<BacktestResponse> => {
    const response = await api.get(`/backtest/${backtestId}`)
    return response.data
  },

  // Get performance benchmarks
  getBenchmarks: async (): Promise<ApiResponse> => {
    const response = await api.get('/backtest/performance/benchmarks')
    return response.data
  },

  // Optimize strategy parameters
  optimizeStrategy: async (
    baseQuery: BacktestRequest,
    optimizationParams: any
  ): Promise<ApiResponse> => {
    const response = await api.post('/backtest/optimize', {
      base_query: baseQuery,
      optimization_params: optimizationParams,
    })
    return response.data
  },

  // Get period analytics
  getPeriodAnalytics: async (
    backtestId: string,
    periodType: 'monthly' | 'quarterly' | 'yearly' = 'monthly'
  ): Promise<ApiResponse> => {
    const response = await api.get(`/backtest/analytics/periods`, {
      params: { backtest_id: backtestId, period_type: periodType }
    })
    return response.data
  },
}

// Metrics API endpoints
export const metricsApi = {
  // Get available metrics
  getAvailableMetrics: async (params?: {
    category?: string
    dataType?: string
    relativeOnly?: boolean
  }): Promise<MetricDefinition[]> => {
    const response = await api.get('/metrics/available', { params })
    return response.data
  },

  // Get metric categories
  getCategories: async (): Promise<ApiResponse> => {
    const response = await api.get('/metrics/categories')
    return response.data
  },

  // Get available operators
  getOperators: async (): Promise<ApiResponse> => {
    const response = await api.get('/metrics/operators')
    return response.data
  },

  // Get specific metric definition
  getMetricDefinition: async (metricId: string): Promise<MetricDefinition> => {
    const response = await api.get(`/metrics/${metricId}`)
    return response.data
  },

  // Get example queries
  getExampleQueries: async (): Promise<ApiResponse> => {
    const response = await api.get('/metrics/examples/queries')
    return response.data
  },

  // Get validation rules
  getValidationRules: async (): Promise<ApiResponse> => {
    const response = await api.get('/metrics/validation/rules')
    return response.data
  },
}

// Utility functions
export const apiUtils = {
  // Check if error is network-related
  isNetworkError: (error: any): boolean => {
    return !error.response && error.request
  },

  // Check if error is client-side (4xx)
  isClientError: (error: any): boolean => {
    return error.response?.status >= 400 && error.response?.status < 500
  },

  // Check if error is server-side (5xx)
  isServerError: (error: any): boolean => {
    return error.response?.status >= 500
  },

  // Extract error message from API response
  getErrorMessage: (error: any): string => {
    if (error.response?.data?.message) {
      return error.response.data.message
    }
    if (error.response?.data?.errors?.length > 0) {
      return error.response.data.errors[0]
    }
    if (error.message) {
      return error.message
    }
    return 'An unexpected error occurred'
  },

  // Format validation errors
  formatValidationErrors: (error: any): string[] => {
    if (error.response?.data?.errors) {
      return error.response.data.errors
    }
    return [apiUtils.getErrorMessage(error)]
  },
}

export default api