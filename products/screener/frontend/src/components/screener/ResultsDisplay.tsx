import { useState, useMemo } from 'react'
import type { ScreenerResponse, ScreenerResult, ColumnSort, TableFilter } from '@/types/screener'
import { 
  ChevronUpDownIcon,
  ChevronUpIcon,
  ChevronDownIcon,
  FunnelIcon,
  ArrowTopRightOnSquareIcon
} from '@heroicons/react/24/outline'
import toast from 'react-hot-toast'

interface ResultsDisplayProps {
  response: ScreenerResponse
  onExport: (format: 'csv' | 'xlsx' | 'json') => void
}

export default function ResultsDisplay({ response, onExport }: ResultsDisplayProps) {
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize, setPageSize] = useState(50)
  const [sortBy, setSortBy] = useState<ColumnSort | null>(null)
  const [filters, setFilters] = useState<TableFilter[]>([])
  const [showFilters, setShowFilters] = useState(false)

  // Get all column keys from results and query
  const allColumns = useMemo(() => {
    const columnsSet = new Set([
      'company.symbol',
      'company.name',
      'company.market_cap',
      ...response.query.columns
    ])
    
    if (response.query.includeForwardReturns) {
      response.query.includeForwardReturns.forEach(period => {
        columnsSet.add(`forwardReturns.${period}`)
      })
    }
    
    return Array.from(columnsSet)
  }, [response])

  // Sort and filter results
  const processedResults = useMemo(() => {
    let filtered = [...response.results]

    // Apply filters
    filters.forEach(filter => {
      filtered = filtered.filter(result => {
        const value = getNestedValue(result, filter.column)
        if (value === null || value === undefined) return false
        
        switch (filter.operator) {
          case '>':
            return Number(value) > Number(filter.value)
          case '<':
            return Number(value) < Number(filter.value)
          case '=':
            return String(value).toLowerCase().includes(String(filter.value).toLowerCase())
          default:
            return true
        }
      })
    })

    // Apply sorting
    if (sortBy) {
      filtered.sort((a, b) => {
        const aValue = getNestedValue(a, sortBy.column)
        const bValue = getNestedValue(b, sortBy.column)
        
        if (aValue === null || aValue === undefined) return 1
        if (bValue === null || bValue === undefined) return -1
        
        const comparison = typeof aValue === 'number' && typeof bValue === 'number'
          ? aValue - bValue
          : String(aValue).localeCompare(String(bValue))
        
        return sortBy.direction === 'desc' ? -comparison : comparison
      })
    }

    return filtered
  }, [response.results, sortBy, filters])

  // Pagination
  const paginatedResults = useMemo(() => {
    const start = (currentPage - 1) * pageSize
    return processedResults.slice(start, start + pageSize)
  }, [processedResults, currentPage, pageSize])

  const totalPages = Math.ceil(processedResults.length / pageSize)

  // Helper to get nested object values
  const getNestedValue = (obj: any, path: string): any => {
    return path.split('.').reduce((current, key) => current?.[key], obj)
  }

  // Helper to format values for display
  const formatValue = (value: any, column: string): string => {
    if (value === null || value === undefined) return '-'
    
    if (column.includes('forwardReturns')) {
      const num = Number(value)
      return isNaN(num) ? '-' : `${(num * 100).toFixed(2)}%`
    }
    
    if (column.includes('market_cap')) {
      const num = Number(value)
      return isNaN(num) ? '-' : `$${(num / 1e9).toFixed(2)}B`
    }
    
    if (typeof value === 'number') {
      return value.toFixed(2)
    }
    
    return String(value)
  }

  // Helper to get column display name
  const getColumnName = (column: string): string => {
    if (column === 'company.symbol') return 'Symbol'
    if (column === 'company.name') return 'Company'
    if (column === 'company.market_cap') return 'Market Cap'
    if (column.startsWith('forwardReturns.')) {
      const period = column.split('.')[1]
      return `${period} Return`
    }
    
    // Convert snake_case to Title Case
    return column
      .split('_')
      .map(word => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ')
  }

  // Handle sorting
  const handleSort = (column: string) => {
    setSortBy(prev => {
      if (!prev || prev.column !== column) {
        return { column, direction: 'desc' }
      }
      if (prev.direction === 'desc') {
        return { column, direction: 'asc' }
      }
      return null // Remove sorting
    })
  }

  // Add filter
  const addFilter = (column: string, operator: string, value: string) => {
    setFilters(prev => [
      ...prev.filter(f => f.column !== column),
      { column, operator, value }
    ])
  }

  // Remove filter
  const removeFilter = (column: string) => {
    setFilters(prev => prev.filter(f => f.column !== column))
  }

  // Get performance color class
  const getPerformanceColor = (value: number): string => {
    if (value > 0) return 'performance-positive'
    if (value < 0) return 'performance-negative'
    return 'performance-neutral'
  }

  return (
    <div className="space-y-4">
      {/* Controls */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div className="flex items-center space-x-4">
          <div className="flex items-center space-x-2">
            <span className="text-sm text-muted-foreground">Show:</span>
            <select
              value={pageSize}
              onChange={(e) => {
                setPageSize(Number(e.target.value))
                setCurrentPage(1)
              }}
              className="select w-20"
            >
              <option value={25}>25</option>
              <option value={50}>50</option>
              <option value={100}>100</option>
            </select>
          </div>
          
          <button
            onClick={() => setShowFilters(!showFilters)}
            className={`btn-outline flex items-center space-x-2 ${showFilters ? 'bg-muted' : ''}`}
          >
            <FunnelIcon className="w-4 h-4" />
            <span>Filters</span>
            {filters.length > 0 && (
              <span className="bg-primary text-primary-foreground text-xs rounded-full px-2 py-1">
                {filters.length}
              </span>
            )}
          </button>
        </div>

        <div className="text-sm text-muted-foreground">
          Showing {(currentPage - 1) * pageSize + 1} to {Math.min(currentPage * pageSize, processedResults.length)} of {processedResults.length} results
        </div>
      </div>

      {/* Filters panel */}
      {showFilters && (
        <div className="card">
          <div className="card-content py-3">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              {allColumns.slice(0, 6).map(column => (
                <div key={column} className="flex items-center space-x-2">
                  <span className="text-sm text-muted-foreground min-w-0 flex-1 truncate">
                    {getColumnName(column)}
                  </span>
                  <select 
                    className="select w-16 text-xs"
                    onChange={(e) => {
                      if (e.target.value === '') {
                        removeFilter(column)
                      }
                    }}
                  >
                    <option value="">All</option>
                    <option value=">">{'>'}</option>
                    <option value="<">{'<'}</option>
                    <option value="=">=</option>
                  </select>
                  <input
                    type="text"
                    placeholder="Value"
                    className="input text-xs w-24"
                    onBlur={(e) => {
                      if (e.target.value) {
                        addFilter(column, '>', e.target.value)
                      } else {
                        removeFilter(column)
                      }
                    }}
                  />
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Results table */}
      <div className="card">
        <div className="overflow-x-auto">
          <table className="results-table">
            <thead className="table-header">
              <tr>
                {allColumns.map(column => (
                  <th 
                    key={column}
                    className="table-head cursor-pointer hover:bg-muted/80 transition-colors"
                    onClick={() => handleSort(column)}
                  >
                    <div className="flex items-center space-x-1">
                      <span className="truncate">{getColumnName(column)}</span>
                      {sortBy?.column === column ? (
                        sortBy.direction === 'desc' ? (
                          <ChevronDownIcon className="w-4 h-4 text-primary" />
                        ) : (
                          <ChevronUpIcon className="w-4 h-4 text-primary" />
                        )
                      ) : (
                        <ChevronUpDownIcon className="w-4 h-4 text-muted-foreground" />
                      )}
                    </div>
                  </th>
                ))}
                <th className="table-head">Actions</th>
              </tr>
            </thead>
            <tbody className="table-body">
              {paginatedResults.map((result, index) => (
                <tr key={result.company.id || index} className="table-row">
                  {allColumns.map(column => {
                    const value = getNestedValue(result, column)
                    const formattedValue = formatValue(value, column)
                    
                    return (
                      <td key={column} className="table-cell">
                        {column.includes('forwardReturns') ? (
                          <span className={getPerformanceColor(Number(value) || 0)}>
                            {formattedValue}
                          </span>
                        ) : column === 'company.symbol' ? (
                          <span className="font-mono font-medium text-primary">
                            {formattedValue}
                          </span>
                        ) : (
                          <span>{formattedValue}</span>
                        )}
                      </td>
                    )
                  })}
                  <td className="table-cell">
                    <button
                      onClick={() => {
                        // Open company details or external link
                        const symbol = result.company.symbol
                        toast.success(`Opening details for ${symbol}`)
                      }}
                      className="p-1 text-muted-foreground hover:text-primary transition-colors"
                      title={`View details for ${result.company.symbol}`}
                    >
                      <ArrowTopRightOnSquareIcon className="w-4 h-4" />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          
          {paginatedResults.length === 0 && (
            <div className="text-center py-12 text-muted-foreground">
              No results match your current filters
            </div>
          )}
        </div>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <button
              onClick={() => setCurrentPage(1)}
              disabled={currentPage === 1}
              className="btn-outline disabled:opacity-50"
            >
              First
            </button>
            <button
              onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
              disabled={currentPage === 1}
              className="btn-outline disabled:opacity-50"
            >
              Previous
            </button>
          </div>

          <div className="flex items-center space-x-1">
            {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
              const page = Math.max(1, Math.min(totalPages - 4, currentPage - 2)) + i
              return (
                <button
                  key={page}
                  onClick={() => setCurrentPage(page)}
                  className={`
                    px-3 py-1 rounded text-sm transition-colors
                    ${page === currentPage 
                      ? 'bg-primary text-primary-foreground' 
                      : 'text-muted-foreground hover:text-foreground hover:bg-muted'
                    }
                  `}
                >
                  {page}
                </button>
              )
            })}
          </div>

          <div className="flex items-center space-x-2">
            <button
              onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
              disabled={currentPage === totalPages}
              className="btn-outline disabled:opacity-50"
            >
              Next
            </button>
            <button
              onClick={() => setCurrentPage(totalPages)}
              disabled={currentPage === totalPages}
              className="btn-outline disabled:opacity-50"
            >
              Last
            </button>
          </div>
        </div>
      )}

      {/* Summary stats */}
      {response.summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="card">
            <div className="card-content py-3">
              <div className="text-center">
                <div className="text-2xl font-bold text-foreground">
                  {response.summary.count}
                </div>
                <div className="text-sm text-muted-foreground">Total Matches</div>
              </div>
            </div>
          </div>
          
          {response.summary.winRates && Object.entries(response.summary.winRates).map(([period, rate]) => (
            <div key={period} className="card">
              <div className="card-content py-3">
                <div className="text-center">
                  <div className={`text-2xl font-bold ${getPerformanceColor(rate)}`}>
                    {(rate * 100).toFixed(1)}%
                  </div>
                  <div className="text-sm text-muted-foreground">{period} Win Rate</div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}