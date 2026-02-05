import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { screenerApi } from '@/services/api'
import type { SavedQuery } from '@/types/screener'
import { 
  MagnifyingGlassIcon, 
  BookmarkIcon, 
  TrashIcon,
  PencilIcon,
  PlayIcon,
  ClockIcon,
  TagIcon,
  GlobeAltIcon,
  LockClosedIcon
} from '@heroicons/react/24/outline'
import { format } from 'date-fns'
import toast from 'react-hot-toast'

export default function SavedQueriesPage() {
  const [selectedTags, setSelectedTags] = useState<string[]>([])
  const [includePublic, setIncludePublic] = useState(true)
  const [searchTerm, setSearchTerm] = useState('')

  // Fetch saved queries
  const { 
    data: queries = [], 
    isLoading,
    refetch 
  } = useQuery({
    queryKey: ['saved-queries', { includePublic, tags: selectedTags }],
    queryFn: () => screenerApi.getSavedQueries({
      includePublic,
      tags: selectedTags.length > 0 ? selectedTags : undefined,
      limit: 100,
    }),
  })

  // Filter queries by search term
  const filteredQueries = queries.filter(query =>
    query.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
    query.description?.toLowerCase().includes(searchTerm.toLowerCase()) ||
    query.tags.some(tag => tag.toLowerCase().includes(searchTerm.toLowerCase()))
  )

  // Get all available tags
  const allTags = Array.from(new Set(queries.flatMap(q => q.tags))).sort()

  // Delete query
  const deleteQuery = async (queryId: string) => {
    if (!confirm('Are you sure you want to delete this query?')) return

    try {
      await screenerApi.deleteSavedQuery(queryId)
      toast.success('Query deleted successfully')
      refetch()
    } catch (error: any) {
      toast.error(`Failed to delete query: ${error.response?.data?.detail || error.message}`)
    }
  }

  // Load query for screening
  const loadQuery = (query: SavedQuery) => {
    // This would typically navigate to the screener page with the loaded query
    // For now, we'll just show a toast
    toast.success(`Loading query: ${query.name}`)
    // navigate('/', { state: { loadedQuery: query.query } })
  }

  // Load query for backtesting
  const backtestQuery = (query: SavedQuery) => {
    toast.success(`Loading query for backtest: ${query.name}`)
    // navigate('/backtest', { state: { loadedQuery: query.query } })
  }

  const handleTagToggle = (tag: string) => {
    setSelectedTags(prev =>
      prev.includes(tag)
        ? prev.filter(t => t !== tag)
        : [...prev, tag]
    )
  }

  return (
    <div className="space-y-8">
      {/* Page header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-3xl font-bold text-foreground">Saved Queries</h1>
          <p className="text-muted-foreground mt-2">
            Manage your saved screening strategies and discover community queries
          </p>
        </div>

        <div className="flex items-center space-x-3">
          <div className="text-sm text-muted-foreground">
            {filteredQueries.length} of {queries.length} queries
          </div>
        </div>
      </div>

      {/* Filters */}
      <div className="card">
        <div className="card-content py-4">
          <div className="flex flex-col sm:flex-row gap-4">
            {/* Search */}
            <div className="flex-1">
              <div className="relative">
                <MagnifyingGlassIcon className="w-4 h-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-muted-foreground" />
                <input
                  type="text"
                  placeholder="Search queries..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="input pl-10"
                />
              </div>
            </div>

            {/* Include public toggle */}
            <div className="flex items-center space-x-2">
              <input
                type="checkbox"
                id="includePublic"
                checked={includePublic}
                onChange={(e) => setIncludePublic(e.target.checked)}
                className="rounded border-border"
              />
              <label htmlFor="includePublic" className="text-sm text-muted-foreground">
                Include public queries
              </label>
            </div>
          </div>

          {/* Tag filters */}
          {allTags.length > 0 && (
            <div className="mt-4">
              <div className="text-sm text-muted-foreground mb-2">Filter by tags:</div>
              <div className="flex flex-wrap gap-2">
                {allTags.map(tag => (
                  <button
                    key={tag}
                    onClick={() => handleTagToggle(tag)}
                    className={`
                      inline-flex items-center space-x-1 px-3 py-1 rounded-full text-xs font-medium transition-colors
                      ${selectedTags.includes(tag)
                        ? 'bg-primary text-primary-foreground'
                        : 'bg-muted text-muted-foreground hover:bg-muted/80'
                      }
                    `}
                  >
                    <TagIcon className="w-3 h-3" />
                    <span>{tag}</span>
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Queries list */}
      {isLoading ? (
        <div className="card">
          <div className="card-content py-12">
            <div className="flex items-center justify-center">
              <div className="loading-pulse text-muted-foreground">Loading saved queries...</div>
            </div>
          </div>
        </div>
      ) : filteredQueries.length === 0 ? (
        <div className="card">
          <div className="card-content py-16">
            <div className="text-center">
              <BookmarkIcon className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
              <h3 className="text-lg font-semibold text-foreground mb-2">
                {queries.length === 0 ? 'No saved queries yet' : 'No queries match your filters'}
              </h3>
              <p className="text-muted-foreground mb-6 max-w-md mx-auto">
                {queries.length === 0 
                  ? 'Save your screening strategies to build a library of proven approaches.'
                  : 'Try adjusting your search or tag filters to find more queries.'
                }
              </p>
            </div>
          </div>
        </div>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-6">
          {filteredQueries.map((query) => (
            <div key={query.id} className="card hover:shadow-lg transition-shadow">
              <div className="card-header">
                <div className="flex justify-between items-start">
                  <div className="flex-1">
                    <div className="flex items-center space-x-2 mb-1">
                      <h3 className="card-title text-base">{query.name}</h3>
                      {query.isPublic ? (
                        <GlobeAltIcon className="w-4 h-4 text-blue-500" title="Public query" />
                      ) : (
                        <LockClosedIcon className="w-4 h-4 text-muted-foreground" title="Private query" />
                      )}
                    </div>
                    {query.description && (
                      <p className="text-sm text-muted-foreground line-clamp-2">
                        {query.description}
                      </p>
                    )}
                  </div>

                  <div className="flex items-center space-x-1">
                    <button
                      onClick={() => deleteQuery(query.id)}
                      className="p-1 text-muted-foreground hover:text-destructive transition-colors"
                      title="Delete query"
                    >
                      <TrashIcon className="w-4 h-4" />
                    </button>
                  </div>
                </div>
              </div>

              <div className="card-content space-y-4">
                {/* Query stats */}
                <div className="grid grid-cols-2 gap-4 text-sm">
                  <div>
                    <div className="text-muted-foreground">Conditions</div>
                    <div className="font-medium">
                      {query.query.groups.reduce((acc, g) => acc + g.conditions.length, 0)}
                    </div>
                  </div>
                  <div>
                    <div className="text-muted-foreground">Columns</div>
                    <div className="font-medium">{query.query.columns.length}</div>
                  </div>
                </div>

                {/* Tags */}
                {query.tags.length > 0 && (
                  <div>
                    <div className="flex flex-wrap gap-1">
                      {query.tags.map(tag => (
                        <span
                          key={tag}
                          className="inline-flex items-center px-2 py-1 rounded text-xs bg-muted text-muted-foreground"
                        >
                          {tag}
                        </span>
                      ))}
                    </div>
                  </div>
                )}

                {/* Metadata */}
                <div className="text-xs text-muted-foreground space-y-1">
                  <div className="flex items-center space-x-1">
                    <ClockIcon className="w-3 h-3" />
                    <span>Created {format(new Date(query.createdAt), 'MMM d, yyyy')}</span>
                  </div>
                  {query.updatedAt !== query.createdAt && (
                    <div className="flex items-center space-x-1">
                      <PencilIcon className="w-3 h-3" />
                      <span>Updated {format(new Date(query.updatedAt), 'MMM d, yyyy')}</span>
                    </div>
                  )}
                </div>

                {/* Actions */}
                <div className="flex space-x-2 pt-2 border-t border-border">
                  <button
                    onClick={() => loadQuery(query)}
                    className="btn-primary flex-1 flex items-center justify-center space-x-1"
                  >
                    <PlayIcon className="w-4 h-4" />
                    <span>Screen</span>
                  </button>
                  <button
                    onClick={() => backtestQuery(query)}
                    className="btn-outline flex-1 flex items-center justify-center space-x-1"
                  >
                    <ClockIcon className="w-4 h-4" />
                    <span>Backtest</span>
                  </button>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}