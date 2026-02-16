/**
 * Quality Screener Page
 * Standalone screener with category filtering
 */

import { useState, useMemo } from 'react';
import { useCategories, useCompanies } from '../hooks/useQualityScreener';
import { QualityFilters } from '../components/QualityFilters';
import { CompanyCard, CompanyCardExpanded } from '../components/CompanyCard';
import { SaveToWatchlistModal } from '../../watchlist';
import type { ScreenerFilters } from '../types';

type SortField =
  | 'quality_score'
  | 'tier'
  | 'market_cap'
  | 'roic'
  | 'fcf_yield'
  | 'ocf_to_ni'
  | 'company_name';

export default function QualityScreenerPage() {
  const [filters, setFilters] = useState<ScreenerFilters>({});
  const [sortField, setSortField] = useState<SortField>('tier');
  const [sortAsc, setSortAsc] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [viewMode, setViewMode] = useState<'grid' | 'expanded'>('grid');
  const [showWatchlistModal, setShowWatchlistModal] = useState(false);

  const { data: categories, isLoading: categoriesLoading } = useCategories();
  const { data: response, isLoading: companiesLoading, error } = useCompanies(filters);

  // Sort and filter companies
  const sortedCompanies = useMemo(() => {
    if (!response?.candidates) return [];

    let filtered = response.candidates;

    // Apply search filter
    if (searchTerm) {
      const term = searchTerm.toLowerCase();
      filtered = filtered.filter(
        (c) =>
          c.ticker.toLowerCase().includes(term) ||
          c.company_name.toLowerCase().includes(term)
      );
    }

    // Sort
    return [...filtered].sort((a, b) => {
      let aVal: number | string | null = null;
      let bVal: number | string | null = null;

      switch (sortField) {
        case 'quality_score':
          aVal = a.quality_score;
          bVal = b.quality_score;
          break;
        case 'tier':
          aVal = a.tier;
          bVal = b.tier;
          break;
        case 'market_cap':
          aVal = a.market_cap;
          bVal = b.market_cap;
          break;
        case 'roic':
          aVal = a.roic ?? -999;
          bVal = b.roic ?? -999;
          break;
        case 'fcf_yield':
          aVal = a.fcf_yield ?? -999;
          bVal = b.fcf_yield ?? -999;
          break;
        case 'ocf_to_ni':
          aVal = a.ocf_to_ni ?? -999;
          bVal = b.ocf_to_ni ?? -999;
          break;
        case 'company_name':
          aVal = a.company_name;
          bVal = b.company_name;
          break;
      }

      if (aVal === null || bVal === null) return 0;

      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortAsc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
      }

      return sortAsc
        ? (aVal as number) - (bVal as number)
        : (bVal as number) - (aVal as number);
    });
  }, [response?.candidates, searchTerm, sortField, sortAsc]);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortAsc(!sortAsc);
    } else {
      setSortField(field);
      setSortAsc(field === 'tier'); // Ascending for tier, descending for others
    }
  };

  // Generate a source description based on active filters
  const getFilterSource = useMemo(() => {
    const parts: string[] = ['Quality Screener'];
    if (filters.tiers?.length) parts.push(`Tier ${filters.tiers.join(',')}`);
    if (filters.business_models?.length) parts.push(filters.business_models.join(', '));
    if (filters.cash_qualities?.length) parts.push(filters.cash_qualities.join(', '));
    if (searchTerm) parts.push(`search: "${searchTerm}"`);
    return parts.join(' | ');
  }, [filters, searchTerm]);

  if (error) {
    return (
      <div className="p-8 text-center">
        <p className="text-red-500">Error loading screener data</p>
        <p className="text-gray-500 text-sm mt-2">{String(error)}</p>
      </div>
    );
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Quality Screener</h1>
        <p className="text-gray-500 mt-1">
          Filter companies by business model, quality tier, and cash conversion
        </p>
      </div>

      {/* Summary Stats */}
      {response?.summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <StatCard
            label="Total Companies"
            value={response.summary.total_companies}
          />
          <StatCard
            label="Cash Cows"
            value={response.summary.by_business_model['Cash Cow'] || 0}
            color="text-green-600"
          />
          <StatCard
            label="Compounders"
            value={response.summary.by_business_model['Compounder'] || 0}
            color="text-blue-600"
          />
          <StatCard
            label="Red Flags"
            value={response.summary.by_business_model['Red Flag'] || 0}
            color="text-red-600"
          />
        </div>
      )}

      <div className="flex flex-col lg:flex-row gap-6">
        {/* Filters Sidebar */}
        <div className="lg:w-72 flex-shrink-0">
          {categoriesLoading ? (
            <div className="bg-white rounded-lg p-4 text-center text-gray-500">
              Loading filters...
            </div>
          ) : categories ? (
            <QualityFilters
              categories={categories}
              filters={filters}
              onFiltersChange={setFilters}
            />
          ) : null}
        </div>

        {/* Main Content */}
        <div className="flex-1">
          {/* Toolbar */}
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4 mb-4">
            <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between">
              {/* Search */}
              <input
                type="text"
                placeholder="Search by ticker or company name..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full sm:w-64 px-3 py-2 border border-gray-300 rounded-md text-sm focus:ring-indigo-500 focus:border-indigo-500"
              />

              <div className="flex items-center gap-4">
                {/* Sort */}
                <div className="flex items-center gap-2">
                  <label className="text-sm text-gray-500">Sort:</label>
                  <select
                    value={sortField}
                    onChange={(e) => handleSort(e.target.value as SortField)}
                    className="text-sm border-gray-300 rounded-md focus:ring-indigo-500 focus:border-indigo-500"
                  >
                    <option value="tier">Tier</option>
                    <option value="quality_score">Quality Score</option>
                    <option value="market_cap">Market Cap</option>
                    <option value="roic">ROIC</option>
                    <option value="fcf_yield">FCF Yield</option>
                    <option value="ocf_to_ni">OCF/NI</option>
                    <option value="company_name">Name</option>
                  </select>
                  <button
                    onClick={() => setSortAsc(!sortAsc)}
                    className="text-gray-500 hover:text-gray-700"
                    title={sortAsc ? 'Ascending' : 'Descending'}
                  >
                    {sortAsc ? '↑' : '↓'}
                  </button>
                </div>

                {/* View Toggle */}
                <div className="flex border border-gray-300 rounded-md overflow-hidden">
                  <button
                    onClick={() => setViewMode('grid')}
                    className={`px-3 py-1.5 text-sm ${
                      viewMode === 'grid'
                        ? 'bg-indigo-500 text-white'
                        : 'bg-white text-gray-600 hover:bg-gray-50'
                    }`}
                  >
                    Grid
                  </button>
                  <button
                    onClick={() => setViewMode('expanded')}
                    className={`px-3 py-1.5 text-sm ${
                      viewMode === 'expanded'
                        ? 'bg-indigo-500 text-white'
                        : 'bg-white text-gray-600 hover:bg-gray-50'
                    }`}
                  >
                    Expanded
                  </button>
                </div>

                {/* Save to Watchlist */}
                <button
                  onClick={() => setShowWatchlistModal(true)}
                  disabled={sortedCompanies.length === 0}
                  className="px-3 py-1.5 text-sm font-medium text-white bg-green-600 rounded-md hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Save to Watchlist
                </button>
              </div>
            </div>

            {/* Results count */}
            <p className="text-sm text-gray-500 mt-3">
              Showing {sortedCompanies.length} companies
              {response?.score_date && (
                <span className="ml-2">
                  (as of {new Date(response.score_date).toLocaleDateString()})
                </span>
              )}
            </p>
          </div>

          {/* Companies Grid */}
          {companiesLoading ? (
            <div className="text-center py-12 text-gray-500">
              Loading companies...
            </div>
          ) : sortedCompanies.length === 0 ? (
            <div className="text-center py-12 text-gray-500">
              No companies match your filters
            </div>
          ) : viewMode === 'grid' ? (
            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
              {sortedCompanies.map((company) => (
                <CompanyCard key={company.ticker} company={company} />
              ))}
            </div>
          ) : (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              {sortedCompanies.map((company) => (
                <CompanyCardExpanded key={company.ticker} company={company} />
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Watchlist Modal */}
      <SaveToWatchlistModal
        isOpen={showWatchlistModal}
        onClose={() => setShowWatchlistModal(false)}
        tickers={sortedCompanies.map((c) => c.ticker)}
        source={getFilterSource}
      />
    </div>
  );
}

function StatCard({
  label,
  value,
  color = 'text-gray-900',
}: {
  label: string;
  value: number;
  color?: string;
}) {
  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
      <p className="text-sm text-gray-500">{label}</p>
      <p className={`text-2xl font-bold ${color}`}>{value}</p>
    </div>
  );
}
