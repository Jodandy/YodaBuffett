/**
 * Quality Screener Filters Component
 */

import { useState } from 'react';
import type { CategoryOptions, ScreenerFilters } from '../types';

interface QualityFiltersProps {
  categories: CategoryOptions;
  filters: ScreenerFilters;
  onFiltersChange: (filters: ScreenerFilters) => void;
}

export function QualityFilters({
  categories,
  filters,
  onFiltersChange,
}: QualityFiltersProps) {
  const [isExpanded, setIsExpanded] = useState(true);

  const toggleTier = (tier: number) => {
    const current = filters.tiers || [];
    const updated = current.includes(tier)
      ? current.filter((t) => t !== tier)
      : [...current, tier];
    onFiltersChange({ ...filters, tiers: updated.length > 0 ? updated : undefined });
  };

  const toggleModel = (model: string) => {
    const current = filters.business_models || [];
    const updated = current.includes(model)
      ? current.filter((m) => m !== model)
      : [...current, model];
    onFiltersChange({ ...filters, business_models: updated.length > 0 ? updated : undefined });
  };

  const toggleSize = (size: string) => {
    const current = filters.size_categories || [];
    const updated = current.includes(size)
      ? current.filter((s) => s !== size)
      : [...current, size];
    onFiltersChange({ ...filters, size_categories: updated.length > 0 ? updated : undefined });
  };

  const toggleCash = (cash: string) => {
    const current = filters.cash_qualities || [];
    const updated = current.includes(cash)
      ? current.filter((c) => c !== cash)
      : [...current, cash];
    onFiltersChange({ ...filters, cash_qualities: updated.length > 0 ? updated : undefined });
  };

  const toggleProfitable = () => {
    onFiltersChange({ ...filters, profitable_only: !filters.profitable_only });
  };

  const clearFilters = () => {
    onFiltersChange({});
  };

  const hasFilters =
    (filters.tiers?.length ?? 0) > 0 ||
    (filters.business_models?.length ?? 0) > 0 ||
    (filters.size_categories?.length ?? 0) > 0 ||
    (filters.cash_qualities?.length ?? 0) > 0 ||
    filters.profitable_only;

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-semibold text-gray-900">Filters</h3>
        <div className="flex items-center gap-2">
          {hasFilters && (
            <button
              onClick={clearFilters}
              className="text-sm text-gray-500 hover:text-gray-700"
            >
              Clear all
            </button>
          )}
          <button
            onClick={() => setIsExpanded(!isExpanded)}
            className="text-gray-400 hover:text-gray-600"
          >
            {isExpanded ? '−' : '+'}
          </button>
        </div>
      </div>

      {isExpanded && (
        <div className="space-y-6">
          {/* Business Model */}
          <div>
            <h4 className="text-sm font-medium text-gray-700 mb-2">Business Model</h4>
            <div className="flex flex-wrap gap-2">
              {categories.business_models.map((opt) => (
                <button
                  key={opt.value}
                  onClick={() => toggleModel(opt.value)}
                  className={`px-3 py-1.5 rounded-full text-sm font-medium transition-colors ${
                    filters.business_models?.includes(opt.value)
                      ? opt.value === 'Cash Cow'
                        ? 'bg-green-500 text-white'
                        : opt.value === 'Compounder'
                        ? 'bg-blue-500 text-white'
                        : opt.value === 'Red Flag'
                        ? 'bg-red-500 text-white'
                        : opt.value === 'Caution'
                        ? 'bg-yellow-500 text-white'
                        : 'bg-gray-500 text-white'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                  title={opt.description}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </div>

          {/* Quality Tier */}
          <div>
            <h4 className="text-sm font-medium text-gray-700 mb-2">Quality Tier</h4>
            <div className="flex flex-wrap gap-2">
              {categories.tiers.map((opt) => (
                <button
                  key={opt.value}
                  onClick={() => toggleTier(Number(opt.value))}
                  className={`px-3 py-1.5 rounded-full text-sm font-medium transition-colors ${
                    filters.tiers?.includes(Number(opt.value))
                      ? Number(opt.value) <= 2
                        ? 'bg-green-500 text-white'
                        : Number(opt.value) === 3
                        ? 'bg-yellow-500 text-white'
                        : 'bg-red-500 text-white'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                  title={opt.description}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </div>

          {/* Size Category */}
          <div>
            <h4 className="text-sm font-medium text-gray-700 mb-2">Size</h4>
            <div className="flex flex-wrap gap-2">
              {categories.size_categories.map((opt) => (
                <button
                  key={opt.value}
                  onClick={() => toggleSize(opt.value)}
                  className={`px-3 py-1.5 rounded-full text-sm font-medium transition-colors ${
                    filters.size_categories?.includes(opt.value)
                      ? 'bg-indigo-500 text-white'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                  title={opt.description}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </div>

          {/* Cash Quality */}
          <div>
            <h4 className="text-sm font-medium text-gray-700 mb-2">Cash Quality (OCF/NI)</h4>
            <div className="flex flex-wrap gap-2">
              {categories.cash_qualities.map((opt) => (
                <button
                  key={opt.value}
                  onClick={() => toggleCash(opt.value)}
                  className={`px-3 py-1.5 rounded-full text-sm font-medium transition-colors ${
                    filters.cash_qualities?.includes(opt.value)
                      ? opt.value === 'Excellent' || opt.value === 'Good'
                        ? 'bg-green-500 text-white'
                        : opt.value === 'Moderate'
                        ? 'bg-yellow-500 text-white'
                        : 'bg-red-500 text-white'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                  title={opt.description}
                >
                  {opt.label}
                </button>
              ))}
            </div>
          </div>

          {/* Profitable Only */}
          <div>
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={filters.profitable_only || false}
                onChange={toggleProfitable}
                className="w-4 h-4 text-indigo-600 rounded border-gray-300 focus:ring-indigo-500"
              />
              <span className="text-sm text-gray-700">Profitable only</span>
            </label>
          </div>
        </div>
      )}
    </div>
  );
}
