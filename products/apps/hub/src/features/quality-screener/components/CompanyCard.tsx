/**
 * Quality Screener Company Card
 */

import { Link } from 'react-router-dom';
import type { QualityCandidate } from '../types';
import {
  BUSINESS_MODEL_COLORS,
  SIZE_COLORS,
  CASH_QUALITY_COLORS,
  TIER_COLORS,
} from '../types';

interface CompanyCardProps {
  company: QualityCandidate;
}

function formatPercent(value: number | null): string {
  if (value === null) return '-';
  return `${(value * 100).toFixed(1)}%`;
}

function formatNumber(value: number | null): string {
  if (value === null) return '-';
  return value.toFixed(1);
}

function formatMarketCap(value: number): string {
  if (value >= 1e9) return `$${(value / 1e9).toFixed(1)}B`;
  return `$${(value / 1e6).toFixed(0)}M`;
}

export function CompanyCard({ company }: CompanyCardProps) {
  const tierColor = TIER_COLORS[company.tier] || 'bg-gray-400';
  const modelColor = BUSINESS_MODEL_COLORS[company.business_model] || BUSINESS_MODEL_COLORS['Unknown'];
  const sizeColor = SIZE_COLORS[company.size_category] || 'bg-gray-100 text-gray-600';
  const cashColor = CASH_QUALITY_COLORS[company.cash_quality] || 'text-gray-400';

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4 hover:shadow-md transition-shadow">
      {/* Header */}
      <div className="flex items-start justify-between mb-3">
        <div>
          <Link
            to={`/company/${company.ticker}`}
            className="text-lg font-semibold text-gray-900 hover:text-indigo-600"
          >
            {company.ticker}
          </Link>
          <p className="text-sm text-gray-500 truncate max-w-[200px]">
            {company.company_name}
          </p>
        </div>

        {/* Tier Badge */}
        <div className="flex items-center gap-1">
          <div className={`w-6 h-6 rounded-full ${tierColor} flex items-center justify-center`}>
            <span className="text-white text-xs font-bold">{company.tier}</span>
          </div>
        </div>
      </div>

      {/* Badges */}
      <div className="flex flex-wrap gap-1.5 mb-3">
        <span className={`px-2 py-0.5 rounded-full text-xs font-medium border ${modelColor}`}>
          {company.business_model}
        </span>
        <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${sizeColor}`}>
          {company.size_category}
        </span>
        {company.net_cash && (
          <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-emerald-100 text-emerald-700">
            Net Cash
          </span>
        )}
        {!company.is_profitable && (
          <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-700">
            Unprofitable
          </span>
        )}
      </div>

      {/* Key Metrics Grid */}
      <div className="grid grid-cols-3 gap-2 text-sm mb-3">
        <div>
          <span className="text-gray-500 text-xs">Market Cap</span>
          <p className="font-medium">{formatMarketCap(company.market_cap)}</p>
        </div>
        <div>
          <span className="text-gray-500 text-xs">ROIC</span>
          <p className="font-medium">{formatPercent(company.roic)}</p>
        </div>
        <div>
          <span className="text-gray-500 text-xs">P/E</span>
          <p className="font-medium">{formatNumber(company.pe_ratio)}</p>
        </div>
      </div>

      {/* Cash Conversion */}
      <div className="flex items-center justify-between text-sm mb-3 py-2 border-t border-gray-100">
        <div>
          <span className="text-gray-500 text-xs">Cash Quality</span>
          <p className={`font-medium ${cashColor}`}>
            {company.cash_quality}
            {company.ocf_to_ni && (
              <span className="text-gray-400 ml-1">
                ({formatPercent(company.ocf_to_ni)})
              </span>
            )}
          </p>
        </div>
        <div className="text-right">
          <span className="text-gray-500 text-xs">FCF Yield</span>
          <p className="font-medium">{formatPercent(company.fcf_yield)}</p>
        </div>
      </div>

      {/* Reasons/Concerns Preview */}
      {(company.reasons.length > 0 || company.concerns.length > 0) && (
        <div className="border-t border-gray-100 pt-2 text-xs">
          {company.reasons.length > 0 && (
            <p className="text-green-600 truncate">
              + {company.reasons[0]}
            </p>
          )}
          {company.concerns.length > 0 && (
            <p className="text-red-500 truncate">
              - {company.concerns[0]}
            </p>
          )}
        </div>
      )}
    </div>
  );
}

// Expanded card with more details
export function CompanyCardExpanded({ company }: CompanyCardProps) {
  const tierColor = TIER_COLORS[company.tier] || 'bg-gray-400';
  const modelColor = BUSINESS_MODEL_COLORS[company.business_model] || BUSINESS_MODEL_COLORS['Unknown'];
  const cashColor = CASH_QUALITY_COLORS[company.cash_quality] || 'text-gray-400';

  return (
    <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-5 hover:shadow-md transition-shadow">
      {/* Header */}
      <div className="flex items-start justify-between mb-4">
        <div>
          <div className="flex items-center gap-2">
            <Link
              to={`/company/${company.ticker}`}
              className="text-xl font-semibold text-gray-900 hover:text-indigo-600"
            >
              {company.ticker}
            </Link>
            <div className={`w-7 h-7 rounded-full ${tierColor} flex items-center justify-center`}>
              <span className="text-white text-sm font-bold">{company.tier}</span>
            </div>
          </div>
          <p className="text-sm text-gray-500">{company.company_name}</p>
        </div>

        <div className="text-right">
          <p className="text-2xl font-bold text-gray-900">{company.quality_score}</p>
          <p className="text-xs text-gray-500">Quality Score</p>
        </div>
      </div>

      {/* Badges */}
      <div className="flex flex-wrap gap-2 mb-4">
        <span className={`px-3 py-1 rounded-full text-sm font-medium border ${modelColor}`}>
          {company.business_model}
        </span>
        <span className="px-3 py-1 rounded-full text-sm font-medium bg-gray-100 text-gray-700">
          {company.size_category} Cap
        </span>
        <span className={`px-3 py-1 rounded-full text-sm font-medium ${cashColor} bg-gray-50`}>
          {company.cash_quality} Cash
        </span>
        {company.net_cash && (
          <span className="px-3 py-1 rounded-full text-sm font-medium bg-emerald-100 text-emerald-700">
            Net Cash
          </span>
        )}
      </div>

      {/* Business Model Reason */}
      <p className="text-sm text-gray-600 mb-4 italic">
        "{company.business_model_reason}"
      </p>

      {/* Metrics Grid */}
      <div className="grid grid-cols-4 gap-4 mb-4">
        <MetricBox label="Market Cap" value={formatMarketCap(company.market_cap)} />
        <MetricBox label="ROIC" value={formatPercent(company.roic)} />
        <MetricBox label="ROE" value={formatPercent(company.roe)} />
        <MetricBox label="Growth" value={formatPercent(company.revenue_cagr)} />
        <MetricBox label="Gross Margin" value={formatPercent(company.gross_margin)} />
        <MetricBox label="Op Margin" value={formatPercent(company.operating_margin)} />
        <MetricBox label="Net Margin" value={formatPercent(company.net_margin)} />
        <MetricBox label="FCF Yield" value={formatPercent(company.fcf_yield)} />
        <MetricBox label="OCF/NI" value={formatPercent(company.ocf_to_ni)} />
        <MetricBox label="CapEx/Rev" value={formatPercent(company.capex_to_revenue)} />
        <MetricBox label="P/E" value={formatNumber(company.pe_ratio)} />
        <MetricBox label="EV/EBITDA" value={formatNumber(company.ev_to_ebitda)} />
      </div>

      {/* Reasons & Concerns */}
      <div className="grid grid-cols-2 gap-4 border-t border-gray-100 pt-4">
        <div>
          <h4 className="text-sm font-medium text-green-700 mb-2">Why it looks quality</h4>
          <ul className="text-xs text-gray-600 space-y-1">
            {company.reasons.slice(0, 4).map((r, i) => (
              <li key={i} className="flex items-start gap-1">
                <span className="text-green-500">+</span>
                <span>{r}</span>
              </li>
            ))}
            {company.reasons.length === 0 && (
              <li className="text-gray-400">No notable positives</li>
            )}
          </ul>
        </div>
        <div>
          <h4 className="text-sm font-medium text-red-700 mb-2">Concerns to investigate</h4>
          <ul className="text-xs text-gray-600 space-y-1">
            {company.concerns.slice(0, 4).map((c, i) => (
              <li key={i} className="flex items-start gap-1">
                <span className="text-red-500">-</span>
                <span>{c}</span>
              </li>
            ))}
            {company.concerns.length === 0 && (
              <li className="text-gray-400">No notable concerns</li>
            )}
          </ul>
        </div>
      </div>
    </div>
  );
}

function MetricBox({ label, value }: { label: string; value: string }) {
  return (
    <div className="text-center">
      <p className="text-xs text-gray-500">{label}</p>
      <p className="text-sm font-medium text-gray-900">{value}</p>
    </div>
  );
}
