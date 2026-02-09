/**
 * CompanyDetailPage
 * Main company detail page with tabs for different sections
 */

import { useState } from 'react'
import { useParams } from 'react-router-dom'
import { ExclamationTriangleIcon, DocumentTextIcon, CalendarIcon } from '@heroicons/react/24/outline'
import { useCompanyDetail, usePriceHistory, useFinancials, useDocuments, useCalendarEvents } from '../hooks/useCompanyDetail'
import { CompanyHeader } from '../components/CompanyHeader'
import { DimensionGrid } from '../components/DimensionGrid'
import { PriceChart } from '../components/PriceChart'
import { MetricGrid } from '../components/MetricCard'
import { CompanyTabs } from '../components/CompanyTabs'
import type { CompanyTab, PriceTimeRange } from '../types'

/**
 * Format period label with statement type indicator
 * e.g., "2024-12-31 (FY)" for annual, "2024-09-30 (Q3)" for quarterly
 */
function formatPeriodLabel(periodDate: string, statementType: string, fiscalQuarter?: number | null): string {
  if (statementType === 'annual') {
    return `${periodDate} (FY)`
  }
  // For quarterly, use fiscal quarter if available, otherwise derive from month
  if (fiscalQuarter) {
    return `${periodDate} (Q${fiscalQuarter})`
  }
  // Fallback: derive quarter from month
  const month = new Date(periodDate).getMonth() + 1
  const quarter = Math.ceil(month / 3)
  return `${periodDate} (Q${quarter})`
}

export default function CompanyDetailPage() {
  const { symbol } = useParams<{ symbol: string }>()
  const [activeTab, setActiveTab] = useState<CompanyTab>('overview')
  const [priceRange, setPriceRange] = useState<PriceTimeRange>('1Y')

  // Fetch company data
  const { data: company, isLoading } = useCompanyDetail(symbol || '')
  const { data: priceHistory, isLoading: pricesLoading } = usePriceHistory(symbol || '', priceRange)
  const { data: financials, isLoading: financialsLoading } = useFinancials(symbol)
  const { data: documentsData } = useDocuments(symbol)
  const { data: eventsData } = useCalendarEvents(symbol)

  // Extract arrays from response objects
  const documents = documentsData?.documents ?? []
  const events = eventsData?.events ?? []

  // Determine if we have basic data (price history gives us company name)
  const hasBasicData = priceHistory?.prices && priceHistory.prices.length > 0
  const companyName = company?.companyName || priceHistory?.companyName || symbol

  // Loading state - wait for at least one data source
  if (isLoading && pricesLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
      </div>
    )
  }

  // If no data at all, show error
  if (!company && !hasBasicData && !pricesLoading) {
    return (
      <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-6 text-center">
        <ExclamationTriangleIcon className="w-12 h-12 text-red-400 mx-auto mb-4" />
        <p className="text-red-400 font-medium">Company not found</p>
        <p className="text-sm text-muted-foreground mt-2">
          Could not find company with symbol: {symbol}
        </p>
      </div>
    )
  }

  // Basic view mode - no Fat Pitch data but have other data
  const isBasicMode = !company

  // Prepare key metrics from dimension scores (only if we have Fat Pitch data)
  const keyMetrics = company ? [
    {
      label: 'Quality Score',
      value: company.qualityScore,
      format: 'number' as const,
    },
    {
      label: 'Cheapness',
      value: company.cheapnessScore,
      format: 'number' as const,
    },
    {
      label: 'Profitability',
      value: company.dimensionScores?.profitability,
      format: 'number' as const,
    },
    {
      label: 'Growth',
      value: company.dimensionScores?.growth,
      format: 'number' as const,
    },
  ] : []


  return (
    <div className="space-y-6">
      {/* Header */}
      {company ? (
        <CompanyHeader company={company} />
      ) : (
        // Basic header when no Fat Pitch data
        <div className="flex items-start justify-between">
          <div>
            <h1 className="text-3xl font-bold text-foreground">{companyName}</h1>
            <p className="text-lg text-muted-foreground">{symbol}</p>
            {isBasicMode && (
              <p className="text-sm text-yellow-500 mt-2">
                Limited data available - company not in screener rankings
              </p>
            )}
          </div>
          {priceHistory?.latestPrice && (
            <div className="text-right">
              <p className="text-2xl font-bold text-foreground">
                {priceHistory.latestPrice.toFixed(2)} SEK
              </p>
              {priceHistory.priceChangePercent !== undefined && (
                <p className={`text-sm ${priceHistory.priceChangePercent >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                  {priceHistory.priceChangePercent >= 0 ? '+' : ''}{priceHistory.priceChangePercent.toFixed(2)}%
                </p>
              )}
            </div>
          )}
        </div>
      )}

      {/* Tabs */}
      <CompanyTabs
        activeTab={activeTab}
        onTabChange={setActiveTab}
        documentCount={documents.length}
        eventCount={events.length}
      />

      {/* Tab Content */}
      <div className="mt-6">
        {activeTab === 'overview' && (
          <div className="space-y-8">
            {/* Price Chart */}
            <PriceChart
              prices={priceHistory?.prices || []}
              currentRange={priceRange}
              onRangeChange={setPriceRange}
              loading={pricesLoading}
            />

            {/* Key Metrics - only show if we have Fat Pitch data */}
            {keyMetrics.length > 0 && (
              <div>
                <h3 className="text-lg font-semibold text-foreground mb-4">Key Metrics</h3>
                <MetricGrid metrics={keyMetrics} columns={4} />
              </div>
            )}

            {/* Dimension Scores - only show if we have Fat Pitch data */}
            {company && company.dimensionScores && (
              <div className="bg-card border border-border rounded-lg p-6">
                <h3 className="text-lg font-semibold text-foreground mb-2">Dimension Scores</h3>
                <p className="text-sm text-muted-foreground mb-6">Click on any dimension to see detailed breakdown</p>
                <DimensionGrid
                  dimensionScores={company.dimensionScores}
                  dimensionContributions={company.dimensionContributions}
                  showContributions
                  companyId={company.companyId}
                />
              </div>
            )}

            {/* Pitch Summary - only show if we have Fat Pitch data */}
            {company?.pitchSummary && (
              <div className="bg-card border border-border rounded-lg p-6">
                <h3 className="text-lg font-semibold text-foreground mb-2">Summary</h3>
                <p className="text-muted-foreground">{company.pitchSummary}</p>
              </div>
            )}

            {/* Basic mode notice */}
            {isBasicMode && (
              <div className="bg-yellow-500/10 border border-yellow-500/20 rounded-lg p-6">
                <h3 className="text-lg font-semibold text-yellow-500 mb-2">Limited Data</h3>
                <p className="text-muted-foreground">
                  This company is not currently in the Fat Pitch screener rankings.
                  Scoring data, dimension analysis, and quality metrics are not available.
                  Price data, financials, documents, and events may still be available.
                </p>
              </div>
            )}
          </div>
        )}

        {activeTab === 'financials' && (
          <div className="space-y-6">
            {financialsLoading ? (
              <div className="flex items-center justify-center h-64">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
              </div>
            ) : !financials ? (
              <div className="bg-card border border-border rounded-lg p-6 text-center">
                <DocumentTextIcon className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
                <h3 className="text-lg font-semibold text-foreground mb-2">No Financial Data</h3>
                <p className="text-muted-foreground">
                  No financial statements found for this company.
                </p>
              </div>
            ) : (
              <>
                {/* Income Statements */}
                {financials.incomeStatements && financials.incomeStatements.length > 0 && (
                  <div className="bg-card border border-border rounded-lg p-6">
                    <h3 className="text-lg font-semibold text-foreground mb-4">Income Statement</h3>
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-border">
                            <th className="text-left py-2 text-muted-foreground font-medium">Metric</th>
                            {financials.incomeStatements.slice(0, 5).map((stmt, i) => (
                              <th key={i} className="text-right py-2 text-muted-foreground font-medium">
                                {formatPeriodLabel(stmt.periodDate, stmt.statementType, stmt.fiscalQuarter)}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          <tr className="border-b border-border/50">
                            <td className="py-2 text-foreground">Revenue</td>
                            {financials.incomeStatements.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.totalRevenue ? `${(stmt.totalRevenue / 1e6).toFixed(0)}M` : '-'}
                              </td>
                            ))}
                          </tr>
                          <tr className="border-b border-border/50">
                            <td className="py-2 text-foreground">Gross Profit</td>
                            {financials.incomeStatements.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.grossProfit ? `${(stmt.grossProfit / 1e6).toFixed(0)}M` : '-'}
                              </td>
                            ))}
                          </tr>
                          <tr className="border-b border-border/50">
                            <td className="py-2 text-foreground">Operating Income</td>
                            {financials.incomeStatements.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.operatingIncome ? `${(stmt.operatingIncome / 1e6).toFixed(0)}M` : '-'}
                              </td>
                            ))}
                          </tr>
                          <tr className="border-b border-border/50">
                            <td className="py-2 text-foreground">Net Income</td>
                            {financials.incomeStatements.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.netIncome ? `${(stmt.netIncome / 1e6).toFixed(0)}M` : '-'}
                              </td>
                            ))}
                          </tr>
                          <tr>
                            <td className="py-2 text-foreground">EPS</td>
                            {financials.incomeStatements.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.basicEps ?? stmt.dilutedEps ?? '-'}
                              </td>
                            ))}
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}

                {/* Balance Sheets */}
                {financials.balanceSheets && financials.balanceSheets.length > 0 && (
                  <div className="bg-card border border-border rounded-lg p-6">
                    <h3 className="text-lg font-semibold text-foreground mb-4">Balance Sheet</h3>
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-border">
                            <th className="text-left py-2 text-muted-foreground font-medium">Metric</th>
                            {financials.balanceSheets.slice(0, 5).map((stmt, i) => (
                              <th key={i} className="text-right py-2 text-muted-foreground font-medium">
                                {formatPeriodLabel(stmt.periodDate, stmt.statementType)}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          <tr className="border-b border-border/50">
                            <td className="py-2 text-foreground">Total Assets</td>
                            {financials.balanceSheets.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.totalAssets ? `${(stmt.totalAssets / 1e6).toFixed(0)}M` : '-'}
                              </td>
                            ))}
                          </tr>
                          <tr className="border-b border-border/50">
                            <td className="py-2 text-foreground">Cash & Equivalents</td>
                            {financials.balanceSheets.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.cashAndEquivalents ? `${(stmt.cashAndEquivalents / 1e6).toFixed(0)}M` : '-'}
                              </td>
                            ))}
                          </tr>
                          <tr className="border-b border-border/50">
                            <td className="py-2 text-foreground">Total Liabilities</td>
                            {financials.balanceSheets.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.totalLiabilities ? `${(stmt.totalLiabilities / 1e6).toFixed(0)}M` : '-'}
                              </td>
                            ))}
                          </tr>
                          <tr className="border-b border-border/50">
                            <td className="py-2 text-foreground">Total Debt</td>
                            {financials.balanceSheets.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.totalDebt ? `${(stmt.totalDebt / 1e6).toFixed(0)}M` : '-'}
                              </td>
                            ))}
                          </tr>
                          <tr>
                            <td className="py-2 text-foreground">Total Equity</td>
                            {financials.balanceSheets.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.totalEquity ? `${(stmt.totalEquity / 1e6).toFixed(0)}M` : '-'}
                              </td>
                            ))}
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}

                {/* Cash Flow Statements */}
                {financials.cashFlowStatements && financials.cashFlowStatements.length > 0 && (
                  <div className="bg-card border border-border rounded-lg p-6">
                    <h3 className="text-lg font-semibold text-foreground mb-4">Cash Flow</h3>
                    <div className="overflow-x-auto">
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="border-b border-border">
                            <th className="text-left py-2 text-muted-foreground font-medium">Metric</th>
                            {financials.cashFlowStatements.slice(0, 5).map((stmt, i) => (
                              <th key={i} className="text-right py-2 text-muted-foreground font-medium">
                                {formatPeriodLabel(stmt.periodDate, stmt.statementType)}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          <tr className="border-b border-border/50">
                            <td className="py-2 text-foreground">Operating Cash Flow</td>
                            {financials.cashFlowStatements.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.operatingCashFlow ? `${(stmt.operatingCashFlow / 1e6).toFixed(0)}M` : '-'}
                              </td>
                            ))}
                          </tr>
                          <tr className="border-b border-border/50">
                            <td className="py-2 text-foreground">Investing Cash Flow</td>
                            {financials.cashFlowStatements.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.investingCashFlow ? `${(stmt.investingCashFlow / 1e6).toFixed(0)}M` : '-'}
                              </td>
                            ))}
                          </tr>
                          <tr className="border-b border-border/50">
                            <td className="py-2 text-foreground">Financing Cash Flow</td>
                            {financials.cashFlowStatements.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.financingCashFlow ? `${(stmt.financingCashFlow / 1e6).toFixed(0)}M` : '-'}
                              </td>
                            ))}
                          </tr>
                          <tr>
                            <td className="py-2 text-foreground">Free Cash Flow</td>
                            {financials.cashFlowStatements.slice(0, 5).map((stmt, i) => (
                              <td key={i} className="text-right py-2 font-mono text-foreground">
                                {stmt.freeCashFlow ? `${(stmt.freeCashFlow / 1e6).toFixed(0)}M` : '-'}
                              </td>
                            ))}
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        )}

        {activeTab === 'documents' && (
          <div className="space-y-4">
            {/* Document Stats */}
            {documentsData && (
              <div className="flex gap-4 text-sm text-muted-foreground">
                <span>{documentsData.totalCount.toLocaleString()} total documents</span>
                <span>·</span>
                <span>{documentsData.downloadedCount.toLocaleString()} downloaded</span>
                <span>·</span>
                <span>{documentsData.extractedCount.toLocaleString()} extracted</span>
              </div>
            )}

            {documents.length === 0 ? (
              <div className="bg-card border border-border rounded-lg p-6 text-center">
                <DocumentTextIcon className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
                <h3 className="text-lg font-semibold text-foreground mb-2">No Documents</h3>
                <p className="text-muted-foreground">
                  No documents found for this company.
                </p>
              </div>
            ) : (
              <div className="bg-card border border-border rounded-lg divide-y divide-border">
                {documents.map((doc) => (
                  <div key={doc.id} className="p-4 flex items-center justify-between">
                    <div className="flex-1">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-xs px-2 py-0.5 rounded bg-blue-500/10 text-blue-400">
                          {doc.documentType.replace('_', ' ')}
                        </span>
                        {doc.reportPeriod && doc.reportPeriod !== 'Unknown' && (
                          <span className="text-xs px-2 py-0.5 rounded bg-muted text-muted-foreground">
                            {doc.reportPeriod}
                          </span>
                        )}
                        {doc.hasLocalFile && (
                          <span className="text-xs px-2 py-0.5 rounded bg-green-500/10 text-green-400">
                            Downloaded
                          </span>
                        )}
                        {doc.hasExtractedText && (
                          <span className="text-xs px-2 py-0.5 rounded bg-purple-500/10 text-purple-400">
                            Extracted
                          </span>
                        )}
                      </div>
                      {doc.title && (
                        <p className="text-sm font-medium text-foreground mt-1 line-clamp-1">
                          {doc.title}
                        </p>
                      )}
                      <p className="text-xs text-muted-foreground mt-1">
                        {doc.publishDate ? new Date(doc.publishDate).toLocaleDateString() : 'Unknown date'}
                        {doc.language && ` · ${doc.language.toUpperCase()}`}
                        {doc.pageCount && ` · ${doc.pageCount} pages`}
                      </p>
                    </div>
                    {doc.sourceUrl && (
                      <a
                        href={doc.sourceUrl}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-sm text-blue-500 hover:text-blue-400 ml-4"
                      >
                        View
                      </a>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {activeTab === 'events' && (
          <div className="space-y-4">
            {/* Event Stats */}
            {eventsData && (
              <div className="flex gap-4 text-sm text-muted-foreground">
                <span>{eventsData.totalCount.toLocaleString()} total events</span>
                <span>·</span>
                <span>{eventsData.upcomingCount.toLocaleString()} upcoming</span>
              </div>
            )}

            {events.length === 0 ? (
              <div className="bg-card border border-border rounded-lg p-6 text-center">
                <CalendarIcon className="w-12 h-12 text-muted-foreground mx-auto mb-4" />
                <h3 className="text-lg font-semibold text-foreground mb-2">No Events</h3>
                <p className="text-muted-foreground">
                  No calendar events found for this company.
                </p>
              </div>
            ) : (
              <div className="bg-card border border-border rounded-lg divide-y divide-border">
                {events.map((event) => {
                  const eventDate = new Date(event.eventDate)
                  const isPast = eventDate < new Date()
                  const isUpcoming = !isPast && eventDate <= new Date(Date.now() + 30 * 24 * 60 * 60 * 1000)

                  return (
                    <div key={event.id} className={`p-4 ${isPast ? 'opacity-60' : ''}`}>
                      <div className="flex items-start justify-between">
                        <div>
                          <div className="flex items-center gap-2 flex-wrap">
                            <span className={`text-xs px-2 py-0.5 rounded ${
                              event.eventType === 'earnings' ? 'bg-blue-500/10 text-blue-400' :
                              event.eventType === 'dividend' ? 'bg-green-500/10 text-green-400' :
                              event.eventType === 'agm' ? 'bg-purple-500/10 text-purple-400' :
                              'bg-muted text-muted-foreground'
                            }`}>
                              {event.eventType}
                            </span>
                            {isUpcoming && (
                              <span className="text-xs px-2 py-0.5 rounded bg-yellow-500/10 text-yellow-400">
                                Upcoming
                              </span>
                            )}
                            {event.confirmed && (
                              <span className="text-xs px-2 py-0.5 rounded bg-green-500/10 text-green-400">
                                Confirmed
                              </span>
                            )}
                          </div>
                          {event.title && (
                            <p className="text-sm font-medium text-foreground mt-1">
                              {event.title}
                            </p>
                          )}
                          <p className="text-sm text-muted-foreground mt-1">
                            {eventDate.toLocaleDateString('en-US', {
                              weekday: 'short',
                              year: 'numeric',
                              month: 'short',
                              day: 'numeric',
                            })}
                            {event.eventTime && ` at ${event.eventTime}`}
                          </p>
                          {event.dividendAmount && (
                            <p className="text-sm text-green-400 mt-1">
                              Dividend: {event.dividendAmount} {event.dividendCurrency || ''}
                              {event.exDividendDate && ` · Ex-div: ${new Date(event.exDividendDate).toLocaleDateString()}`}
                            </p>
                          )}
                        </div>
                        {event.webcastUrl && (
                          <a
                            href={event.webcastUrl}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-sm text-blue-500 hover:text-blue-400 ml-4"
                          >
                            Webcast
                          </a>
                        )}
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
