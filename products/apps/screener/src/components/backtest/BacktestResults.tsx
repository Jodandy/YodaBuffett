import { useState, useMemo } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar } from 'recharts'
import type { BacktestResponse } from '@/types/screener'
import { 
  ChartBarIcon, 
  ArrowTrendingUpIcon, 
  ArrowTrendingDownIcon,
  ArrowUpIcon,
  ArrowDownIcon
} from '@heroicons/react/24/outline'
import { format } from 'date-fns'

interface BacktestResultsProps {
  response: BacktestResponse
}

export default function BacktestResults({ response }: BacktestResultsProps) {
  const [selectedPeriod, setSelectedPeriod] = useState<string>(
    Object.keys(response.summary.avgReturns)[0] || '1M'
  )
  const [activeTab, setActiveTab] = useState<'performance' | 'signals' | 'periods'>('performance')

  // Prepare chart data
  const chartData = useMemo(() => {
    return response.results.map(result => ({
      date: result.date,
      dateFormatted: format(new Date(result.date), 'MMM yyyy'),
      signals: result.matches,
      return: result.avgReturn[selectedPeriod] || 0,
      winRate: (result.winRate[selectedPeriod] || 0) * 100,
      sharpe: result.sharpeRatio[selectedPeriod] || 0,
    }))
  }, [response.results, selectedPeriod])

  // Calculate cumulative returns
  const cumulativeData = useMemo(() => {
    let cumulative = 1
    return chartData.map(point => {
      cumulative *= (1 + point.return / 100)
      return {
        ...point,
        cumulativeReturn: (cumulative - 1) * 100
      }
    })
  }, [chartData])

  // Get performance indicators
  const getPerformanceColor = (value: number): string => {
    if (value > 0) return 'text-green-600'
    if (value < 0) return 'text-red-600'
    return 'text-gray-600'
  }

  const getPerformanceIcon = (value: number) => {
    if (value > 0) return <ArrowTrendingUpIcon className="w-4 h-4 text-green-600" />
    if (value < 0) return <ArrowTrendingDownIcon className="w-4 h-4 text-red-600" />
    return <div className="w-4 h-4" />
  }

  const tabs = [
    { id: 'performance', label: 'Performance', icon: ChartBarIcon },
    { id: 'signals', label: 'Signal Analysis', icon: ArrowTrendingUpIcon },
    { id: 'periods', label: 'Period Breakdown', icon: ArrowUpIcon },
  ]

  return (
    <div className="space-y-6">
      {/* Period selector */}
      <div className="flex flex-wrap gap-2">
        {Object.keys(response.summary.avgReturns).map(period => (
          <button
            key={period}
            onClick={() => setSelectedPeriod(period)}
            className={`
              px-4 py-2 rounded-lg text-sm font-medium transition-colors
              ${period === selectedPeriod
                ? 'bg-primary text-primary-foreground'
                : 'bg-muted text-muted-foreground hover:bg-muted/80 hover:text-foreground'
              }
            `}
          >
            {period} Returns
          </button>
        ))}
      </div>

      {/* Summary metrics */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="card">
          <div className="card-content py-4">
            <div className="flex items-center justify-between">
              <div>
                <div className="text-2xl font-bold text-foreground">
                  {response.summary.totalSignals}
                </div>
                <div className="text-sm text-muted-foreground">Total Signals</div>
              </div>
              <ChartBarIcon className="w-8 h-8 text-muted-foreground" />
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-content py-4">
            <div className="flex items-center justify-between">
              <div>
                <div className={`text-2xl font-bold ${getPerformanceColor(response.summary.avgReturns[selectedPeriod] || 0)}`}>
                  {(response.summary.avgReturns[selectedPeriod] || 0).toFixed(2)}%
                </div>
                <div className="text-sm text-muted-foreground">Avg Return ({selectedPeriod})</div>
              </div>
              {getPerformanceIcon(response.summary.avgReturns[selectedPeriod] || 0)}
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-content py-4">
            <div className="flex items-center justify-between">
              <div>
                <div className={`text-2xl font-bold ${getPerformanceColor((response.summary.winRates[selectedPeriod] || 0) * 100 - 50)}`}>
                  {((response.summary.winRates[selectedPeriod] || 0) * 100).toFixed(1)}%
                </div>
                <div className="text-sm text-muted-foreground">Win Rate ({selectedPeriod})</div>
              </div>
              <div className="w-8 h-8 flex items-center justify-center">
                {(response.summary.winRates[selectedPeriod] || 0) > 0.5 ? (
                  <ArrowUpIcon className="w-6 h-6 text-green-600" />
                ) : (
                  <ArrowDownIcon className="w-6 h-6 text-red-600" />
                )}
              </div>
            </div>
          </div>
        </div>

        <div className="card">
          <div className="card-content py-4">
            <div className="flex items-center justify-between">
              <div>
                <div className={`text-2xl font-bold ${getPerformanceColor(response.summary.sharpeRatios[selectedPeriod] || 0)}`}>
                  {(response.summary.sharpeRatios[selectedPeriod] || 0).toFixed(2)}
                </div>
                <div className="text-sm text-muted-foreground">Sharpe Ratio</div>
              </div>
              <div className="w-8 h-8 bg-muted/50 rounded flex items-center justify-center">
                <span className="text-xs font-mono text-muted-foreground">σ</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-border">
        <nav className="flex space-x-8">
          {tabs.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id as any)}
              className={`
                flex items-center space-x-2 py-2 px-1 border-b-2 text-sm font-medium transition-colors
                ${activeTab === tab.id
                  ? 'border-primary text-primary'
                  : 'border-transparent text-muted-foreground hover:text-foreground hover:border-muted'
                }
              `}
            >
              <tab.icon className="w-4 h-4" />
              <span>{tab.label}</span>
            </button>
          ))}
        </nav>
      </div>

      {/* Tab content */}
      {activeTab === 'performance' && (
        <div className="space-y-6">
          {/* Cumulative returns chart */}
          <div className="card">
            <div className="card-header">
              <h3 className="card-title">Cumulative Returns ({selectedPeriod})</h3>
              <p className="card-description">
                Strategy performance over time vs individual period returns
              </p>
            </div>
            <div className="card-content">
              <div className="h-80">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={cumulativeData}>
                    <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
                    <XAxis 
                      dataKey="dateFormatted" 
                      tick={{ fontSize: 12 }}
                      interval="preserveStartEnd"
                    />
                    <YAxis tick={{ fontSize: 12 }} />
                    <Tooltip 
                      labelFormatter={(label) => `Period: ${label}`}
                      formatter={(value: number, name: string) => [
                        `${value.toFixed(2)}%`,
                        name === 'cumulativeReturn' ? 'Cumulative Return' : 'Period Return'
                      ]}
                    />
                    <Line 
                      type="monotone" 
                      dataKey="cumulativeReturn" 
                      stroke="#3b82f6" 
                      strokeWidth={2}
                      dot={{ r: 3 }}
                    />
                    <Line 
                      type="monotone" 
                      dataKey="return" 
                      stroke="#94a3b8" 
                      strokeWidth={1}
                      dot={{ r: 2 }}
                      strokeDasharray="5 5"
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>

          {/* Best/Worst periods */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="card">
              <div className="card-header">
                <h3 className="card-title text-green-600">Best Performance</h3>
              </div>
              <div className="card-content space-y-3">
                <div className="flex justify-between items-center">
                  <span className="text-sm text-muted-foreground">Best Month:</span>
                  <div className="text-right">
                    <div className="font-medium text-green-600">
                      {typeof response.summary.bestMonth[selectedPeriod] === 'object' 
                        ? (response.summary.bestMonth[selectedPeriod] as any).return?.toFixed(2) 
                        : response.summary.bestMonth[selectedPeriod]
                      }%
                    </div>
                    <div className="text-xs text-muted-foreground">
                      {typeof response.summary.bestMonth[selectedPeriod] === 'object' 
                        ? format(new Date((response.summary.bestMonth[selectedPeriod] as any).date), 'MMM yyyy')
                        : 'N/A'
                      }
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div className="card">
              <div className="card-header">
                <h3 className="card-title text-red-600">Worst Performance</h3>
              </div>
              <div className="card-content space-y-3">
                <div className="flex justify-between items-center">
                  <span className="text-sm text-muted-foreground">Worst Month:</span>
                  <div className="text-right">
                    <div className="font-medium text-red-600">
                      {typeof response.summary.worstMonth[selectedPeriod] === 'object' 
                        ? (response.summary.worstMonth[selectedPeriod] as any).return?.toFixed(2) 
                        : response.summary.worstMonth[selectedPeriod]
                      }%
                    </div>
                    <div className="text-xs text-muted-foreground">
                      {typeof response.summary.worstMonth[selectedPeriod] === 'object' 
                        ? format(new Date((response.summary.worstMonth[selectedPeriod] as any).date), 'MMM yyyy')
                        : 'N/A'
                      }
                    </div>
                  </div>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm text-muted-foreground">Max Drawdown:</span>
                  <div className="font-medium text-red-600">
                    {response.summary.maxDrawdown.toFixed(2)}%
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {activeTab === 'signals' && (
        <div className="space-y-6">
          {/* Signal count chart */}
          <div className="card">
            <div className="card-header">
              <h3 className="card-title">Signal Count by Period</h3>
              <p className="card-description">
                Number of companies matching screening criteria over time
              </p>
            </div>
            <div className="card-content">
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={chartData}>
                    <CartesianGrid strokeDasharray="3 3" className="opacity-30" />
                    <XAxis 
                      dataKey="dateFormatted" 
                      tick={{ fontSize: 12 }}
                      interval="preserveStartEnd"
                    />
                    <YAxis tick={{ fontSize: 12 }} />
                    <Tooltip 
                      labelFormatter={(label) => `Period: ${label}`}
                      formatter={(value: number) => [`${value}`, 'Signals']}
                    />
                    <Bar dataKey="signals" fill="#3b82f6" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>

          {/* Signal statistics */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="card">
              <div className="card-content py-4">
                <div className="text-center">
                  <div className="text-xl font-bold text-foreground">
                    {Math.round(response.summary.totalSignals / response.results.length)}
                  </div>
                  <div className="text-sm text-muted-foreground">Avg Signals/Period</div>
                </div>
              </div>
            </div>
            <div className="card">
              <div className="card-content py-4">
                <div className="text-center">
                  <div className="text-xl font-bold text-foreground">
                    {Math.max(...chartData.map(d => d.signals))}
                  </div>
                  <div className="text-sm text-muted-foreground">Max Signals</div>
                </div>
              </div>
            </div>
            <div className="card">
              <div className="card-content py-4">
                <div className="text-center">
                  <div className="text-xl font-bold text-foreground">
                    {Math.min(...chartData.map(d => d.signals))}
                  </div>
                  <div className="text-sm text-muted-foreground">Min Signals</div>
                </div>
              </div>
            </div>
            <div className="card">
              <div className="card-content py-4">
                <div className="text-center">
                  <div className="text-xl font-bold text-foreground">
                    {chartData.filter(d => d.signals > 0).length}
                  </div>
                  <div className="text-sm text-muted-foreground">Active Periods</div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {activeTab === 'periods' && (
        <div className="space-y-6">
          {/* Period-by-period results */}
          <div className="card">
            <div className="card-header">
              <h3 className="card-title">Period-by-Period Results</h3>
              <p className="card-description">
                Detailed breakdown of each screening period
              </p>
            </div>
            <div className="card-content">
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border">
                      <th className="text-left py-3">Date</th>
                      <th className="text-center py-3">Signals</th>
                      <th className="text-center py-3">Avg Return</th>
                      <th className="text-center py-3">Win Rate</th>
                      <th className="text-center py-3">Sharpe Ratio</th>
                    </tr>
                  </thead>
                  <tbody>
                    {response.results.slice(0, 20).map((result, index) => (
                      <tr key={index} className="border-b border-border/50">
                        <td className="py-3 font-mono text-sm">
                          {format(new Date(result.date), 'MMM dd, yyyy')}
                        </td>
                        <td className="text-center py-3">{result.matches}</td>
                        <td className={`text-center py-3 font-medium ${getPerformanceColor(result.avgReturn[selectedPeriod] || 0)}`}>
                          {(result.avgReturn[selectedPeriod] || 0).toFixed(2)}%
                        </td>
                        <td className="text-center py-3">
                          {((result.winRate[selectedPeriod] || 0) * 100).toFixed(1)}%
                        </td>
                        <td className="text-center py-3">
                          {(result.sharpeRatio[selectedPeriod] || 0).toFixed(2)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {response.results.length > 20 && (
                  <div className="text-center py-4 text-muted-foreground text-sm">
                    Showing first 20 of {response.results.length} periods
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}