import type { BacktestRequest } from '@/types/screener'
import { CalendarIcon, ClockIcon, ChartBarIcon } from '@heroicons/react/24/outline'

interface BacktestSettingsProps {
  request: BacktestRequest
  onChange: (updates: Partial<BacktestRequest>) => void
  estimatedTime: number
}

const frequencies = [
  { value: 'daily', label: 'Daily', description: 'Every business day' },
  { value: 'weekly', label: 'Weekly', description: 'Every Monday' },
  { value: 'monthly', label: 'Monthly', description: 'First business day of month' },
]

const forwardPeriods = [
  { value: '1W', label: '1 Week' },
  { value: '1M', label: '1 Month' },
  { value: '3M', label: '3 Months' },
  { value: '6M', label: '6 Months' },
  { value: '1Y', label: '1 Year' },
  { value: '2Y', label: '2 Years' },
]

export default function BacktestSettings({ request, onChange, estimatedTime }: BacktestSettingsProps) {
  // Calculate number of periods
  const calculatePeriods = () => {
    const start = new Date(request.startDate)
    const end = new Date(request.endDate)
    
    if (request.frequency === 'daily') {
      const days = Math.floor((end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24))
      return Math.floor(days * 5 / 7) // Approximate business days
    } else if (request.frequency === 'weekly') {
      const weeks = Math.floor((end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24 * 7))
      return weeks
    } else {
      const months = (end.getFullYear() - start.getFullYear()) * 12 + (end.getMonth() - start.getMonth())
      return months
    }
  }

  const totalPeriods = calculatePeriods()

  return (
    <div className="space-y-6">
      {/* Time period */}
      <div className="card">
        <div className="card-header">
          <div className="flex items-center space-x-2">
            <CalendarIcon className="w-5 h-5 text-primary" />
            <h3 className="card-title text-base">Time Period</h3>
          </div>
        </div>
        <div className="card-content space-y-4">
          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              Start Date
            </label>
            <input
              type="date"
              value={request.startDate}
              onChange={(e) => onChange({ startDate: e.target.value })}
              className="input w-full"
              min="2021-01-01"
              max={new Date().toISOString().split('T')[0]}
            />
          </div>
          
          <div>
            <label className="block text-sm font-medium text-foreground mb-2">
              End Date
            </label>
            <input
              type="date"
              value={request.endDate}
              onChange={(e) => onChange({ endDate: e.target.value })}
              className="input w-full"
              min={request.startDate}
              max={new Date().toISOString().split('T')[0]}
            />
          </div>

          <div className="text-sm text-muted-foreground">
            <div className="flex justify-between">
              <span>Duration:</span>
              <span>
                {Math.floor((new Date(request.endDate).getTime() - new Date(request.startDate).getTime()) / (1000 * 60 * 60 * 24))} days
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Frequency */}
      <div className="card">
        <div className="card-header">
          <div className="flex items-center space-x-2">
            <ChartBarIcon className="w-5 h-5 text-primary" />
            <h3 className="card-title text-base">Screening Frequency</h3>
          </div>
        </div>
        <div className="card-content space-y-3">
          {frequencies.map((freq) => (
            <label key={freq.value} className="flex items-start space-x-3 cursor-pointer">
              <input
                type="radio"
                checked={request.frequency === freq.value}
                onChange={() => onChange({ frequency: freq.value as any })}
                className="mt-1 text-primary"
              />
              <div className="flex-1">
                <div className="text-sm font-medium text-foreground">{freq.label}</div>
                <div className="text-xs text-muted-foreground">{freq.description}</div>
              </div>
            </label>
          ))}
          
          <div className="pt-2 border-t border-border text-sm text-muted-foreground">
            <div className="flex justify-between">
              <span>Screening periods:</span>
              <span className="font-medium">{totalPeriods}</span>
            </div>
          </div>
        </div>
      </div>

      {/* Forward returns */}
      <div className="card">
        <div className="card-header">
          <h3 className="card-title text-base">Forward Returns</h3>
          <p className="card-description">
            Calculate returns over these future periods for each screening date
          </p>
        </div>
        <div className="card-content">
          <div className="grid grid-cols-2 gap-3">
            {forwardPeriods.map((period) => (
              <label key={period.value} className="flex items-center space-x-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={request.forwardPeriods.includes(period.value)}
                  onChange={(e) => {
                    const newPeriods = e.target.checked
                      ? [...request.forwardPeriods, period.value]
                      : request.forwardPeriods.filter(p => p !== period.value)
                    onChange({ forwardPeriods: newPeriods })
                  }}
                  className="rounded border-border text-primary"
                />
                <span className="text-sm text-foreground">{period.label}</span>
              </label>
            ))}
          </div>
          
          {request.forwardPeriods.length === 0 && (
            <div className="mt-3 p-3 bg-orange-50 border border-orange-200 rounded-lg">
              <p className="text-sm text-orange-800">
                Select at least one forward return period to measure strategy performance
              </p>
            </div>
          )}
        </div>
      </div>

      {/* Execution summary */}
      <div className="card">
        <div className="card-header">
          <div className="flex items-center space-x-2">
            <ClockIcon className="w-5 h-5 text-primary" />
            <h3 className="card-title text-base">Execution Summary</h3>
          </div>
        </div>
        <div className="card-content space-y-3">
          <div className="grid grid-cols-2 gap-4 text-sm">
            <div>
              <div className="text-muted-foreground">Total Periods</div>
              <div className="font-medium text-foreground">{totalPeriods}</div>
            </div>
            <div>
              <div className="text-muted-foreground">Est. Time</div>
              <div className="font-medium text-foreground">{Math.ceil(estimatedTime)}s</div>
            </div>
            <div>
              <div className="text-muted-foreground">Forward Periods</div>
              <div className="font-medium text-foreground">{request.forwardPeriods.length}</div>
            </div>
            <div>
              <div className="text-muted-foreground">Frequency</div>
              <div className="font-medium text-foreground capitalize">{request.frequency}</div>
            </div>
          </div>

          {estimatedTime > 60 && (
            <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg">
              <p className="text-sm text-blue-800">
                <strong>Long-running backtest:</strong> Consider using "Quick Test" first 
                to validate your strategy before running the full backtest.
              </p>
            </div>
          )}
          
          {totalPeriods > 100 && (
            <div className="p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
              <p className="text-sm text-yellow-800">
                <strong>High frequency:</strong> {totalPeriods} periods will generate many data points. 
                Consider monthly frequency for faster execution.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}