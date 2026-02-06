import { useState } from 'react'
import type { ScreenerQuery, QueryGroup, QueryCondition, MetricDefinition } from '@/types/screener'
import { 
  PlusIcon, 
  TrashIcon, 
  AdjustmentsHorizontalIcon,
  InformationCircleIcon 
} from '@heroicons/react/24/outline'
import { v4 as uuidv4 } from 'uuid'

interface QueryBuilderProps {
  query: ScreenerQuery
  onChange: (query: ScreenerQuery) => void
  availableMetrics: MetricDefinition[]
}

const operators = [
  { value: '>', label: 'Greater than', symbol: '>' },
  { value: '>=', label: 'Greater than or equal', symbol: '≥' },
  { value: '<', label: 'Less than', symbol: '<' },
  { value: '<=', label: 'Less than or equal', symbol: '≤' },
  { value: '=', label: 'Equal to', symbol: '=' },
  { value: '!=', label: 'Not equal to', symbol: '≠' },
  { value: 'between', label: 'Between', symbol: '↔' },
]

export default function QueryBuilder({ query, onChange, availableMetrics }: QueryBuilderProps) {
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set())

  // Group metrics by category
  const metricsByCategory = availableMetrics.reduce((acc, metric) => {
    if (!acc[metric.category]) acc[metric.category] = []
    acc[metric.category].push(metric)
    return acc
  }, {} as Record<string, MetricDefinition[]>)

  // Add new group
  const addGroup = () => {
    const newGroup: QueryGroup = {
      id: uuidv4(),
      conditions: [],
      logicalOperator: 'AND',
    }
    
    onChange({
      ...query,
      groups: [...query.groups, newGroup],
    })
    
    setExpandedGroups(prev => new Set([...prev, newGroup.id]))
  }

  // Remove group
  const removeGroup = (groupId: string) => {
    onChange({
      ...query,
      groups: query.groups.filter(g => g.id !== groupId),
    })
    
    setExpandedGroups(prev => {
      const newSet = new Set(prev)
      newSet.delete(groupId)
      return newSet
    })
  }

  // Update group
  const updateGroup = (groupId: string, updates: Partial<QueryGroup>) => {
    onChange({
      ...query,
      groups: query.groups.map(g => 
        g.id === groupId ? { ...g, ...updates } : g
      ),
    })
  }

  // Add condition to group
  const addCondition = (groupId: string) => {
    const newCondition: QueryCondition = {
      id: uuidv4(),
      leftOperand: '',
      operator: '>',
      rightOperand: '',
      isRelative: false,
    }
    
    updateGroup(groupId, {
      conditions: [
        ...query.groups.find(g => g.id === groupId)!.conditions,
        newCondition
      ]
    })
  }

  // Remove condition from group
  const removeCondition = (groupId: string, conditionId: string) => {
    const group = query.groups.find(g => g.id === groupId)!
    updateGroup(groupId, {
      conditions: group.conditions.filter(c => c.id !== conditionId)
    })
  }

  // Update condition
  const updateCondition = (groupId: string, conditionId: string, updates: Partial<QueryCondition>) => {
    const group = query.groups.find(g => g.id === groupId)!
    updateGroup(groupId, {
      conditions: group.conditions.map(c =>
        c.id === conditionId ? { ...c, ...updates } : c
      )
    })
  }

  // Toggle group expansion
  const toggleGroup = (groupId: string) => {
    setExpandedGroups(prev => {
      const newSet = new Set(prev)
      if (newSet.has(groupId)) {
        newSet.delete(groupId)
      } else {
        newSet.add(groupId)
      }
      return newSet
    })
  }

  // Get metric by ID
  const getMetric = (metricId: string) => {
    return availableMetrics.find(m => m.id === metricId)
  }

  // Render condition input based on operator and metric type
  const renderConditionValue = (
    groupId: string, 
    condition: QueryCondition, 
    metric?: MetricDefinition
  ) => {
    if (condition.operator === 'between') {
      return (
        <div className="flex items-center space-x-2">
          <input
            type="number"
            placeholder="Min"
            value={Array.isArray(condition.rightOperand) ? condition.rightOperand[0] || '' : ''}
            onChange={(e) => {
              const currentValue = Array.isArray(condition.rightOperand) ? condition.rightOperand : ['', '']
              updateCondition(groupId, condition.id, {
                rightOperand: [e.target.value, currentValue[1] || '']
              })
            }}
            className="input flex-1"
          />
          <span className="text-muted-foreground">and</span>
          <input
            type="number"
            placeholder="Max"
            value={Array.isArray(condition.rightOperand) ? condition.rightOperand[1] || '' : ''}
            onChange={(e) => {
              const currentValue = Array.isArray(condition.rightOperand) ? condition.rightOperand : ['', '']
              updateCondition(groupId, condition.id, {
                rightOperand: [currentValue[0] || '', e.target.value]
              })
            }}
            className="input flex-1"
          />
        </div>
      )
    }

    if (condition.isRelative) {
      return (
        <select
          value={typeof condition.rightOperand === 'string' ? condition.rightOperand : ''}
          onChange={(e) => updateCondition(groupId, condition.id, { rightOperand: e.target.value })}
          className="select flex-1"
        >
          <option value="">Select metric...</option>
          {Object.entries(metricsByCategory).map(([category, metrics]) => (
            <optgroup key={category} label={category.charAt(0).toUpperCase() + category.slice(1)}>
              {metrics.map(m => (
                <option key={m.id} value={m.id}>
                  {m.name} {m.unit && `(${m.unit})`}
                </option>
              ))}
            </optgroup>
          ))}
        </select>
      )
    }

    return (
      <input
        type="number"
        placeholder="Value"
        value={typeof condition.rightOperand === 'number' ? condition.rightOperand : condition.rightOperand || ''}
        onChange={(e) => {
          const value = e.target.value === '' ? '' : parseFloat(e.target.value)
          updateCondition(groupId, condition.id, { rightOperand: value })
        }}
        className="input flex-1"
      />
    )
  }

  return (
    <div className="space-y-6">
      {/* Group logic selector */}
      {query.groups.length > 1 && (
        <div className="flex items-center space-x-3">
          <span className="text-sm text-muted-foreground">Combine groups with:</span>
          <div className="flex items-center space-x-2">
            <label className="flex items-center space-x-2 cursor-pointer">
              <input
                type="radio"
                checked={query.groupLogic === 'AND'}
                onChange={() => onChange({ ...query, groupLogic: 'AND' })}
                className="text-primary"
              />
              <span className="text-sm font-medium">AND</span>
            </label>
            <label className="flex items-center space-x-2 cursor-pointer">
              <input
                type="radio"
                checked={query.groupLogic === 'OR'}
                onChange={() => onChange({ ...query, groupLogic: 'OR' })}
                className="text-primary"
              />
              <span className="text-sm font-medium">OR</span>
            </label>
          </div>
        </div>
      )}

      {/* Groups */}
      {query.groups.map((group, groupIndex) => (
        <div key={group.id} className="query-group">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center space-x-3">
              <button
                onClick={() => toggleGroup(group.id)}
                className="flex items-center space-x-2 text-sm font-medium text-foreground hover:text-primary transition-colors"
              >
                <AdjustmentsHorizontalIcon className="w-4 h-4" />
                <span>Group {groupIndex + 1}</span>
                <span className="text-muted-foreground">
                  ({group.conditions.length} conditions)
                </span>
              </button>
              
              {groupIndex > 0 && (
                <div className="px-2 py-1 bg-muted rounded text-xs font-mono text-muted-foreground">
                  {query.groupLogic}
                </div>
              )}
            </div>

            <div className="flex items-center space-x-2">
              {group.conditions.length > 1 && (
                <div className="flex items-center space-x-2">
                  <span className="text-xs text-muted-foreground">Logic:</span>
                  <select
                    value={group.logicalOperator}
                    onChange={(e) => updateGroup(group.id, { 
                      logicalOperator: e.target.value as 'AND' | 'OR' 
                    })}
                    className="text-xs border border-border rounded px-2 py-1 bg-background"
                  >
                    <option value="AND">AND</option>
                    <option value="OR">OR</option>
                  </select>
                </div>
              )}
              
              <button
                onClick={() => removeGroup(group.id)}
                className="p-1 text-muted-foreground hover:text-destructive transition-colors"
              >
                <TrashIcon className="w-4 h-4" />
              </button>
            </div>
          </div>

          {(expandedGroups.has(group.id) || group.conditions.length === 0) && (
            <div className="space-y-3">
              {/* Conditions */}
              {group.conditions.map((condition, conditionIndex) => {
                const metric = getMetric(condition.leftOperand)
                
                return (
                  <div key={condition.id} className="condition-row">
                    {conditionIndex > 0 && (
                      <div className="operator-badge">
                        {group.logicalOperator}
                      </div>
                    )}
                    
                    {/* Left operand (metric) */}
                    <select
                      value={condition.leftOperand}
                      onChange={(e) => updateCondition(group.id, condition.id, { 
                        leftOperand: e.target.value,
                        rightOperand: '' // Reset value when changing metric
                      })}
                      className="select flex-1"
                    >
                      <option value="">Select metric...</option>
                      {Object.entries(metricsByCategory).map(([category, metrics]) => (
                        <optgroup key={category} label={category.charAt(0).toUpperCase() + category.slice(1)}>
                          {metrics.map(m => (
                            <option key={m.id} value={m.id}>
                              {m.name} {m.unit && `(${m.unit})`}
                            </option>
                          ))}
                        </optgroup>
                      ))}
                    </select>

                    {/* Operator */}
                    <select
                      value={condition.operator}
                      onChange={(e) => updateCondition(group.id, condition.id, { 
                        operator: e.target.value,
                        rightOperand: '' // Reset value when changing operator
                      })}
                      className="select w-32"
                      disabled={!condition.leftOperand}
                    >
                      {operators.map(op => (
                        <option key={op.value} value={op.value}>
                          {op.symbol} {op.label}
                        </option>
                      ))}
                    </select>

                    {/* Relative toggle */}
                    {metric && (
                      <label className="flex items-center space-x-2 cursor-pointer">
                        <input
                          type="checkbox"
                          checked={condition.isRelative}
                          onChange={(e) => updateCondition(group.id, condition.id, { 
                            isRelative: e.target.checked,
                            rightOperand: '' // Reset value when toggling relative
                          })}
                          className="rounded border-border"
                        />
                        <span className="text-xs text-muted-foreground">vs metric</span>
                      </label>
                    )}

                    {/* Right operand (value or metric) */}
                    {renderConditionValue(group.id, condition, metric)}

                    {/* Remove condition */}
                    <button
                      onClick={() => removeCondition(group.id, condition.id)}
                      className="p-1 text-muted-foreground hover:text-destructive transition-colors"
                    >
                      <TrashIcon className="w-4 h-4" />
                    </button>
                  </div>
                )
              })}

              {/* Add condition */}
              <button
                onClick={() => addCondition(group.id)}
                className="btn-outline w-full flex items-center justify-center space-x-2"
              >
                <PlusIcon className="w-4 h-4" />
                <span>Add Condition</span>
              </button>
            </div>
          )}
        </div>
      ))}

      {/* Add group */}
      <button
        onClick={addGroup}
        className="btn-primary flex items-center space-x-2"
      >
        <PlusIcon className="w-4 h-4" />
        <span>Add Condition Group</span>
      </button>

      {/* Help text */}
      {query.groups.length === 0 && (
        <div className="bg-muted/50 rounded-lg p-4">
          <div className="flex items-start space-x-3">
            <InformationCircleIcon className="w-5 h-5 text-primary flex-shrink-0 mt-0.5" />
            <div className="text-sm space-y-2">
              <p className="font-medium text-foreground">Building Your First Query</p>
              <ul className="text-muted-foreground space-y-1">
                <li>• Start by adding a condition group</li>
                <li>• Select metrics like P/E ratio, ROE, market cap, etc.</li>
                <li>• Use "vs metric" for relative comparisons (P/E vs Industry P/E)</li>
                <li>• Combine multiple conditions with AND/OR logic</li>
              </ul>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}