/**
 * CompanyTabs Component
 * Tab navigation for the company detail page
 */

import { cn } from '@yodabuffett/ui'
import type { CompanyTab } from '../types'

interface CompanyTabsProps {
  activeTab: CompanyTab
  onTabChange: (tab: CompanyTab) => void
  documentCount?: number
  eventCount?: number
}

const tabs: { id: CompanyTab; label: string }[] = [
  { id: 'overview', label: 'Overview' },
  { id: 'financials', label: 'Financials' },
  { id: 'documents', label: 'Documents' },
  { id: 'events', label: 'Events' },
]

export function CompanyTabs({
  activeTab,
  onTabChange,
  documentCount,
  eventCount,
}: CompanyTabsProps) {
  return (
    <div className="border-b border-border">
      <nav className="flex gap-1">
        {tabs.map(tab => {
          const isActive = activeTab === tab.id
          let count: number | undefined
          if (tab.id === 'documents') count = documentCount
          if (tab.id === 'events') count = eventCount

          return (
            <button
              key={tab.id}
              onClick={() => onTabChange(tab.id)}
              className={cn(
                'px-4 py-3 text-sm font-medium transition-colors relative',
                isActive
                  ? 'text-foreground'
                  : 'text-muted-foreground hover:text-foreground'
              )}
            >
              <span className="flex items-center gap-2">
                {tab.label}
                {count !== undefined && count > 0 && (
                  <span className={cn(
                    'text-xs px-1.5 py-0.5 rounded-full',
                    isActive
                      ? 'bg-blue-600 text-white'
                      : 'bg-muted text-muted-foreground'
                  )}>
                    {count}
                  </span>
                )}
              </span>
              {isActive && (
                <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-blue-600" />
              )}
            </button>
          )
        })}
      </nav>
    </div>
  )
}
