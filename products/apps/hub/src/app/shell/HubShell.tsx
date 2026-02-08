import { useState, type ReactNode } from 'react'
import { cn } from '@yodabuffett/ui'
import { Sidebar } from './Sidebar'
import { TopBar } from './TopBar'
import { MainContent } from './MainContent'

interface HubShellProps {
  children: ReactNode
}

export function HubShell({ children }: HubShellProps) {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)

  return (
    <div className="min-h-screen bg-background flex">
      {/* Left Sidebar - Fixed position */}
      <Sidebar
        collapsed={sidebarCollapsed}
        onToggle={() => setSidebarCollapsed(!sidebarCollapsed)}
      />

      {/* Main area */}
      <div
        className={cn(
          'flex-1 flex flex-col min-h-screen transition-all duration-300',
          sidebarCollapsed ? 'ml-16' : 'ml-60'
        )}
      >
        {/* Top Bar */}
        <TopBar />

        {/* Content */}
        <MainContent>{children}</MainContent>
      </div>
    </div>
  )
}
