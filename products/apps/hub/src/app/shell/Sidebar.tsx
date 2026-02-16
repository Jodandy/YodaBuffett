import { Link, useLocation } from 'react-router-dom'
import { cn } from '@yodabuffett/ui'
import {
  HomeIcon,
  BriefcaseIcon,
  EyeIcon,
  BellIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  FunnelIcon,
  CalendarIcon,
  SparklesIcon,
} from '@heroicons/react/24/outline'

interface SidebarProps {
  collapsed: boolean
  onToggle: () => void
}

const navigation = [
  { name: 'Dashboard', href: '/', icon: HomeIcon },
  { name: 'Fat Pitch', href: '/screener', icon: FunnelIcon },
  { name: 'Quality', href: '/quality', icon: SparklesIcon },
  { name: 'Calendar', href: '/calendar', icon: CalendarIcon },
  { name: 'Portfolios', href: '/portfolios', icon: BriefcaseIcon },
  { name: 'Watchlist', href: '/watchlist', icon: EyeIcon },
  { name: 'Alerts', href: '/alerts', icon: BellIcon, disabled: true },
]

export function Sidebar({ collapsed, onToggle }: SidebarProps) {
  const location = useLocation()

  return (
    <aside
      className={cn(
        'fixed left-0 top-0 h-screen bg-card border-r border-border flex flex-col transition-all duration-300 z-50',
        collapsed ? 'w-16' : 'w-60'
      )}
    >
      {/* Logo/Brand */}
      <div className="h-16 flex items-center px-4 border-b border-border">
        <Link to="/" className="flex items-center space-x-3">
          <div className="w-8 h-8 bg-gradient-to-br from-blue-600 to-green-500 rounded-lg flex items-center justify-center flex-shrink-0">
            <span className="text-white font-bold text-lg">YB</span>
          </div>
          {!collapsed && (
            <div>
              <h1 className="text-lg font-bold text-foreground">YodaBuffett</h1>
              <p className="text-xs text-muted-foreground">Hub</p>
            </div>
          )}
        </Link>
      </div>

      {/* Navigation */}
      <nav className="flex-1 py-4 space-y-1 px-3">
        {navigation.map((item) => {
          const isActive =
            location.pathname === item.href ||
            (item.href !== '/' && location.pathname.startsWith(item.href))

          if (item.disabled) {
            return (
              <div
                key={item.name}
                className={cn(
                  'flex items-center space-x-3 px-3 py-2.5 rounded-lg text-sm font-medium',
                  'text-muted-foreground opacity-50 cursor-not-allowed'
                )}
              >
                <item.icon className="w-5 h-5 flex-shrink-0" />
                {!collapsed && (
                  <>
                    <span>{item.name}</span>
                    <span className="ml-auto text-xs bg-muted px-1.5 py-0.5 rounded">
                      Soon
                    </span>
                  </>
                )}
              </div>
            )
          }

          return (
            <Link
              key={item.name}
              to={item.href}
              className={cn(
                'flex items-center space-x-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors',
                isActive
                  ? 'bg-blue-600 text-white'
                  : 'text-muted-foreground hover:text-foreground hover:bg-muted'
              )}
            >
              <item.icon className="w-5 h-5 flex-shrink-0" />
              {!collapsed && <span>{item.name}</span>}
            </Link>
          )
        })}
      </nav>

      {/* Collapse toggle */}
      <div className="p-3 border-t border-border">
        <button
          onClick={onToggle}
          className="w-full flex items-center justify-center p-2 rounded-lg text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
        >
          {collapsed ? (
            <ChevronRightIcon className="w-5 h-5" />
          ) : (
            <ChevronLeftIcon className="w-5 h-5" />
          )}
        </button>
      </div>
    </aside>
  )
}
