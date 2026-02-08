import { MagnifyingGlassIcon, Cog6ToothIcon } from '@heroicons/react/24/outline'

export function TopBar() {
  return (
    <header className="h-16 bg-card border-b border-border sticky top-0 z-40">
      <div className="h-full px-6 flex items-center justify-between">
        {/* Left: Spacer */}
        <div className="flex-1" />

        {/* Center: Global Search */}
        <div className="flex-1 max-w-xl">
          <div className="relative">
            <MagnifyingGlassIcon className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-muted-foreground" />
            <input
              type="text"
              placeholder="Search stocks, portfolios..."
              className="w-full pl-10 pr-4 py-2 bg-muted border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>
        </div>

        {/* Right: User Info */}
        <div className="flex-1 flex items-center justify-end space-x-4">
          <button className="p-2 text-muted-foreground hover:text-foreground transition-colors rounded-lg hover:bg-muted">
            <Cog6ToothIcon className="w-5 h-5" />
          </button>
          <div className="flex items-center space-x-3">
            <div className="w-8 h-8 bg-gradient-to-br from-blue-500 to-purple-500 rounded-full flex items-center justify-center">
              <span className="text-white text-sm font-medium">U</span>
            </div>
          </div>
        </div>
      </div>
    </header>
  )
}
