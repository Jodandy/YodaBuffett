import { type ReactNode } from 'react'
import { Link, useLocation } from 'react-router-dom'

export interface NavItem {
  name: string
  href: string
  icon: React.ComponentType<{ className?: string }>
}

interface PageLayoutProps {
  children: ReactNode
  productName: string
  navigation: NavItem[]
  actions?: ReactNode
}

export function PageLayout({
  children,
  productName,
  navigation,
  actions,
}: PageLayoutProps) {
  const location = useLocation()

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="border-b border-border bg-card">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-16">
            {/* Logo and brand */}
            <div className="flex items-center">
              <Link to="/" className="flex items-center space-x-3">
                <div className="w-8 h-8 bg-gradient-to-br from-yb-blue to-yb-green rounded-lg flex items-center justify-center">
                  <span className="text-white font-bold text-lg">YB</span>
                </div>
                <div>
                  <h1 className="text-xl font-bold text-foreground">
                    YodaBuffett
                  </h1>
                  <p className="text-xs text-muted-foreground">{productName}</p>
                </div>
              </Link>
            </div>

            {/* Navigation */}
            <nav className="hidden md:flex space-x-8">
              {navigation.map((item) => {
                const isActive = location.pathname === item.href
                return (
                  <Link
                    key={item.name}
                    to={item.href}
                    className={`
                      flex items-center space-x-2 px-3 py-2 rounded-md text-sm font-medium transition-colors
                      ${
                        isActive
                          ? 'bg-primary text-primary-foreground'
                          : 'text-muted-foreground hover:text-foreground hover:bg-muted'
                      }
                    `}
                  >
                    <item.icon className="w-4 h-4" />
                    <span>{item.name}</span>
                  </Link>
                )
              })}
            </nav>

            {/* User actions */}
            <div className="flex items-center space-x-4">{actions}</div>
          </div>
        </div>

        {/* Mobile navigation */}
        <div className="md:hidden border-t border-border">
          <div className="max-w-7xl mx-auto px-4">
            <div className="flex space-x-4 py-3">
              {navigation.map((item) => {
                const isActive = location.pathname === item.href
                return (
                  <Link
                    key={item.name}
                    to={item.href}
                    className={`
                      flex flex-col items-center space-y-1 px-3 py-2 rounded-md text-xs font-medium transition-colors
                      ${
                        isActive
                          ? 'bg-primary text-primary-foreground'
                          : 'text-muted-foreground hover:text-foreground hover:bg-muted'
                      }
                    `}
                  >
                    <item.icon className="w-5 h-5" />
                    <span>{item.name}</span>
                  </Link>
                )
              })}
            </div>
          </div>
        </div>
      </header>

      {/* Main content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {children}
      </main>

      {/* Footer */}
      <footer className="border-t border-border bg-card mt-16">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
          <div className="flex justify-between items-center">
            <div className="text-sm text-muted-foreground">
              © {new Date().getFullYear()} YodaBuffett. Powered by AI-driven
              financial intelligence.
            </div>
            <div className="flex space-x-6 text-sm text-muted-foreground">
              <a
                href="#"
                className="hover:text-foreground transition-colors"
              >
                Documentation
              </a>
              <a
                href="#"
                className="hover:text-foreground transition-colors"
              >
                Support
              </a>
              <a
                href="#"
                className="hover:text-foreground transition-colors"
              >
                API
              </a>
            </div>
          </div>
        </div>
      </footer>
    </div>
  )
}
