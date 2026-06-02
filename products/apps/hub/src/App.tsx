import { Routes, Route } from 'react-router-dom'
import { Suspense, lazy } from 'react'
import { HubShell } from '@/app/shell/HubShell'
import { LoadingSpinner } from '@yodabuffett/ui'

// Lazy load pages
const DashboardPage = lazy(() => import('@/features/dashboard/pages/DashboardPage'))
const ScreenerPage = lazy(() => import('@/features/screener/pages/ScreenerPage'))
const QualityScreenerPage = lazy(() => import('@/features/quality-screener/pages/QualityScreenerPage'))
const BusinessScreenerPage = lazy(() => import('@/features/business-screener/pages/BusinessScreenerPage'))
const CompanyDetailPage = lazy(() => import('@/features/company/pages/CompanyDetailPage'))
const PortfolioListPage = lazy(() => import('@/features/portfolio/pages/PortfolioListPage'))
const PortfolioDetailPage = lazy(() => import('@/features/portfolio/pages/PortfolioDetailPage'))
const CalendarPage = lazy(() => import('@/features/calendar/pages/CalendarPage'))
const WatchlistListPage = lazy(() => import('@/features/watchlist/pages/WatchlistListPage'))
const WatchlistDetailPage = lazy(() => import('@/features/watchlist/pages/WatchlistDetailPage'))

function App() {
  return (
    <HubShell>
      <Suspense fallback={<LoadingSpinner />}>
        <Routes>
          <Route path="/" element={<DashboardPage />} />
          <Route path="/screener" element={<ScreenerPage />} />
          <Route path="/quality" element={<QualityScreenerPage />} />
          <Route path="/business-screener" element={<BusinessScreenerPage />} />
          <Route path="/company/:symbol" element={<CompanyDetailPage />} />
          <Route path="/portfolios" element={<PortfolioListPage />} />
          <Route path="/portfolios/:portfolioId" element={<PortfolioDetailPage />} />
          <Route path="/watchlist" element={<WatchlistListPage />} />
          <Route path="/watchlist/:watchlistId" element={<WatchlistDetailPage />} />
          <Route path="/calendar" element={<CalendarPage />} />
        </Routes>
      </Suspense>
    </HubShell>
  )
}

export default App
