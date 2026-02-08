import { Routes, Route } from 'react-router-dom'
import { Suspense, lazy } from 'react'
import { HubShell } from '@/app/shell/HubShell'
import { LoadingSpinner } from '@yodabuffett/ui'

// Lazy load pages
const DashboardPage = lazy(() => import('@/features/dashboard/pages/DashboardPage'))
const PortfolioListPage = lazy(() => import('@/features/portfolio/pages/PortfolioListPage'))
const PortfolioDetailPage = lazy(() => import('@/features/portfolio/pages/PortfolioDetailPage'))

function App() {
  return (
    <HubShell>
      <Suspense fallback={<LoadingSpinner />}>
        <Routes>
          <Route path="/" element={<DashboardPage />} />
          <Route path="/portfolios" element={<PortfolioListPage />} />
          <Route path="/portfolios/:portfolioId" element={<PortfolioDetailPage />} />
        </Routes>
      </Suspense>
    </HubShell>
  )
}

export default App
