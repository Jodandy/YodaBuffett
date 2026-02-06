import { Routes, Route } from 'react-router-dom'
import { PageLayout, type NavItem } from '@yodabuffett/ui'
import {
  MagnifyingGlassIcon,
  DocumentTextIcon,
  ChartBarSquareIcon,
} from '@heroicons/react/24/outline'

const navigation: NavItem[] = [
  { name: 'Search', href: '/', icon: MagnifyingGlassIcon },
  { name: 'Documents', href: '/documents', icon: DocumentTextIcon },
  { name: 'Anomalies', href: '/anomalies', icon: ChartBarSquareIcon },
]

function HomePage() {
  return (
    <div className="space-y-6">
      <div className="card">
        <div className="card-header">
          <h2 className="card-title">Welcome to YodaBuffett Research</h2>
          <p className="card-description">
            Explore financial documents, analyze patterns, and discover insights
          </p>
        </div>
        <div className="card-content">
          <p className="text-muted-foreground">
            This is a scaffold for the Research product. Features coming soon:
          </p>
          <ul className="mt-4 space-y-2 text-sm text-muted-foreground">
            <li>- Semantic search across financial documents</li>
            <li>- Temporal anomaly detection</li>
            <li>- Document analysis and summarization</li>
            <li>- Cross-company pattern discovery</li>
          </ul>
        </div>
      </div>
    </div>
  )
}

function DocumentsPage() {
  return (
    <div className="card">
      <div className="card-header">
        <h2 className="card-title">Documents</h2>
        <p className="card-description">Browse and search financial documents</p>
      </div>
      <div className="card-content">
        <p className="text-muted-foreground">Coming soon...</p>
      </div>
    </div>
  )
}

function AnomaliesPage() {
  return (
    <div className="card">
      <div className="card-header">
        <h2 className="card-title">Anomaly Detection</h2>
        <p className="card-description">
          Discover unusual patterns in company communications
        </p>
      </div>
      <div className="card-content">
        <p className="text-muted-foreground">Coming soon...</p>
      </div>
    </div>
  )
}

export default function App() {
  return (
    <PageLayout productName="Research" navigation={navigation}>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/documents" element={<DocumentsPage />} />
        <Route path="/anomalies" element={<AnomaliesPage />} />
      </Routes>
    </PageLayout>
  )
}
