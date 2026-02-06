import { Routes, Route } from 'react-router-dom'
import Layout from './components/layout/Layout'
import ScreenerPage from './pages/ScreenerPage'
import BacktestPage from './pages/BacktestPage'
import SavedQueriesPage from './pages/SavedQueriesPage'
import './App.css'

function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<ScreenerPage />} />
        <Route path="/backtest" element={<BacktestPage />} />
        <Route path="/saved" element={<SavedQueriesPage />} />
      </Routes>
    </Layout>
  )
}

export default App