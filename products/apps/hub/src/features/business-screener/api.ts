/**
 * Business Screener API
 * Fetches data from the Business Screener Deluxe backend
 */

import { api, toCamelCase } from '@/services/api'
import type {
  ScreenDefinition,
  ScreenResult,
  MultiHit,
  ScreenerDashboard,
  ScreenType,
} from './types'

// Fetch all screen definitions
export async function fetchScreenDefinitions(): Promise<ScreenDefinition[]> {
  const response = await api.get('/business-screener/screens')
  return toCamelCase<ScreenDefinition[]>(response.data)
}

// Fetch results for a specific screen
export async function fetchScreenResults(screenType: ScreenType): Promise<ScreenResult[]> {
  const response = await api.get(`/business-screener/screens/${screenType}/results`)
  return toCamelCase<ScreenResult[]>(response.data)
}

// Fetch all active results across all screens
export async function fetchAllResults(
  activeOnly: boolean = true,
  scoreDate?: string,
  limit: number = 2000
): Promise<ScreenResult[]> {
  const params: Record<string, string> = { limit: String(limit) }
  if (activeOnly) params.active = 'true'
  if (scoreDate) params.score_date = scoreDate
  const response = await api.get('/business-screener/results', { params })
  return toCamelCase<ScreenResult[]>(response.data)
}

// Fetch results for a specific company
export async function fetchCompanyScreens(companyId: string): Promise<ScreenResult[]> {
  const response = await api.get(`/business-screener/companies/${companyId}/screens`)
  return toCamelCase<ScreenResult[]>(response.data)
}

// Fetch multi-hit companies (passing 2+ screens)
export async function fetchMultiHits(): Promise<MultiHit[]> {
  const response = await api.get('/business-screener/dashboard/multi-hits')
  return toCamelCase<MultiHit[]>(response.data)
}

// Fetch dashboard summary
export async function fetchDashboard(): Promise<ScreenerDashboard> {
  const response = await api.get('/business-screener/dashboard')
  return toCamelCase<ScreenerDashboard>(response.data)
}

// Fetch warnings and red flags
export async function fetchWarnings(): Promise<ScreenResult[]> {
  const response = await api.get('/business-screener/dashboard/warnings')
  return toCamelCase<ScreenResult[]>(response.data)
}

// Trigger a screen run (POST)
export async function triggerScreenRun(screenType: ScreenType): Promise<{ status: string; count: number }> {
  const response = await api.post(`/business-screener/screens/${screenType}/run`)
  return toCamelCase<{ status: string; count: number }>(response.data)
}

// Trigger Tier B analysis for a company
export async function triggerTierBAnalysis(companyId: string): Promise<{ status: string }> {
  const response = await api.post(`/business-screener/analysis/tier-b/${companyId}`)
  return toCamelCase<{ status: string }>(response.data)
}

// Trigger Tier C deep analysis for a company
export async function triggerTierCAnalysis(companyId: string): Promise<{ status: string }> {
  const response = await api.post(`/business-screener/analysis/tier-c/${companyId}`)
  return toCamelCase<{ status: string }>(response.data)
}

// Fetch LLM analysis results for a company
export async function fetchCompanyAnalysis(companyId: string): Promise<unknown[]> {
  const response = await api.get(`/business-screener/analysis/${companyId}`)
  return toCamelCase<unknown[]>(response.data)
}
