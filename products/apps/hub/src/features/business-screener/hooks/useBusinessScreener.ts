/**
 * Business Screener Hooks
 * React Query hooks for Business Screener Deluxe data
 */

import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  fetchScreenDefinitions,
  fetchScreenResults,
  fetchAllResults,
  fetchCompanyScreens,
  fetchMultiHits,
  fetchDashboard,
  fetchWarnings,
  triggerScreenRun,
  triggerTierBAnalysis,
  triggerTierCAnalysis,
  fetchCompanyAnalysis,
} from '../api'
import type { ScreenType } from '../types'

// Query keys
export const businessScreenerKeys = {
  all: ['business-screener'] as const,
  definitions: () => [...businessScreenerKeys.all, 'definitions'] as const,
  results: () => [...businessScreenerKeys.all, 'results'] as const,
  allResults: (activeOnly: boolean, scoreDate?: string) => [...businessScreenerKeys.results(), 'all', { activeOnly, scoreDate }] as const,
  screenResults: (screenType: ScreenType) => [...businessScreenerKeys.results(), 'screen', screenType] as const,
  companyScreens: (companyId: string) => [...businessScreenerKeys.results(), 'company', companyId] as const,
  multiHits: () => [...businessScreenerKeys.all, 'multi-hits'] as const,
  dashboard: () => [...businessScreenerKeys.all, 'dashboard'] as const,
  warnings: () => [...businessScreenerKeys.all, 'warnings'] as const,
  analysis: (companyId: string) => [...businessScreenerKeys.all, 'analysis', companyId] as const,
}

// Fetch screen definitions
export function useScreenDefinitions() {
  return useQuery({
    queryKey: businessScreenerKeys.definitions(),
    queryFn: fetchScreenDefinitions,
    staleTime: 60 * 60 * 1000, // 1 hour - definitions rarely change
  })
}

// Fetch all results with optional point-in-time date
export function useAllResults(activeOnly: boolean = true, scoreDate?: string) {
  return useQuery({
    queryKey: businessScreenerKeys.allResults(activeOnly, scoreDate),
    queryFn: () => fetchAllResults(activeOnly, scoreDate),
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}

// Fetch results for a specific screen
export function useScreenResults(screenType: ScreenType) {
  return useQuery({
    queryKey: businessScreenerKeys.screenResults(screenType),
    queryFn: () => fetchScreenResults(screenType),
    staleTime: 5 * 60 * 1000,
  })
}

// Fetch results for a specific company
export function useCompanyScreens(companyId: string) {
  return useQuery({
    queryKey: businessScreenerKeys.companyScreens(companyId),
    queryFn: () => fetchCompanyScreens(companyId),
    enabled: !!companyId,
    staleTime: 5 * 60 * 1000,
  })
}

// Fetch multi-hit companies
export function useMultiHits() {
  return useQuery({
    queryKey: businessScreenerKeys.multiHits(),
    queryFn: fetchMultiHits,
    staleTime: 5 * 60 * 1000,
  })
}

// Fetch dashboard summary
export function useDashboard() {
  return useQuery({
    queryKey: businessScreenerKeys.dashboard(),
    queryFn: fetchDashboard,
    staleTime: 5 * 60 * 1000,
  })
}

// Fetch warnings
export function useWarnings() {
  return useQuery({
    queryKey: businessScreenerKeys.warnings(),
    queryFn: fetchWarnings,
    staleTime: 5 * 60 * 1000,
  })
}

// Fetch company analysis
export function useCompanyAnalysis(companyId: string) {
  return useQuery({
    queryKey: businessScreenerKeys.analysis(companyId),
    queryFn: () => fetchCompanyAnalysis(companyId),
    enabled: !!companyId,
    staleTime: 10 * 60 * 1000,
  })
}

// Mutation: Trigger screen run
export function useTriggerScreenRun() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (screenType: ScreenType) => triggerScreenRun(screenType),
    onSuccess: (_, screenType) => {
      // Invalidate relevant queries
      queryClient.invalidateQueries({ queryKey: businessScreenerKeys.screenResults(screenType) })
      queryClient.invalidateQueries({ queryKey: businessScreenerKeys.results() })
      queryClient.invalidateQueries({ queryKey: businessScreenerKeys.dashboard() })
    },
  })
}

// Mutation: Trigger Tier B analysis
export function useTriggerTierBAnalysis() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (companyId: string) => triggerTierBAnalysis(companyId),
    onSuccess: (_, companyId) => {
      queryClient.invalidateQueries({ queryKey: businessScreenerKeys.companyScreens(companyId) })
      queryClient.invalidateQueries({ queryKey: businessScreenerKeys.analysis(companyId) })
    },
  })
}

// Mutation: Trigger Tier C analysis
export function useTriggerTierCAnalysis() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (companyId: string) => triggerTierCAnalysis(companyId),
    onSuccess: (_, companyId) => {
      queryClient.invalidateQueries({ queryKey: businessScreenerKeys.companyScreens(companyId) })
      queryClient.invalidateQueries({ queryKey: businessScreenerKeys.analysis(companyId) })
    },
  })
}
