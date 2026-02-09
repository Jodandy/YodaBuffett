/**
 * Screener Hooks
 * React Query hooks for Fat Pitch data
 */

import { useQuery } from '@tanstack/react-query'
import { fetchPitches, fetchActionablePitches, fetchPitchesByStage, fetchSummary, fetchCompanyPitch, fetchWeightProfiles } from '../api'
import type { BusinessStage } from '../types'

// Query keys
export const pitchKeys = {
  all: ['pitches'] as const,
  lists: () => [...pitchKeys.all, 'list'] as const,
  list: (filters: Record<string, unknown>) => [...pitchKeys.lists(), filters] as const,
  listWithProfile: (profile: string) => [...pitchKeys.lists(), { profile }] as const,
  actionable: () => [...pitchKeys.all, 'actionable'] as const,
  byStage: (stage: BusinessStage) => [...pitchKeys.all, 'stage', stage] as const,
  summary: () => [...pitchKeys.all, 'summary'] as const,
  detail: (id: string) => [...pitchKeys.all, 'detail', id] as const,
  weightProfiles: () => [...pitchKeys.all, 'weight-profiles'] as const,
}

// Fetch all pitches with optional weight profile
export function usePitches(weightProfile?: string) {
  return useQuery({
    queryKey: weightProfile ? pitchKeys.listWithProfile(weightProfile) : pitchKeys.lists(),
    queryFn: () => fetchPitches(weightProfile),
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}

// Fetch available weight profiles
export function useWeightProfiles() {
  return useQuery({
    queryKey: pitchKeys.weightProfiles(),
    queryFn: fetchWeightProfiles,
    staleTime: 30 * 60 * 1000, // 30 minutes - profiles don't change often
  })
}

// Fetch actionable pitches
export function useActionablePitches() {
  return useQuery({
    queryKey: pitchKeys.actionable(),
    queryFn: fetchActionablePitches,
    staleTime: 5 * 60 * 1000,
  })
}

// Fetch pitches by stage
export function usePitchesByStage(stage: BusinessStage) {
  return useQuery({
    queryKey: pitchKeys.byStage(stage),
    queryFn: () => fetchPitchesByStage(stage),
    staleTime: 5 * 60 * 1000,
  })
}

// Fetch summary statistics
export function useSummary() {
  return useQuery({
    queryKey: pitchKeys.summary(),
    queryFn: fetchSummary,
    staleTime: 5 * 60 * 1000,
  })
}

// Fetch single company pitch
export function useCompanyPitch(companyId: string) {
  return useQuery({
    queryKey: pitchKeys.detail(companyId),
    queryFn: () => fetchCompanyPitch(companyId),
    enabled: !!companyId,
    staleTime: 5 * 60 * 1000,
  })
}
