/**
 * Screener API
 * Fetches Fat Pitch data from backend
 */

import { api, toCamelCase } from '@/services/api'
import type { FatPitchSummary, FatPitch, BusinessStage, WeightProfileListResponse } from './types'

// Fetch all pitches with optional weight profile and date
export async function fetchPitches(weightProfile?: string, scoreDate?: string): Promise<FatPitch[]> {
  const params: Record<string, string | number> = { limit: 2000 }
  if (weightProfile) {
    params.weight_profile = weightProfile
  }
  if (scoreDate) {
    params.score_date = scoreDate
  }
  const response = await api.get('/fat-pitch/pitches', { params })
  return toCamelCase<FatPitch[]>(response.data)
}

// Fetch available weight profiles
export async function fetchWeightProfiles(): Promise<WeightProfileListResponse> {
  try {
    const response = await api.get('/fat-pitch/weight-profiles')
    return toCamelCase<WeightProfileListResponse>(response.data)
  } catch (error) {
    console.warn('Failed to fetch weight profiles:', error)
    // Return default fallback
    return {
      profiles: [
        { name: 'optimal', description: 'Best predictor from backtesting', weights: {}, isDefault: true }
      ],
      defaultProfile: 'optimal'
    }
  }
}

// Fetch actionable pitches only
export async function fetchActionablePitches(): Promise<FatPitch[]> {
  const response = await api.get('/fat-pitch/pitches/actionable')
  return toCamelCase<FatPitch[]>(response.data)
}

// Fetch pitches by stage
export async function fetchPitchesByStage(stage: BusinessStage): Promise<FatPitch[]> {
  const response = await api.get(`/fat-pitch/pitches/stage/${stage}`)
  return toCamelCase<FatPitch[]>(response.data)
}

// Fetch summary statistics
export async function fetchSummary(): Promise<FatPitchSummary> {
  const response = await api.get('/fat-pitch/summary')
  return toCamelCase<FatPitchSummary>(response.data)
}

// Fetch single company pitch
export async function fetchCompanyPitch(companyId: string): Promise<FatPitch> {
  const response = await api.get(`/fat-pitch/pitches/company/${companyId}`)
  return toCamelCase<FatPitch>(response.data)
}

// Fetch momentum pitches (companies with improving scores)
export async function fetchMomentumPitches(
  minScoreChange: number = 5,
  weightProfile?: string,
  scoreDate?: string
): Promise<FatPitch[]> {
  const params: Record<string, string | number> = {
    min_score_change: minScoreChange,
    limit: 100
  }
  if (weightProfile) {
    params.weight_profile = weightProfile
  }
  if (scoreDate) {
    params.score_date = scoreDate
  }
  const response = await api.get('/fat-pitch/pitches/momentum', { params })
  return toCamelCase<FatPitch[]>(response.data)
}
