/**
 * Screener API
 * Fetches Fat Pitch data from backend
 */

import { api, toCamelCase } from '@/services/api'
import type { FatPitchSummary, FatPitch, BusinessStage } from './types'

// Fetch all pitches
export async function fetchPitches(): Promise<FatPitch[]> {
  const response = await api.get('/fat-pitch/pitches')
  return toCamelCase<FatPitch[]>(response.data)
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
