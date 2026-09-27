import { useCallback, useEffect, useState } from 'react'
import type { TripScoreRequest, TripScoreResult } from './types'

type Snapshot<T> = { value: T; savedAt: string }

function fromCache<T>(key: string): Snapshot<T> | null {
  try {
    const raw = sessionStorage.getItem(`urbanflow:${key}`)
    return raw ? JSON.parse(raw) as Snapshot<T> : null
  } catch { return null }
}

export function useResource<T>(key: string, url: string) {
  const [snapshot, setSnapshot] = useState<Snapshot<T> | null>(() => fromCache<T>(key))
  const [loading, setLoading] = useState(!snapshot)
  const [error, setError] = useState<string | null>(null)

  const refresh = useCallback(async (force = false) => {
    setLoading(true)
    setError(null)
    try {
      const response = await fetch(force ? `${url}${url.includes('?') ? '&' : '?'}refresh=1` : url)
      if (!response.ok) throw new Error(`Service returned HTTP ${response.status}`)
      const value = await response.json() as T
      const next = { value, savedAt: new Date().toISOString() }
      setSnapshot(next)
      try { sessionStorage.setItem(`urbanflow:${key}`, JSON.stringify(next)) } catch { /* cache optional */ }
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Unable to reach service')
    } finally { setLoading(false) }
  }, [key, url])

  useEffect(() => { if (!snapshot) void refresh() }, []) // Initial read; later reads are explicit.
  return { data: snapshot?.value ?? null, savedAt: snapshot?.savedAt ?? null, loading, error, refresh }
}

export async function postForecast(zone_id: number, forecast_at: string): Promise<import('./types').Forecast> {
  const response = await fetch('/v1/forecast', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ zone_id, forecast_at }),
  })
  if (!response.ok) {
    const body = await response.json().catch(() => ({}))
    throw new Error(typeof body.detail === 'string' ? body.detail : `Forecast service returned HTTP ${response.status}`)
  }
  return response.json()
}

export async function postTripScore(request: TripScoreRequest): Promise<TripScoreResult> {
  const response = await fetch('/v1/anomaly/score', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(request),
  })
  if (!response.ok) {
    const body = await response.json().catch(() => ({})) as { detail?: unknown }
    throw new Error(typeof body.detail === 'string' ? body.detail : `Trip scoring service returned HTTP ${response.status}`)
  }
  return response.json() as Promise<TripScoreResult>
}
