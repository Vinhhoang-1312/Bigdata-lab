import { afterEach, describe, expect, it, vi } from 'vitest'
import { postTripScore } from './api'
import type { TripScoreRequest, TripScoreResult } from './types'

const request: TripScoreRequest = {
  pickup_zone_id: 43,
  dropoff_zone_id: 161,
  trip_distance_miles: 4.2,
  trip_duration_minutes: 18,
  fare_amount_usd: 22.5,
}

const result: TripScoreResult = {
  run_id: 'trip-anomaly-test',
  method: 'spark_mllib_kmeans',
  score: 1.23,
  threshold: 2.4,
  review_required: false,
  nearest_cluster: 3,
  reason: 'Fare above learned pattern',
  derived_speed_mph: 14,
  features_used: ['trip_distance', 'trip_duration_minutes', 'fare_amount', 'average_speed_mph'],
  use_status: 'research_only',
}

describe('ML API contracts', () => {
  afterEach(() => vi.unstubAllGlobals())

  it('posts an exact trip scoring request and returns the typed result', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify(result), { status: 200 }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(postTripScore(request)).resolves.toEqual(result)
    expect(fetchMock).toHaveBeenCalledWith('/v1/anomaly/score', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(request),
    })
  })

  it('turns FastAPI detail into a readable error', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ detail: 'trip distance must be positive' }), { status: 422 }))
    vi.stubGlobal('fetch', fetchMock)

    await expect(postTripScore(request)).rejects.toThrow('trip distance must be positive')
  })
})
