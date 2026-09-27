import { useMemo, useState } from 'react'
import { Alert, Button, InputNumber, Select, Tag } from 'antd'
import { SearchCheck, TriangleAlert } from 'lucide-react'
import { postTripScore } from './api'
import type { TripScoreRequest, TripScoreResult } from './types'
import type { ZoneGeometry } from './TripAnomalyMap'

const fallbackZones = [
  { id: 43, name: 'Central Park' },
  { id: 161, name: 'Midtown Center' },
]

export default function TripScoreForm({ geometry, onScore }: {
  geometry: ZoneGeometry | null
  onScore: (request: TripScoreRequest, result: TripScoreResult) => void
}) {
  const zones = useMemo(() => {
    const fromGeometry = geometry?.features.map(feature => ({
      id: Number(feature.properties.location_id), name: String(feature.properties.zone ?? `Zone ${feature.properties.location_id}`),
    })).filter(item => Number.isInteger(item.id) && item.id > 0)
    return fromGeometry?.length ? fromGeometry : fallbackZones
  }, [geometry])
  const [pickup, setPickup] = useState<number | null>(43)
  const [dropoff, setDropoff] = useState<number | null>(161)
  const [distance, setDistance] = useState<number | null>(4.2)
  const [duration, setDuration] = useState<number | null>(18)
  const [fare, setFare] = useState<number | null>(22.5)
  const [result, setResult] = useState<TripScoreResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const valid = pickup != null && dropoff != null && distance != null && distance > 0
    && duration != null && duration > 0 && fare != null && fare >= 0

  async function submit() {
    if (!valid || pickup == null || dropoff == null || distance == null || duration == null || fare == null) return
    const request: TripScoreRequest = {
      pickup_zone_id: pickup, dropoff_zone_id: dropoff,
      trip_distance_miles: distance, trip_duration_minutes: duration, fare_amount_usd: fare,
    }
    setLoading(true); setError(null); setResult(null)
    try {
      const next = await postTripScore(request)
      setResult(next)
      onScore(request, next)
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : 'Trip scoring failed')
    } finally { setLoading(false) }
  }

  return <section className="trip-score-form" aria-label="Check a trip form">
    <div className="section-heading"><div><span className="eyebrow">K-means inference</span><h2>Check one trip</h2></div><span className="muted">No speed input required</span></div>
    <p className="form-helper">Pickup and dropoff zones provide map context only. The model uses distance, duration, fare and derived speed.</p>
    <div className="trip-score-grid">
      <div><label htmlFor="score-pickup">Pickup zone</label><Select id="score-pickup" aria-label="Pickup zone" showSearch optionFilterProp="label" value={pickup} onChange={value => { setPickup(value); setResult(null) }} options={zones.map(zone => ({ value: zone.id, label: `${zone.id} · ${zone.name}` }))} /></div>
      <div><label htmlFor="score-dropoff">Dropoff zone</label><Select id="score-dropoff" aria-label="Dropoff zone" showSearch optionFilterProp="label" value={dropoff} onChange={value => { setDropoff(value); setResult(null) }} options={zones.map(zone => ({ value: zone.id, label: `${zone.id} · ${zone.name}` }))} /></div>
      <div><label htmlFor="score-distance">Trip distance (miles)</label><InputNumber id="score-distance" aria-label="Trip distance (miles)" min={0.01} step={0.1} value={distance} onChange={setDistance} /></div>
      <div><label htmlFor="score-duration">Trip duration (minutes)</label><InputNumber id="score-duration" aria-label="Trip duration (minutes)" min={0.01} step={1} value={duration} onChange={setDuration} /></div>
      <div><label htmlFor="score-fare">Fare (USD)</label><InputNumber id="score-fare" aria-label="Fare (USD)" min={0} step={0.5} value={fare} onChange={setFare} prefix="$" /></div>
    </div>
    <Button type="primary" icon={<SearchCheck size={16} />} onClick={() => void submit()} disabled={!valid} loading={loading}>Score trip</Button>
    {error && <Alert className="trip-score-alert" type="error" showIcon message="Trip scoring unavailable" description={error} />}
    {result && <div className="trip-score-result" aria-live="polite">
      <div className="trip-score-result-head"><div><span className="eyebrow">Model decision</span><h3>{result.review_required ? 'Review recommended' : 'Within learned pattern'}</h3></div><Tag color={result.review_required ? 'red' : 'green'}>Score {result.score.toFixed(2)}</Tag></div>
      <div className="trip-score-facts"><span>Threshold <strong>{result.threshold.toFixed(2)}</strong></span><span>Nearest learned cluster: <strong>{result.nearest_cluster}</strong></span><span>Derived speed <strong>{result.derived_speed_mph.toFixed(1)} mph</strong></span></div>
      <p><TriangleAlert size={15} /> {result.reason}. This is a review signal, not a fraud probability or safety verdict.</p>
      <small>Run {result.run_id} · {result.use_status}</small>
    </div>}
  </section>
}
