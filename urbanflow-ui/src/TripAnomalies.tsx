import { useEffect, useMemo, useState } from 'react'
import { Alert, Button, Empty, Segmented, Spin, Tag, Tooltip } from 'antd'
import { RefreshCw, TriangleAlert } from 'lucide-react'
import { useResource } from './api'
import { formatCount } from './format'
import type { TripAnomaly, TripAnomalySnapshot, TripScoreRequest, TripScoreResult } from './types'
import TripAnomalyMap, { type ZoneGeometry } from './TripAnomalyMap'
import TripScoreForm from './TripScoreForm'

function zoneName(geometry: ZoneGeometry | null, id: number): string {
  const feature = geometry?.features.find(item => Number(item.properties.location_id) === id)
  return feature ? `${id} · ${String(feature.properties.zone)}` : `Zone ${id}`
}

function tripTime(value: string): string {
  return value.replace('T', ' ').slice(0, 16)
}

function tripDuration(minutes: number): string {
  return minutes < 1 ? `${Math.max(1, Math.round(minutes * 60))} sec` : `${minutes.toFixed(1)} min`
}

function TripDetail({ trip, geometry, checkMode = false }: { trip: TripAnomaly; geometry: ZoneGeometry | null; checkMode?: boolean }) {
  return <section className="trip-detail" aria-label="Selected trip details">
    <div className="section-heading"><div><span className="eyebrow">{checkMode ? 'Scored input' : `Review candidate #${trip.rank}`}</span><h2>{zoneName(geometry, trip.pickup_zone_id)} → {zoneName(geometry, trip.dropoff_zone_id)}</h2></div><Tag color={trip.score >= 2.4 ? 'red' : 'gold'}>Score {trip.score.toFixed(2)}</Tag></div>
    <div className="trip-facts"><div><span>Pickup · New York</span><strong>{tripTime(trip.pickup_at)}</strong></div><div><span>Distance</span><strong>{trip.trip_distance_miles.toFixed(1)} mi</strong></div><div><span>Duration</span><strong>{tripDuration(trip.trip_duration_minutes)}</strong></div><div><span>Fare</span><strong>${trip.fare_amount_usd.toFixed(2)}</strong></div></div>
    <p><TriangleAlert size={16} /> {trip.reason}. The score is distance from a learned trip pattern, not a probability of fraud.</p>
  </section>
}

export default function TripAnomalies() {
  const anomalies = useResource<TripAnomalySnapshot>('ml-trip-anomalies-v1', '/v1/anomaly/trips')
  const zones = useResource<ZoneGeometry>('stream-zones', '/v1/streaming/zones')
  const [selectedRank, setSelectedRank] = useState<number | null>(null)
  const [mode, setMode] = useState<'queue' | 'check'>('queue')
  const [checkedTrip, setCheckedTrip] = useState<TripAnomaly | null>(null)
  const data = anomalies.data
  const selected = useMemo(() => data?.trips.find(item => item.rank === selectedRank) ?? data?.trips[0] ?? null, [data, selectedRank])
  const visibleSelected = mode === 'check' ? checkedTrip : selected
  const visibleTrips = mode === 'check' ? (checkedTrip ? [checkedTrip] : []) : data?.trips ?? []
  useEffect(() => {
    if (selectedRank != null && data && !data.trips.some(item => item.rank === selectedRank)) setSelectedRank(null)
  }, [data, selectedRank])

  function handleScore(request: TripScoreRequest, result: TripScoreResult) {
    setCheckedTrip({
      rank: 0, pickup_at: '2023-02-19T18:00:00-05:00',
      pickup_zone_id: request.pickup_zone_id, dropoff_zone_id: request.dropoff_zone_id,
      trip_distance_miles: request.trip_distance_miles, trip_duration_minutes: request.trip_duration_minutes,
      fare_amount_usd: request.fare_amount_usd, score: result.score, reason: result.reason,
    })
  }

  return <>
    <div className="anomaly-toolbar"><div className="source-strip replay"><span className="source-dot" /><strong>February 2023 Silver sample</strong><span>{mode === 'queue' ? 'Human review candidates' : 'Artifact-backed trip check'}</span></div>
      <Segmented aria-label="K-means mode" value={mode} onChange={value => setMode(value as 'queue' | 'check')} options={[{ label: 'Review queue', value: 'queue' }, { label: 'Check a trip', value: 'check' }]} />
      <Tooltip title="Re-read the completed anomaly artifact"><Button icon={<RefreshCw size={16} />} onClick={() => void anomalies.refresh()} loading={anomalies.loading}>Refresh</Button></Tooltip></div>
    {mode === 'check' && <TripScoreForm geometry={zones.data} onScore={handleScore} />}
    {anomalies.error && <Alert type="warning" showIcon message="Anomaly artifact unavailable" description={`${anomalies.error}. ${data ? 'Showing the last cached result.' : 'Run the Spark MLlib anomaly job first.'}`} />}
    {!data ? anomalies.loading ? <div className="loading-space"><Spin size="large" /></div> : <Empty description="No completed trip anomaly run available" /> : <>
      {mode === 'queue' && <div className="metrics-band"><div className="metric"><span className="metric-label">Trips screened in sample</span><strong>{formatCount(data.screened_sample_count)}</strong><small>{(data.sample_fraction * 100).toFixed(1)}% of eligible February trips</small></div>
        <div className="metric"><span className="metric-label">Above review threshold</span><strong>{formatCount(data.flagged_sample_count)}</strong><small>Score ≥ {data.threshold.toFixed(2)} · January percentile {data.threshold_percentile}</small></div>
        <div className="metric"><span className="metric-label">Shown on this page</span><strong>{formatCount(data.reported_count)}</strong><small>Highest scores from the screened sample</small></div>
        <div className="metric"><span className="metric-label">Spark training workers</span><strong>{data.spark_worker_hosts.length}</strong><small>{data.cluster_count} learned patterns</small></div></div>}
      {mode === 'queue' && <Alert type="info" showIcon message="Unsupervised screening" description="These trips differ from learned distance, duration, fare and speed patterns. No verified anomaly labels exist, so accuracy and fraud conclusions are unavailable." />}
      <div className="anomaly-layout"><section className="map-section"><div className="section-heading"><div><span className="eyebrow">Pickup → dropoff</span><h2>Trip review map</h2></div><span className="muted">Approximate zone centers</span></div>
        {zones.data ? <TripAnomalyMap geometry={zones.data} trips={visibleTrips} selected={visibleSelected} onSelect={rank => { if (mode === 'queue') setSelectedRank(rank) }} /> : <div className="map-placeholder">{zones.loading ? 'Loading taxi zones…' : 'Taxi-zone geometry unavailable'}</div>}
        <div className="legend"><span><i className="legend-color anomaly-pickup" /> Pickup zone</span><span><i className="legend-color anomaly-dropoff" /> Dropoff zone</span><span>Dashed line connects zone centers; no GPS route is available.</span></div>
      </section><aside className="anomaly-list-section"><div className="section-heading"><div><span className="eyebrow">Ranked by model score</span><h2>Trips to review</h2></div></div>
        <div className="anomaly-list">{visibleTrips.map(trip => <button key={trip.rank} className={`anomaly-item ${visibleSelected?.rank === trip.rank ? 'active' : ''}`} onClick={() => { if (mode === 'queue') setSelectedRank(trip.rank) }}>
          <span className="anomaly-item-route"><b>{zoneName(zones.data, trip.pickup_zone_id)}</b><small>to {zoneName(zones.data, trip.dropoff_zone_id)}</small><small>{tripTime(trip.pickup_at)} NY</small></span><strong>{trip.score.toFixed(2)}</strong>
        </button>)}</div>
      </aside></div>
      {visibleSelected && <TripDetail trip={visibleSelected} geometry={zones.data} checkMode={mode === 'check'} />}
      {mode === 'queue' && <><section className="anomaly-method"><div><span className="eyebrow">Model method</span><h2>Spark MLlib K-means</h2><p>Trained on a November 2022 sample, calibrated on January 2023, and screened on a separate February 2023 sample. Four log-scaled trip measurements are standardized before clustering. Higher distance to the nearest learned center means a stronger review signal.</p></div><div><span className="eyebrow">Limits</span><ul>{data.limitations.map(item => <li key={item}>{item}</li>)}</ul></div></section>
      <div className="fetch-note">Source: {data.source_uri} · Run {data.run_id} · Browser read {anomalies.savedAt ? new Date(anomalies.savedAt).toLocaleString() : '—'}</div></>}
    </>}
  </>
}
