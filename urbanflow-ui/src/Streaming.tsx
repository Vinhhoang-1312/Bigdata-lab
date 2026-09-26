import { useEffect, useMemo, useState } from 'react'
import { Alert, Button, Empty, Segmented, Select, Spin, Table, Tag, Tooltip } from 'antd'
import { RefreshCw, Database, Radio, ShieldCheck, ShieldAlert } from 'lucide-react'
import ReactECharts from 'echarts-for-react'
import { useResource } from './api'
import { formatCount, formatRatio, newYorkWindow, sourceLabel } from './format'
import type { StreamingHealth, StreamingOverview, ZoneRow } from './types'
import ZoneMap from './ZoneMap'
import ReplayPlayback from './ReplayPlayback'

type GeoJson = { type: 'FeatureCollection'; features: { type: 'Feature'; properties: Record<string, unknown>; geometry: unknown }[] }

function Metric({ label, value, note }: { label: string; value: string; note?: string }) {
  return <div className="metric"><span className="metric-label">{label}</span><strong title={value}>{value}</strong>{note && <small>{note}</small>}</div>
}

function ZoneComparison({ row }: { row: ZoneRow }) {
  const mean = row.historical_mean_trip_count
  const chart = {
    grid: { left: 88, right: 24, top: 12, bottom: 16 },
    xAxis: { type: 'value', min: 0, axisLabel: { color: '#61727a' }, splitLine: { lineStyle: { color: '#e5ebec' } } },
    yAxis: { type: 'category', data: ['Historical', 'Replay'], axisLabel: { color: '#33434b', fontSize: 13 }, axisLine: { show: false }, axisTick: { show: false } },
    series: [{ type: 'bar', barWidth: 19, data: [
      { value: mean ?? 0, itemStyle: { color: '#e4b65a' } },
      { value: row.current_trip_count, itemStyle: { color: '#167f77' } },
    ], label: { show: true, position: 'right', color: '#25343c', formatter: '{c}' } }],
    tooltip: { trigger: 'axis' },
  }
  return <div className="comparison">
    <div className="section-heading"><div><span className="eyebrow">Selected zone</span><h2>{row.zone_name}</h2></div><span className="zone-id">Zone {row.pickup_zone_id}</span></div>
    <p className="comparison-summary">{row.current_trip_count} pickup{row.current_trip_count === 1 ? '' : 's'} in this 5-minute window. Historical mean: {mean == null ? 'not available' : mean.toFixed(1)}. Ratio: {formatRatio(row.demand_ratio)}.</p>
    {mean != null ? <ReactECharts option={chart} style={{ height: 134, width: '100%' }} opts={{ renderer: 'svg' }} /> : <p className="muted">No reliable historical baseline for this zone.</p>}
  </div>
}

export default function Streaming({ viewpoint }: { viewpoint: 'dispatch' | 'reliability' }) {
  const overview = useResource<StreamingOverview>('stream-overview', '/v1/streaming/overview')
  const health = useResource<StreamingHealth>('stream-health-v3', '/v1/streaming/health')
  const zones = useResource<GeoJson>('stream-zones', '/v1/streaming/zones')
  const [selectedZone, setSelectedZone] = useState<number | null>(null)
  const [mode, setMode] = useState<'snapshot' | 'playback'>('snapshot')
  const [playbackFrame, setPlaybackFrame] = useState<StreamingOverview | null>(null)
  const [refreshTick, setRefreshTick] = useState(0)
  const data = mode === 'playback' && viewpoint === 'dispatch' ? playbackFrame : overview.data
  useEffect(() => {
    if (data?.latest_rows.length && selectedZone == null) setSelectedZone(data.latest_rows[0].pickup_zone_id)
  }, [data, selectedZone])
  const selected = useMemo(() => data?.latest_rows.find(row => row.pickup_zone_id === selectedZone) ?? null, [data, selectedZone])
  const dateLabel = newYorkWindow(data?.latest_window)
  const isReplay = data?.source_mode === 'historical_replay'

  return <div className="workspace-content">
    <div className="page-heading">
      <div><span className="eyebrow">Streaming / {viewpoint === 'dispatch' ? 'Dispatch' : 'Reliability'}</span>
        <h1>{viewpoint === 'dispatch' ? 'Taxi demand by zone' : 'Pipeline reliability'}</h1>
        <p>{viewpoint === 'dispatch' ? 'See where pickups differ from the historical baseline.' : 'Check when this snapshot was produced and what can be verified.'}</p>
      </div>
      <Tooltip title="Read the latest Gold snapshot"><Button icon={<RefreshCw size={16} />} onClick={() => { void overview.refresh(true); void health.refresh(true); setRefreshTick(value => value + 1) }} loading={overview.loading || health.loading}>Refresh</Button></Tooltip>
    </div>

    {viewpoint === 'dispatch' && <Segmented className="stream-mode-switch" value={mode}
      onChange={value => { setMode(value as 'snapshot' | 'playback'); if (value === 'snapshot') setPlaybackFrame(null) }}
      options={[{ label: 'Snapshot', value: 'snapshot' }, { label: 'Playback', value: 'playback' }]} />}
    {viewpoint === 'dispatch' && mode === 'playback' && <ReplayPlayback onFrameChange={setPlaybackFrame} refreshTick={refreshTick} />}

    {(overview.error || health.error) && <Alert type="warning" showIcon message="Source unavailable" description={`${overview.error ?? health.error}. ${data ? 'Showing the last cached snapshot with its original time.' : 'No snapshot is available.'}`} />}
    {data && <div className={`source-strip ${isReplay ? 'replay' : ''}`}><span className="source-dot" /><strong>{sourceLabel(data.source_mode)}</strong><span>{dateLabel}</span><span className="source-detail">5-minute Gold window · America/New_York</span></div>}

    {!data && overview.loading ? <div className="loading-space"><Spin tip="Reading Gold snapshot" size="large"><div /></Spin></div> : mode === 'playback' && viewpoint === 'dispatch' ? null : !data || data.status === 'empty' ? <Empty description="No Gold window has been produced yet" /> : viewpoint === 'dispatch' ? <>
      <div className="metrics-band">
        <Metric label="Pickups in window" value={formatCount(data.current_trip_count)} />
        <Metric label="Zones with pickups" value={formatCount(data.latest_rows.length)} />
        <Metric label="Above historical level" value={formatCount(data.high_demand_zone_count)} note="Ratio > 1.5" />
        <Metric label="Window start (NY)" value={dateLabel} />
      </div>
      <div className="dispatch-layout">
        <section className="map-section"><div className="section-heading"><div><span className="eyebrow">Spatial view</span><h2>New York taxi zones</h2></div><span className="muted">Select a zone on the map</span></div>
          {zones.data ? <ZoneMap geometry={zones.data} rows={data.latest_rows} selectedZone={selectedZone} onSelect={setSelectedZone} /> : <div className="map-placeholder">{zones.loading ? 'Loading map geometry…' : 'Map geometry unavailable'}</div>}
          <div className="legend"><span><i className="legend-color high" /> Above baseline</span><span><i className="legend-color near" /> Near baseline</span><span><i className="legend-color below" /> Below baseline</span><span><i className="legend-color no-baseline" /> Baseline unavailable</span><span><i className="legend-color missing" /> No Gold row</span></div>
        </section>
        <aside className="zone-panel"><div className="section-heading"><div><span className="eyebrow">Latest window</span><h2>Active zones</h2></div></div>
          <div className="zone-list">{data.latest_rows.map(row => <button key={row.pickup_zone_id} className={`zone-item ${selectedZone === row.pickup_zone_id ? 'active' : ''}`} onClick={() => setSelectedZone(row.pickup_zone_id)}>
            <span className="zone-item-name"><b>{row.zone_name}</b><small>Zone {row.pickup_zone_id}</small></span><span className="zone-item-values"><b>{row.current_trip_count}</b><small>{formatRatio(row.demand_ratio)}</small></span>
          </button>)}</div>
          {selected ? <ZoneComparison row={selected} /> : <p className="muted">Select an active zone to compare with history.</p>}
        </aside>
      </div>
      <section className="table-section"><div className="section-heading"><div><span className="eyebrow">Audit rows</span><h2>Gold metrics in this window</h2></div><span className="muted">{data.latest_rows.length} rows</span></div>
        <Table rowKey="pickup_zone_id" dataSource={data.latest_rows} pagination={false} size="small" scroll={{ x: 750 }} columns={[
          { title: 'Pickup zone', dataIndex: 'zone_name', render: (name: string, row: ZoneRow) => `${row.pickup_zone_id} · ${name}` },
          { title: 'Pickups', dataIndex: 'current_trip_count', align: 'right' },
          { title: 'Historical mean', dataIndex: 'historical_mean_trip_count', align: 'right', render: (v: number | null) => v == null ? '—' : v.toFixed(1) },
          { title: 'Demand ratio', dataIndex: 'demand_ratio', align: 'right', render: formatRatio },
          { title: 'Signal', dataIndex: 'is_high_demand', render: (v: boolean) => v ? <Tag color="red">Above baseline</Tag> : <span className="muted">No alert</span> },
        ]} />
      </section>
    </> : <>
      <div className="status-hero"><div className="status-icon">{isReplay ? <Database size={27} /> : <Radio size={27} />}</div><div><span className="eyebrow">Data status</span><h2>{isReplay ? 'Historical replay, not a current city feed' : health.data?.status === 'available' ? 'Latest Gold window is recent' : 'Gold window is delayed'}</h2><p>The displayed window is {dateLabel}. Operational freshness must be judged against its own replay clock, not today's time.</p></div></div>
      {health.data?.spark_stream_status === 'inactive' && <Alert type="warning" showIcon message="Streaming job stopped" description="Spark Master has no active UrbanFlowLiveMetrics application. Kafka and Spark services being online does not mean new Gold windows are being produced." />}
      <div className="metrics-band"><Metric label="Gold rows stored" value={formatCount(health.data?.live_gold_row_count)} /><Metric label="Latest window end" value={newYorkWindow(health.data?.latest_window_end)} /><Metric label="Streaming job" value={health.data?.spark_stream_status === 'active' ? 'Running' : health.data?.spark_stream_status === 'inactive' ? 'Stopped' : 'Unknown'} /><Metric label="Invalid records" value={health.data?.invalid_record_count == null ? 'Not instrumented' : formatCount(health.data.invalid_record_count)} /></div>
      <section className="reliability-grid">
        <div><span className="eyebrow">Verified from storage</span><h2><ShieldCheck size={19} /> Live Gold snapshot</h2><p>The API read the Gold Parquet output and counted the latest completed window. The source timestamp is shown above.</p><code>{health.data?.gold_source_uri ?? 'Gold source unavailable'}</code></div>
        <div><span className="eyebrow">Not yet instrumented</span><h2><ShieldAlert size={19} /> Kafka / Spark lag</h2><p>Consumer lag, invalid-record count and query progress are not supplied to this API. “Unavailable” is not the same as zero or healthy.</p></div>
      </section>
    </>}
    {overview.savedAt && <div className="fetch-note">Browser read at {new Date(overview.savedAt).toLocaleString()} · Refresh is manual</div>}
  </div>
}
