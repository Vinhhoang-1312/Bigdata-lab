import { useEffect, useMemo, useState } from 'react'
import { Alert, Button, Empty, Select, Spin, Tag, Tooltip } from 'antd'
import { ArrowRight, Cpu, RefreshCw, Search, ShieldCheck, TriangleAlert } from 'lucide-react'
import { postForecast, useResource } from './api'
import { formatCount, wapeLabel } from './format'
import type { Evidence, Forecast, ForecastOptions } from './types'

function metricNumber(value: string | number | undefined): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

function EvidenceView({ evidence }: { evidence: Evidence }) {
  const metrics = evidence.metrics
  const wape = metricNumber(metrics?.wape)
  const mae = metricNumber(metrics?.mae)
  const rmse = metricNumber(metrics?.rmse)
  const coverage = metricNumber(metrics?.coverage_10_90)
  const distribution = evidence.distributed_training
  return <>
    <div className="source-strip replay"><span className="source-dot" /><strong>Experimental model</strong><span>GTQN-Lite · {evidence.run_id}</span><span className="source-detail">Benchmark {evidence.benchmark_status}</span></div>
    <div className="evidence-intro"><div><span className="eyebrow">Held-out February 2023</span><h2>How well does it forecast?</h2><p>These are measured errors on observed pickup counts. They are not classification accuracy. The comparison with simpler models is still incomplete.</p></div><Tag color="gold">Preliminary test</Tag></div>
    {metrics ? <div className="metrics-band evidence-metrics">
      <div className="metric"><span className="metric-label">WAPE</span><strong>{wapeLabel(wape)}</strong><small>Total absolute error / total pickups</small></div>
      <div className="metric"><span className="metric-label">MAE</span><strong>{mae == null ? '—' : mae.toFixed(2)}</strong><small>Trips per zone / 30-minute bucket</small></div>
      <div className="metric"><span className="metric-label">RMSE</span><strong>{rmse == null ? '—' : rmse.toFixed(2)}</strong><small>Larger errors count more</small></div>
      <div className="metric"><span className="metric-label">P10–P90 coverage</span><strong>{coverage == null ? '—' : `${(coverage * 100).toFixed(1)}%`}</strong><small>A broad interval can cover more</small></div>
    </div> : <Alert type="warning" showIcon message="Held-out test report is unavailable" />}
    <div className="evidence-columns">
      <section><span className="eyebrow">Distributed training</span><h2><Cpu size={19} /> Two-worker run</h2><div className="evidence-facts"><div><b>{formatCount(distribution.world_size)}</b><span>DDP ranks</span></div><div><b>{formatCount(distribution.distinct_host_count)}</b><span>Distinct hosts</span></div><div><b>{formatCount(evidence.parameter_count)}</b><span>Parameters</span></div></div><p>Best validation weights came from epoch {evidence.best_epoch ?? '—'}; training ended at epoch {evidence.training_end_epoch ?? '—'}. Matching worker checksums verify synchronized weights, not forecast quality.</p></section>
      <section><span className="eyebrow">Model design</span><h2>Graph + time, not a Transformer</h2><div className="model-flow"><span>7-day demand history</span><ArrowRight size={15} /><span>Gated temporal convolutions</span><ArrowRight size={15} /><span>Zone graph</span><ArrowRight size={15} /><span>P10 / P50 / P90</span></div><p>Supervised regression trained on observed taxi pickups. Spark launched two PyTorch DDP ranks; gradient all-reduce happened in PyTorch, not Spark MLlib.</p></section>
    </div>
    <section className="limits-section"><div className="section-heading"><div><span className="eyebrow">Before operational use</span><h2><TriangleAlert size={19} /> What remains unproven</h2></div></div><ul>{evidence.missing_evidence.map(item => <li key={item}>{item}</li>)}{evidence.limitations.map(item => <li key={item}>{item}</li>)}</ul></section>
  </>
}

function ForecastResult({ result, name }: { result: Forecast; name: string }) {
  const width = result.p90 - result.p10
  return <section className="forecast-result" aria-live="polite">
    <div className="section-heading"><div><span className="eyebrow">Model output / 30 minutes</span><h2>{name}</h2></div><Tag color="gold">Experimental</Tag></div>
    <div className="forecast-big"><div><span>Central estimate · P50</span><strong>{result.p50.toFixed(1)}</strong><small>pickup trips</small></div><div className="forecast-range"><span>Estimated range · P10–P90</span><strong>{result.p10.toFixed(1)} to {result.p90.toFixed(1)}</strong><small>Wider means more uncertainty</small></div></div>
    <div className="range-track"><div className="range-fill" style={{ width: '100%' }} /><div className="range-mid" style={{ left: `${width > 0 ? Math.max(2, Math.min(98, ((result.p50 - result.p10) / width) * 100)) : 50}%` }} /></div>
    <div className="range-labels"><span>P10 · {result.p10.toFixed(1)}</span><span>P50 · {result.p50.toFixed(1)}</span><span>P90 · {result.p90.toFixed(1)}</span></div>
    <p className="result-note">Forecast for {result.forecast_at.slice(0, 16).replace('T', ' ')} New York time. The model used {result.observed_history_buckets} of {result.history_buckets} observed history buckets. Best weights: epoch {result.best_epoch}; training ended: epoch {result.training_end_epoch}.</p>
  </section>
}

export default function MachineLearning({ viewpoint }: { viewpoint: 'forecast' | 'evidence' }) {
  const options = useResource<ForecastOptions>('ml-options', '/v1/forecast/options')
  const evidence = useResource<Evidence>('ml-evidence', '/v1/forecast/evidence')
  const [zone, setZone] = useState<number | null>(null)
  const [bucket, setBucket] = useState<string | null>(null)
  const [result, setResult] = useState<Forecast | null>(null)
  const [running, setRunning] = useState(false)
  const [forecastError, setForecastError] = useState<string | null>(null)
  const dates = useMemo(() => [...new Set(options.data?.available_buckets.map(value => value.slice(0, 10)) ?? [])], [options.data])
  const selectedDate = bucket?.slice(0, 10) ?? null
  const timeBuckets = useMemo(() => options.data?.available_buckets.filter(value => value.startsWith(selectedDate ?? '')) ?? [], [options.data, selectedDate])
  useEffect(() => {
    if (!options.data) return
    if (zone == null) setZone(options.data.zones.some(item => item.id === 161) ? 161 : options.data.zones[0]?.id ?? null)
    if (bucket == null) setBucket(options.data.available_buckets.find(value => value.startsWith('2023-02-19T18:00')) ?? options.data.available_buckets[0] ?? null)
  }, [options.data, zone, bucket])
  const selectedName = options.data?.zones.find(item => item.id === zone)?.name ?? `Zone ${zone}`

  async function runForecast() {
    if (zone == null || !bucket) return
    setRunning(true); setForecastError(null); setResult(null)
    try { setResult(await postForecast(zone, bucket)) }
    catch (error) { setForecastError(error instanceof Error ? error.message : 'Forecast failed') }
    finally { setRunning(false) }
  }

  return <div className="workspace-content">
    <div className="page-heading"><div><span className="eyebrow">Machine learning / {viewpoint === 'forecast' ? 'Forecast' : 'Evidence'}</span><h1>{viewpoint === 'forecast' ? 'Pickup demand forecast' : 'Model evidence'}</h1><p>{viewpoint === 'forecast' ? 'Estimate pickups in a selected taxi zone for a 30-minute window.' : 'Separate what the distributed experiment proved from what it has not proved.'}</p></div>
      <Tooltip title="Re-read model artifact and evidence"><Button icon={<RefreshCw size={16} />} onClick={() => { void options.refresh(); void evidence.refresh() }} loading={options.loading || evidence.loading}>Refresh</Button></Tooltip>
    </div>
    {(options.error || evidence.error) && <Alert type="warning" showIcon message="Model service unavailable" description={`${options.error ?? evidence.error}. ${options.data ? 'Showing cached model information.' : 'Start the ML API to use this workspace.'}`} />}
    {viewpoint === 'evidence' ? evidence.data ? <EvidenceView evidence={evidence.data} /> : evidence.loading ? <div className="loading-space"><Spin size="large" /></div> : <Empty description="No model evidence available" /> : <>
      <div className="source-strip replay"><span className="source-dot" /><strong>Experimental forecast</strong><span>February 2023 held-out period</span><span className="source-detail">New York time · 30-minute buckets</span></div>
      <div className="forecast-layout"><section className="forecast-form"><span className="eyebrow">Ask the model</span><h2>Where and when?</h2><p>Choose a pickup zone and a supported time. This model accepts trip history, not images or open-ended questions.</p>
        <label htmlFor="zone-select">Pickup zone</label><Select id="zone-select" showSearch optionFilterProp="label" aria-label="Pickup zone" className="full-control" value={zone} onChange={value => { setZone(value); setResult(null) }} options={options.data?.zones.map(item => ({ value: item.id, label: `${item.id} · ${item.name}` })) ?? []} placeholder="Select a zone" />
        <div className="field-pair"><div><label htmlFor="date-select">Date (New York)</label><Select id="date-select" aria-label="Date (New York)" className="full-control" value={selectedDate} onChange={date => { setBucket(options.data?.available_buckets.find(value => value.startsWith(`${date}T18:00`)) ?? options.data?.available_buckets.find(value => value.startsWith(date)) ?? null); setResult(null) }} options={dates.map(date => ({ value: date, label: date }))} /></div>
          <div><label htmlFor="time-select">Window start (NY)</label><Select id="time-select" aria-label="Window start (NY)" className="full-control" value={bucket} onChange={value => { setBucket(value); setResult(null) }} options={timeBuckets.map(value => ({ value, label: value.slice(11, 16) }))} /></div></div>
        <Button type="primary" size="large" icon={<Search size={17} />} onClick={() => void runForecast()} disabled={!zone || !bucket || !options.data} loading={running} className="run-button">Run forecast</Button>
        <p className="form-footnote"><ShieldCheck size={15} /> The API checks the dataset fingerprint and checkpoint configuration before inference.</p>
      </section>
      <div className="forecast-output">{forecastError && <Alert type="error" showIcon message="Forecast unavailable" description={forecastError} />}{result ? <ForecastResult result={result} name={selectedName} /> : <div className="forecast-empty"><span className="empty-icon"><Search size={29} /></span><h2>Ready to forecast</h2><p>Select a zone and time, then run the model. The output will show a central estimate and an uncertainty range.</p></div>}
        <div className="forecast-context"><span className="eyebrow">How to use this output</span><p>Compare the forecast with staffing or vehicle availability, not as an automatic dispatch order. The current test WAPE is {wapeLabel(metricNumber(evidence.data?.metrics?.wape))}; simple-model benchmarks are still missing.</p></div>
      </div></div>
    </>}
    {options.savedAt && <div className="fetch-note">Model information read at {new Date(options.savedAt).toLocaleString()} · Refresh is manual</div>}
  </div>
}
