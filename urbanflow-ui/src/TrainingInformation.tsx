import { useMemo } from 'react'
import { Alert, Button, Empty, Spin, Tag } from 'antd'
import { Check, Database, GitBranch, RefreshCw, Server, ShieldAlert, Workflow } from 'lucide-react'
import { useResource } from './api'
import type { DemandTrainingInformation, KmeansTrainingInformation, PreprocessingPolicy, TrainingInformation, TrainingTechnique } from './types'
import TrainingPipeline from './TrainingPipeline'

function valueOrUnavailable(value: number | string | null | undefined): string | number {
  return value == null ? 'Unavailable' : value
}

function dateRange(value: [string, string] | undefined): string {
  return value ? `${value[0]} to ${value[1]}` : 'Unavailable'
}

function DemandEvidence({ data }: { data: DemandTrainingInformation }) {
  const distribution = data.distributed_training
  const hosts = distribution.training_hosts
  const checksums = distribution.parameter_checksum_prefixes
  return <>
    <div className="training-facts">
      <div><span>Model</span><strong>{valueOrUnavailable(data.model_name)}</strong><small>Graph-temporal quantile regression</small></div>
      <div><span>History</span><strong>{valueOrUnavailable(data.lookback_buckets)}</strong><small>30-minute buckets · 7 days</small></div>
      <div><span>Workers</span><strong>{valueOrUnavailable(distribution.world_size)}</strong><small>DDP ranks in completed evidence</small></div>
      <div><span>Best epoch</span><strong>{valueOrUnavailable(data.best_epoch)}</strong><small>Validation-selected checkpoint</small></div>
    </div>
    <section className="distributed-evidence" aria-label="Distributed training evidence">
      <div className="section-heading"><div><span className="eyebrow">Training-time evidence</span><h2><Server size={18} /> Two-worker distributed run</h2></div><Tag color={distribution.parameter_checksums_match ? 'green' : 'gold'}>{distribution.status}</Tag></div>
      <p>These records describe the completed training run. They do not prove that workers are currently running or that the model is ready for operational decisions.</p>
      <div className="worker-grid">
        {hosts.map((host, index) => <div className="worker-evidence" key={host}><span className="eyebrow">Rank {distribution.ranks?.[index] ?? index}</span><strong>{host}</strong><small>{distribution.batches == null ? 'Batches unavailable' : `${distribution.batches.toLocaleString()} batches recorded`}</small></div>)}
      </div>
      <div className="all-reduce-row"><span className="evidence-node"><GitBranch size={16} /> Gloo all-reduce</span><span>Gradient synchronization between ranks</span><span className="evidence-node"><Database size={16} /> SeaweedFS checkpoint</span></div>
      <div className="checksum-row"><span>Parameter checksum synchronization</span>{checksums.length ? checksums.map((checksum, index) => <code key={`${checksum}-${index}`}>{checksum}</code>) : <code>Unavailable</code>}{distribution.parameter_checksums_match === true && <span className="checksum-match"><Check size={14} /> Match</span>}</div>
    </section>
  </>
}

function PipelineSummary({ demand, anomaly }: { demand: DemandTrainingInformation; anomaly: KmeansTrainingInformation }) {
  const demandSteps = useMemo(() => [
    { title: 'Silver demand data', detail: `${valueOrUnavailable(demand.lookback_buckets)} history buckets per zone · ${valueOrUnavailable(demand.zone_count)} zones` },
    { title: 'Feature preparation', detail: 'Observed mask, seasonal features and Chronological split' },
    { title: 'GTQN-Lite', detail: 'Graph-temporal model with P10, P50 and P90 quantile heads' },
    { title: 'Spark / TorchDistributor', detail: `Launches ${valueOrUnavailable(demand.distributed_training.world_size)} PyTorch ranks from Spark` },
    { title: 'Gloo all-reduce', detail: 'Ranks synchronize gradients before each optimizer step' },
    { title: 'SeaweedFS checkpoint', detail: 'Model, optimizer and evidence are persisted for resume' },
  ], [demand])
  const anomalySteps = [
    { title: 'Silver trip sample', detail: dateRange(anomaly.periods?.training_period ? [anomaly.periods.training_period, anomaly.periods.training_period] : undefined) },
    { title: 'Log and standardize features', detail: (anomaly.features ?? []).join(', ') || 'Features unavailable' },
    { title: `Spark MLlib K-means · k=${valueOrUnavailable(anomaly.k)}`, detail: 'Assign each trip to its nearest learned center' },
    { title: 'Calibrate review threshold', detail: anomaly.threshold == null ? 'Threshold unavailable' : `Score >= ${anomaly.threshold.toFixed(2)} · percentile ${valueOrUnavailable(anomaly.threshold_percentile)}` },
    { title: 'Human review queue', detail: 'A distance signal for investigation, not a fraud or safety verdict' },
  ]
  return <div className="training-pipeline-grid">
    <TrainingPipeline label="Supervised regression" title="Demand forecasting training pipeline" description="Forecast the next 30-minute pickup demand for each taxi zone." steps={demandSteps} />
    <TrainingPipeline label="Unsupervised screening" title="Anomaly screening pipeline" description="Group trip behavior and surface trips that differ from learned patterns." steps={anomalySteps} />
  </div>
}

function PreprocessingTable({ rows }: { rows: PreprocessingPolicy[] }) {
  return <section className="training-section"><div className="section-heading"><div><span className="eyebrow">Data contract</span><h2>Preprocessing and missing-data policy</h2></div></div><div className="training-table-wrap"><table className="training-table"><thead><tr><th>Input</th><th>Policy</th><th>Why</th><th>Evidence</th></tr></thead><tbody>{rows.map(row => <tr key={row.input}><th scope="row">{row.input}</th><td>{row.policy}</td><td>{row.why}</td><td>{row.evidence}</td></tr>)}</tbody></table></div></section>
}

function TechniqueGlossary({ rows }: { rows: TrainingTechnique[] }) {
  return <section className="training-section"><div className="section-heading"><div><span className="eyebrow">Implementation glossary</span><h2>Techniques used in this project</h2></div></div><div className="technique-grid">{rows.map(row => <article className="technique-item" key={row.name}><div><strong>{row.name}</strong><span>{row.type} · {row.owner_ecosystem}</span></div><p>{row.purpose}. {row.how}.</p><small>Alternative: {row.alternative}. Limit: {row.limit}.</small></article>)}</div></section>
}

export default function TrainingInformation() {
  const resource = useResource<TrainingInformation>('ml-training-information-v1', '/v1/ml/information')
  const data = resource.data
  if (!data && resource.loading) return <div className="training-loading"><Spin size="large" /></div>
  if (!data) return <Alert type="warning" showIcon message="Training information unavailable" description="Start the ML API, then use Refresh to read the completed evidence." />
  const anomaly = data.kmeans_anomaly
  return <div className="training-information">
    <div className="training-toolbar"><div className="source-strip replay"><span className="source-dot" /><strong>Research-only training evidence</strong><span>Artifact-backed documentation</span></div><Button icon={<RefreshCw size={16} />} onClick={() => void resource.refresh()} loading={resource.loading}>Refresh</Button></div>
    {resource.error && <Alert type="warning" showIcon message="Training information refresh failed" description="Showing cached training information." />}
    <section className="training-intro" aria-label="Training-time evidence"><div><span className="eyebrow">Training-time evidence</span><h2>What the two ML workflows actually prove</h2><p>UrbanFlow has one demand forecaster and one unsupervised trip screener. Their artifacts document how the completed runs were built; they do not claim live worker health, accuracy, fraud probability or safety decisions.</p></div><Tag color="gold">{data.use_status}</Tag></section>
    <PipelineSummary demand={data.demand_forecast} anomaly={anomaly} />
    {anomaly.availability === 'unavailable' && <Alert type="warning" showIcon icon={<ShieldAlert size={16} />} message="Anomaly artifact unavailable" description="The demand forecasting evidence is still available. Start the completed Spark MLlib K-means artifact before interpreting trip screening results." />}
    <DemandEvidence data={data.demand_forecast} />
    <PreprocessingTable rows={data.preprocessing} />
    <TechniqueGlossary rows={data.techniques} />
    <section className="training-section training-limitations"><div className="section-heading"><div><span className="eyebrow">Measured quality and limits</span><h2><Workflow size={18} /> What remains unproven</h2></div></div><ul>{data.limitations.map(item => <li key={item}>{item}</li>)}<li>Demand benchmark status: {data.demand_forecast.benchmark_status}; anomaly labels are not verified ground truth.</li></ul></section>
  </div>
}
