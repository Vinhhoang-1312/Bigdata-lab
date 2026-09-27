import { useEffect, useMemo, useState } from 'react'
import { Alert, Button, Empty, Select, Slider, Spin, Tooltip } from 'antd'
import { Pause, Play, SkipBack, SkipForward, StepBack, StepForward } from 'lucide-react'
import ReactECharts from 'echarts-for-react'
import { useResource } from './api'
import { newYorkWindow } from './format'
import type { ReplayCatalog, StreamingOverview } from './types'

const speeds = [
  { value: 800, label: 'Fast' },
  { value: 1500, label: 'Normal' },
  { value: 3000, label: 'Slow' },
]

export default function ReplayPlayback({ onFrameChange, refreshTick }: {
  onFrameChange: (frame: StreamingOverview | null) => void
  refreshTick: number
}) {
  const catalog = useResource<ReplayCatalog>('stream-replay-catalog', '/v1/streaming/replay/catalog')
  const [selectedMonth, setSelectedMonth] = useState<string | null>(null)
  const [index, setIndex] = useState(-1)
  const [playing, setPlaying] = useState(false)
  const [speed, setSpeed] = useState(1500)
  const [frame, setFrame] = useState<StreamingOverview | null>(null)
  const [frameLoading, setFrameLoading] = useState(false)
  const [frameError, setFrameError] = useState<string | null>(null)

  const month = useMemo(() => catalog.data?.months.find(item => item.month === selectedMonth), [catalog.data, selectedMonth])
  const windows = month?.windows ?? []

  useEffect(() => {
    if (!selectedMonth && catalog.data?.months.length) {
      const latest = catalog.data.months.at(-1)!
      setSelectedMonth(latest.month)
      setIndex(latest.windows.length - 1)
    }
  }, [catalog.data, selectedMonth])

  useEffect(() => {
    if (refreshTick > 0) void catalog.refresh(true)
  }, [refreshTick])

  useEffect(() => {
    if (!month || index < 0 || index >= windows.length) return
    const abort = new AbortController()
    setFrameLoading(true)
    setFrameError(null)
    const at = encodeURIComponent(windows[index])
    fetch(`/v1/streaming/replay/frame?window_start=${at}`, { signal: abort.signal })
      .then(async response => {
        if (!response.ok) throw new Error(`Gold frame returned HTTP ${response.status}`)
        return response.json() as Promise<StreamingOverview>
      })
      .then(value => { setFrame(value); onFrameChange(value) })
      .catch(error => {
        if (abort.signal.aborted) return
        setFrameError(error instanceof Error ? error.message : 'Unable to read Gold frame')
        setPlaying(false)
      })
      .finally(() => { if (!abort.signal.aborted) setFrameLoading(false) })
    return () => abort.abort()
  }, [month?.month, index, refreshTick])

  useEffect(() => {
    if (!playing || !month || frameLoading) return
    if (index >= windows.length - 1) {
      setPlaying(false)
      return
    }
    const timer = window.setTimeout(() => setIndex(current => current + 1), speed)
    return () => window.clearTimeout(timer)
  }, [playing, month?.month, index, windows.length, frameLoading, speed])

  function seek(next: number) {
    setPlaying(false)
    setIndex(next)
  }

  function chooseMonth(value: string) {
    const next = catalog.data?.months.find(item => item.month === value)
    setPlaying(false)
    setFrame(null)
    onFrameChange(null)
    setSelectedMonth(value)
    setIndex((next?.windows.length ?? 0) - 1)
  }

  const chart = useMemo(() => ({
    grid: { left: 48, right: 16, top: 15, bottom: 30 },
    xAxis: { type: 'category', data: Array.from({ length: 24 }, (_, hour) => String(hour).padStart(2, '0')),
      axisLabel: { interval: 2, color: '#61727a' }, axisTick: { show: false } },
    yAxis: { type: 'value', min: 0, axisLabel: { color: '#61727a' }, splitLine: { lineStyle: { color: '#e5ebec' } } },
    series: [{ type: 'bar', data: month?.hourly_pickups ?? [], barMaxWidth: 25, itemStyle: { color: '#16877e' } }],
    tooltip: { trigger: 'axis', formatter: '{b}:00 ET<br/>{c} pickups' },
  }), [month])

  return <section className="replay-playback" aria-label="Monthly map playback">
    <div className="replay-head"><div><span className="eyebrow">Stored Gold replay</span><h2>Month playback</h2></div>
      <span className="muted">Map frames are stored 5-minute windows, not a live city feed.</span></div>
    {catalog.error && <Alert type="warning" showIcon message="Replay catalog unavailable" description={catalog.error} />}
    {!catalog.data && catalog.loading ? <Spin /> : !catalog.data?.months.length ? <Empty description="No stored Gold windows to play" /> : <>
      <div className="replay-controls">
        <Select aria-label="Replay month" value={selectedMonth} onChange={chooseMonth} className="replay-month"
          options={catalog.data.months.map(item => ({ value: item.month, label: item.month }))} />
        <div className="replay-buttons">
          <Tooltip title="First stored window"><Button aria-label="First window" icon={<SkipBack size={16} />} disabled={index <= 0} onClick={() => seek(0)} /></Tooltip>
          <Tooltip title="Previous stored window"><Button aria-label="Previous window" icon={<StepBack size={16} />} disabled={index <= 0} onClick={() => seek(index - 1)} /></Tooltip>
          <Tooltip title={playing ? 'Pause playback' : 'Play stored windows'}><Button aria-label={playing ? 'Pause' : 'Play'} type="primary"
            icon={playing ? <Pause size={16} /> : <Play size={16} />}
            onClick={() => { if (playing) setPlaying(false); else { if (index >= windows.length - 1) setIndex(0); setPlaying(true) } }}
            disabled={windows.length < 2} /></Tooltip>
          <Tooltip title="Next stored window"><Button aria-label="Next window" icon={<StepForward size={16} />} disabled={index >= windows.length - 1} onClick={() => seek(index + 1)} /></Tooltip>
          <Tooltip title="Last stored window"><Button aria-label="Last window" icon={<SkipForward size={16} />} disabled={index >= windows.length - 1} onClick={() => seek(windows.length - 1)} /></Tooltip>
        </div>
        <Select aria-label="Playback speed" value={speed} onChange={setSpeed} className="replay-speed" options={speeds} />
      </div>
      {month && <>
        <Slider aria-label="Replay timeline" min={0} max={Math.max(0, windows.length - 1)} value={Math.max(0, index)}
          onChange={seek} tooltip={{ formatter: value => value == null ? '' : newYorkWindow(windows[value]) }} />
        <div className="replay-meta"><strong>{frameLoading ? 'Loading frame...' : frame ? newYorkWindow(frame.latest_window) : 'Choose a window'}</strong>
          <span>{month.window_count} stored windows across {month.observed_day_count} dates</span>
          {frame && <span>{frame.current_trip_count} pickups in this window</span>}</div>
        {frameError && <Alert type="warning" showIcon message="Frame unavailable" description={frameError} />}
        <div className="replay-hourly"><div className="section-heading"><div><span className="eyebrow">Selected month</span><h2>Pickups by hour</h2></div>
          <span className="muted">America/New_York</span></div>
          <ReactECharts option={chart} style={{ height: 180, width: '100%' }} opts={{ renderer: 'svg' }} />
          <p>Counts include stored Gold windows only; missing days are not zero demand.</p>
        </div>
      </>}
    </>}
  </section>
}
