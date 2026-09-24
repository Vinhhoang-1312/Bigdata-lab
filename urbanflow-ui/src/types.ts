export type ZoneRow = {
  pickup_zone_id: number
  zone_name: string
  current_trip_count: number
  historical_mean_trip_count: number | null
  demand_ratio: number | null
  z_score: number | null
  is_high_demand: boolean
}

export type StreamingOverview = {
  status: 'available' | 'empty'
  source_mode: 'historical_replay' | 'live' | 'unknown'
  timezone: string
  window_minutes: number
  latest_window: string | null
  current_trip_count: number
  high_demand_zone_count: number
  latest_rows: ZoneRow[]
}

export type StreamingHealth = {
  status: 'available' | 'degraded' | 'empty'
  latest_window_end: string | null
  live_gold_age_seconds: number | null
  live_gold_row_count: number
  telemetry_status: 'available' | 'unavailable'
  invalid_record_count: number | null
}

export type ForecastOptions = {
  zones: { id: number; name: string }[]
  available_buckets: string[]
  run_id: string
  best_epoch: number
  training_end_epoch: number
  timezone: string
}

export type Forecast = {
  zone_id: number
  forecast_at: string
  horizon_minutes: number
  p10: number
  p50: number
  p90: number
  observed_history_buckets: number
  history_buckets: number
  run_id: string
  best_epoch: number
  training_end_epoch: number
  benchmark_status: string
}

export type Evidence = {
  benchmark_status: 'preliminary' | 'unavailable'
  metrics: Record<string, number | string> | null
  distributed_training: { status?: string; world_size?: number; distinct_host_count?: number; hosts?: string[] }
  run_id: string
  best_epoch: number | null
  training_end_epoch: number | null
  parameter_count: number | null
  missing_evidence: string[]
  limitations: string[]
}
