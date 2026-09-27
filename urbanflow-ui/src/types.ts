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

export type ReplayCatalog = {
  timezone: 'America/New_York'
  months: {
    month: string
    windows: string[]
    window_count: number
    observed_day_count: number
    hourly_pickups: number[]
  }[]
}

export type StreamingHealth = {
  status: 'available' | 'degraded' | 'empty'
  gold_source_uri: string
  latest_window_end: string | null
  live_gold_age_seconds: number | null
  live_gold_row_count: number
  telemetry_status: 'available' | 'unavailable'
  invalid_record_count: number | null
  spark_stream_status: 'active' | 'inactive' | 'unavailable'
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
  use_status: 'research_only'
  observed_actual: number | null
  weekly_naive: number | null
}

export type Evidence = {
  benchmark_status: 'preliminary' | 'unavailable'
  use_status: 'research_only'
  preferred_method: 'weekly_naive' | 'undetermined'
  test_report_fingerprint_verified: boolean
  baseline_comparison: {
    method: string
    test_bucket_count: number
    target_count: number
    matched_count: number
    missing_history_count: number
    mae: number
    rmse: number
    wape: number | null
    model_mae: number
    model_wape: number
    wape_gap: number | null
    same_test_targets: boolean
  } | null
  metrics: Record<string, number | string> | null
  distributed_training: { status?: string; world_size?: number; distinct_host_count?: number; hosts?: string[] }
  run_id: string
  best_epoch: number | null
  training_end_epoch: number | null
  parameter_count: number | null
  missing_evidence: string[]
  limitations: string[]
}

export type TripAnomaly = {
  rank: number
  pickup_at: string
  pickup_zone_id: number
  dropoff_zone_id: number
  trip_distance_miles: number
  trip_duration_minutes: number
  fare_amount_usd: number
  score: number
  reason: string
}

export type TripAnomalySnapshot = {
  status: 'PASS'
  run_id: string
  method: 'spark_mllib_kmeans'
  training_period: string
  calibration_period: string
  screening_period: string
  sample_fraction: number
  training_sample_count: number
  calibration_sample_count: number
  screened_sample_count: number
  flagged_sample_count: number
  reported_count: number
  threshold: number
  threshold_percentile: number
  cluster_count: number
  spark_worker_hosts: string[]
  source_uri: string
  trips: TripAnomaly[]
  limitations: string[]
}

export type TripScoreRequest = {
  pickup_zone_id: number
  dropoff_zone_id: number
  trip_distance_miles: number
  trip_duration_minutes: number
  fare_amount_usd: number
}

export type TripScoreResult = {
  run_id: string
  method: 'spark_mllib_kmeans'
  score: number
  threshold: number
  review_required: boolean
  nearest_cluster: number
  reason: string
  derived_speed_mph: number
  features_used: string[]
  use_status: 'research_only'
}

export type Availability = 'available' | 'unavailable'

export type TrainingTechnique = {
  name: string
  type: string
  owner_ecosystem: string
  purpose: string
  how: string
  alternative: string
  limit: string
}

export type PreprocessingPolicy = {
  input: string
  policy: string
  why: string
  evidence: string
}

export type DemandTrainingInformation = {
  availability: Availability
  model_name: string | null
  model_family: string | null
  run_id: string | null
  use_status: 'research_only'
  benchmark_status: 'preliminary' | 'unavailable'
  lookback_buckets: number | null
  bucket_minutes: number | null
  horizon_minutes: number | null
  splits: Record<'train' | 'validation' | 'test', [string, string]>
  zone_count: number | null
  parameter_count: number | null
  best_epoch: number | null
  training_end_epoch: number | null
  metrics: Record<string, number | string | null> | null
  weekly_baseline: Record<string, number | string | null> | null
  dataset_fingerprint_available: boolean
  dataset_fingerprint_prefix: string | null
  distributed_training: {
    status: string
    world_size: number | null
    ranks: number[] | null
    training_hosts: string[]
    distinct_host_count: number | null
    batches: number | null
    parameter_checksums_match: boolean | null
    parameter_checksum_prefixes: string[]
  }
}

export type KmeansTrainingInformation = {
  availability: Availability
  error?: string
  use_status: 'research_only'
  run_id?: string
  method?: 'spark_mllib_kmeans' | string
  k?: number
  threshold?: number
  threshold_percentile?: number
  periods?: Record<'training_period' | 'calibration_period' | 'screening_period', string | null>
  sample_counts?: Record<'training_sample_count' | 'calibration_sample_count' | 'screened_sample_count', number | null>
  features?: string[]
  training_hosts?: string[]
  limitations?: string[]
}

export type TrainingInformation = {
  use_status: 'research_only'
  demand_forecast: DemandTrainingInformation
  kmeans_anomaly: KmeansTrainingInformation
  preprocessing: PreprocessingPolicy[]
  techniques: TrainingTechnique[]
  limitations: string[]
}
