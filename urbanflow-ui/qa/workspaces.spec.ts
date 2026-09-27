import { expect, test } from '@playwright/test'

const trainingInformationFixture = {
  use_status: 'research_only',
  demand_forecast: {
    availability: 'available', model_name: 'GTQN-Lite', model_family: 'graph_temporal_quantile_regression',
    run_id: 'gtqn-20260917-001', use_status: 'research_only', benchmark_status: 'preliminary',
    lookback_buckets: 336, bucket_minutes: 30, horizon_minutes: 30, zone_count: 263, parameter_count: 1752,
    best_epoch: 2, training_end_epoch: 10,
    splits: { train: ['2022-11-01', '2022-11-30'], validation: ['2023-01-01', '2023-01-31'], test: ['2023-02-01', '2023-02-19'] },
    metrics: { validation_loss: 0.07335 }, weekly_baseline: { wape: 0.62 }, dataset_fingerprint_available: true,
    dataset_fingerprint_prefix: '44af',
    distributed_training: { status: 'PASS', world_size: 2, ranks: [0, 1], training_hosts: ['spark-worker-1', 'spark-worker-2'], distinct_host_count: 2, batches: 1752, parameter_checksums_match: true, parameter_checksum_prefixes: ['a96a7086', 'a96a7086'] },
  },
  kmeans_anomaly: {
    availability: 'available', use_status: 'research_only', run_id: 'trip-anomaly-20260926-001', method: 'spark_mllib_kmeans',
    k: 8, threshold: 2.4, threshold_percentile: 99.5,
    periods: { training_period: 'November 2022', calibration_period: 'January 2023', screening_period: 'February 2023' },
    sample_counts: { training_sample_count: 1000, calibration_sample_count: 1000, screened_sample_count: 56912 },
    features: ['trip_distance', 'trip_duration_minutes', 'fare_amount', 'average_speed_mph'], training_hosts: ['spark-worker-1', 'spark-worker-2'], limitations: ['No verified anomaly labels'],
  },
  preprocessing: [
    { input: 'Source gap', policy: 'No mean/mode fill; pass observed_mask=false', why: 'Separate missing data from zero demand', evidence: 'Tensor observed mask' },
    { input: 'Time split', policy: 'Chronological train, validation and test', why: 'Prevent leakage', evidence: 'Config split indices' },
  ],
  techniques: [
    { name: 'Spark MLlib K-means', type: 'clustering algorithm', owner_ecosystem: 'Apache Spark', purpose: 'Group trip patterns', how: 'Nearest standardized center', alternative: 'DBSCAN', limit: 'Review signal only' },
    { name: 'DDP', type: 'distributed training', owner_ecosystem: 'PyTorch', purpose: 'Train one model across workers', how: 'Synchronize gradients', alternative: 'DataParallel', limit: 'Does not improve data' },
  ],
  limitations: ['Research artifacts require human review'],
}

test('streaming views are usable and labelled as replay', async ({ page }, testInfo) => {
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await page.goto('/streaming')
  await expect(page.getByRole('heading', { name: 'Taxi demand by zone' })).toBeVisible()
  await expect(page.getByText('Historical replay', { exact: true })).toBeVisible({ timeout: 30_000 })
  await expect(page.getByText('Feb 19, 2023', { exact: false }).first()).toBeVisible()
  await expect(page.locator('.zone-item')).toHaveCount(8)
  await expect(page.locator('.zone-map-shell')).toHaveAttribute('data-ready', 'true', { timeout: 20_000 })
  await expect(page.locator('.zone-map canvas')).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('stream-dispatch.png'), fullPage: true })
  await page.getByRole('button', { name: /Reliability/ }).click()
  await expect(page.getByRole('heading', { name: 'Pipeline reliability' })).toBeVisible()
  await expect(page.getByText('Not yet instrumented')).toBeVisible()
  await expect(page.getByText('Streaming job stopped')).toBeVisible()
  await expect(page.getByText(/live_zone_metrics\/task6\/stream_health_20260920_001/)).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('stream-reliability.png'), fullPage: true })
  expect(errors).toEqual([])
  expect(await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth)).toBe(0)
})

test('ML forecast and information are distinct, with measured uncertainty', async ({ page }, testInfo) => {
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await page.goto('/ml')
  await expect(page.getByRole('heading', { name: 'Pickup demand forecast' })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Run forecast' })).toBeEnabled({ timeout: 30_000 })
  await page.getByRole('button', { name: 'Run forecast' }).click()
  await expect(page.getByText('Central estimate · P50')).toBeVisible({ timeout: 30_000 })
  await expect(page.getByText('Estimated range · P10–P90')).toBeVisible()
  await expect(page.getByText('Observed pickups')).toBeVisible()
  await expect(page.getByText('Same time last week')).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('ml-forecast.png'), fullPage: true })
  await page.getByRole('button', { name: 'Training information' }).click()
  await expect(page.getByRole('heading', { name: 'Training information' })).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('ml-training-information.png'), fullPage: true })
  expect(errors).toEqual([])
  expect(await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth)).toBe(0)
})

test('training information explains both ML pipelines and distributed evidence', async ({ page }, testInfo) => {
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await page.route('**/v1/ml/information', route => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(trainingInformationFixture) }))
  await page.goto('/ml/information')
  await expect(page.getByRole('heading', { name: 'Training information' })).toBeVisible()
  await expect(page.getByRole('region', { name: 'Training-time evidence' }).getByText('Training-time evidence', { exact: true })).toBeVisible()
  await expect(page.getByRole('region', { name: 'Demand forecasting training pipeline' })).toContainText('336 history buckets')
  await expect(page.getByRole('region', { name: 'Demand forecasting training pipeline' })).toContainText('Chronological split')
  await expect(page.getByRole('region', { name: 'Anomaly screening pipeline' })).toContainText('k=8')
  await expect(page.getByText('Observed mask', { exact: false }).first()).toBeVisible()
  await expect(page.getByText('No mean/mode fill', { exact: false }).first()).toBeVisible()
  await expect(page.getByText('spark-worker-1', { exact: true })).toBeVisible()
  await expect(page.getByText('spark-worker-2', { exact: true })).toBeVisible()
  await expect(page.getByText('Gloo all-reduce', { exact: true }).first()).toBeVisible()
  await expect(page.getByText('a96a7086', { exact: true })).toHaveCount(2)
  await expect(page.getByRole('region', { name: 'Training-time evidence' }).getByText('Training-time evidence', { exact: true })).toBeVisible()
  const body = page.locator('body')
  await expect(body).toContainText('do not claim live worker health, accuracy, fraud probability')
  await expect(body).toContainText('do not prove that workers are currently running')
  await page.screenshot({ path: testInfo.outputPath('ml-training-information.png'), fullPage: true })
  expect(errors).toEqual([])
  expect(await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth)).toBe(0)
})

test('training information keeps demand evidence visible when anomaly artifact is unavailable', async ({ page }) => {
  const degraded = structuredClone(trainingInformationFixture) as typeof trainingInformationFixture
  degraded.kmeans_anomaly = { availability: 'unavailable', error: 'artifact_unavailable', use_status: 'research_only' }
  let failed = false
  await page.route('**/v1/ml/information', route => route.fulfill(failed
    ? { status: 503, contentType: 'application/json', body: '{}' }
    : { status: 200, contentType: 'application/json', body: JSON.stringify(degraded) }))
  await page.goto('/ml/information')
  await expect(page.getByRole('region', { name: 'Demand forecasting training pipeline' })).toBeVisible()
  await expect(page.getByText('Anomaly artifact unavailable', { exact: true })).toBeVisible()
  failed = true
  await page.getByRole('button', { name: 'Refresh' }).click()
  await expect(page.getByText(/Showing cached training information/)).toBeVisible()
  await expect(page.getByRole('region', { name: 'Demand forecasting training pipeline' })).toBeVisible()
})

test('trip anomaly review shows scored trips and a selectable pickup to dropoff map', async ({ page }, testInfo) => {
  const errors: string[] = []
  page.on('pageerror', error => errors.push(error.message))
  await page.goto('/ml/anomalies')
  await expect(page.getByRole('heading', { name: 'Unusual taxi trips' })).toBeVisible()
  await expect(page.getByText('56,912', { exact: true })).toBeVisible()
  await expect(page.getByText('299', { exact: true })).toBeVisible()
  await expect(page.locator('.anomaly-item')).toHaveCount(80)
  await expect(page.locator('.zone-map-shell')).toHaveAttribute('data-ready', 'true', { timeout: 20_000 })
  await expect.poll(async () => Number(await page.locator('.zone-map-shell').getAttribute('data-zoom'))).toBeGreaterThan(8)
  await expect(page.getByRole('region', { name: 'Selected trip details' })).toContainText('4203.6 mi')
  await page.locator('.anomaly-item').nth(1).click()
  await expect.poll(async () => Number(await page.locator('.zone-map-shell').getAttribute('data-zoom'))).toBeGreaterThan(8)
  await expect(page.getByRole('region', { name: 'Selected trip details' })).toContainText('10.4 mi')
  await expect(page.getByRole('region', { name: 'Selected trip details' })).toContainText('3 sec')
  await expect(page.getByText('no GPS route is available', { exact: false })).toBeVisible()
  await page.evaluate(() => window.scrollTo(0, 0))
  await page.screenshot({ path: testInfo.outputPath('ml-trip-anomalies.png'), fullPage: true })
  expect(errors).toEqual([])
  expect(await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth)).toBe(0)
})

test('K-means check-a-trip scores one trip and keeps the zone map usable', async ({ page }) => {
  await page.route('**/v1/anomaly/score', route => route.fulfill({
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      run_id: 'trip-anomaly-test', method: 'spark_mllib_kmeans', score: 2.75, threshold: 2.4,
      review_required: true, nearest_cluster: 3, reason: 'Fare above learned pattern',
      derived_speed_mph: 14, features_used: ['trip_distance', 'trip_duration_minutes', 'fare_amount', 'average_speed_mph'],
      use_status: 'research_only',
    }),
  }))
  await page.goto('/ml/kmeans')
  await page.getByRole('radiogroup', { name: 'K-means mode' }).getByText('Check a trip', { exact: true }).click()
  await expect(page.locator('.trip-score-form .ant-select-selection-item').nth(0)).toContainText('43')
  await expect(page.locator('.trip-score-form .ant-select-selection-item').nth(1)).toContainText('161')
  await page.getByLabel('Trip distance (miles)').fill('4.2')
  await page.getByLabel('Trip duration (minutes)').fill('18')
  await page.getByLabel('Fare (USD)').fill('22.5')
  await page.getByRole('button', { name: 'Score trip' }).click()
  const scoreForm = page.getByRole('region', { name: 'Check a trip form' })
  await expect(scoreForm.getByText('Review recommended', { exact: true })).toBeVisible()
  await expect(scoreForm.getByText('Score 2.75', { exact: true })).toBeVisible()
  await expect(scoreForm.getByText('Threshold 2.40', { exact: true })).toBeVisible()
  await expect(scoreForm.getByText('Nearest learned cluster: 3', { exact: true })).toBeVisible()
  await expect(scoreForm).toContainText('Fare above learned pattern')
  await expect(scoreForm).toContainText('research_only')
  await expect(page.locator('.zone-map-shell')).toHaveAttribute('data-ready', 'true', { timeout: 20_000 })
  await page.getByRole('button', { name: 'Focus selected trip' }).click()
})

test('K-means check preserves input and queue usability when scoring is unavailable', async ({ page }) => {
  await page.route('**/v1/anomaly/score', route => route.fulfill({
    status: 503,
    contentType: 'application/json',
    body: JSON.stringify({ detail: 'Trip anomaly artifact is unavailable' }),
  }))
  await page.goto('/ml/kmeans')
  await page.getByRole('radiogroup', { name: 'K-means mode' }).getByText('Check a trip', { exact: true }).click()
  const scoreForm = page.getByRole('region', { name: 'Check a trip form' })
  const submit = scoreForm.getByRole('button', { name: 'Score trip' })
  const distance = page.getByLabel('Trip distance (miles)')
  await distance.fill('')
  await expect(submit).toBeDisabled()
  await distance.fill('4.2')
  await submit.click()
  await expect(scoreForm.getByText('Trip scoring unavailable', { exact: true })).toBeVisible()
  await expect(scoreForm).toContainText('Trip anomaly artifact is unavailable')
  await expect(distance).toHaveValue('4.2')
  await page.getByRole('radiogroup', { name: 'K-means mode' }).getByText('Review queue', { exact: true }).click()
  await expect(page.getByText('56,912', { exact: true })).toBeVisible()
  await expect(page.locator('.anomaly-item')).toHaveCount(80)
})

test('viewpoint switching does not reload the document', async ({ page }) => {
  await page.goto('/streaming')
  await expect(page.locator('.zone-item')).toHaveCount(8)
  await page.evaluate(() => { (window as Window & { marker?: number }).marker = 42 })
  await page.getByRole('button', { name: /Reliability/ }).click()
  await page.getByRole('button', { name: /Dispatch/ }).click()
  expect(await page.evaluate(() => (window as Window & { marker?: number }).marker)).toBe(42)
})

test('viewpoint URLs survive reload and browser Back', async ({ page }) => {
  await page.goto('/streaming')
  await page.getByRole('button', { name: /Reliability/ }).click()
  await expect(page).toHaveURL(/\/streaming\/reliability$/)
  await page.reload()
  await expect(page.getByRole('heading', { name: 'Pipeline reliability' })).toBeVisible()
  await page.getByRole('button', { name: /Machine learning/ }).click()
  await expect(page.getByRole('button', { name: 'Demand forecast' })).toBeVisible()
  await expect(page.getByRole('button', { name: 'K-means anomaly' })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Training information' })).toBeVisible()
  await page.getByRole('button', { name: 'Training information' }).click()
  await expect(page).toHaveURL(/\/ml\/information$/)
  await page.reload()
  await expect(page.getByRole('heading', { name: 'Training information' })).toBeVisible()
  await page.goBack()
  await expect(page.getByRole('heading', { name: 'Pickup demand forecast' })).toBeVisible()
  await page.goto('/ml/anomalies')
  await expect(page.getByRole('heading', { name: 'Unusual taxi trips' })).toBeVisible()
  await page.goto('/ml/evidence')
  await expect(page.getByRole('heading', { name: 'Training information' })).toBeVisible()
})

test('workspace return uses cache while Refresh re-reads the APIs', async ({ page }) => {
  const urls: string[] = []
  page.on('request', request => { if (request.url().includes('/v1/')) urls.push(request.url()) })
  await page.goto('/streaming')
  await expect(page.locator('.zone-item')).toHaveCount(8)
  await page.getByRole('button', { name: /Machine learning/ }).click()
  await expect(page.getByRole('button', { name: 'Run forecast' })).toBeEnabled()
  const overviewBefore = urls.filter(url => url.includes('/v1/streaming/overview')).length
  await page.getByRole('button', { name: /^Streaming$/ }).click()
  await expect(page.locator('.zone-item')).toHaveCount(8)
  await page.waitForTimeout(1200)
  expect(urls.filter(url => url.includes('/v1/streaming/overview'))).toHaveLength(overviewBefore)
  await page.getByRole('button', { name: 'Refresh' }).click()
  await expect.poll(() => urls.filter(url => url.includes('/v1/streaming/overview')).length).toBe(overviewBefore + 1)
})

test('unavailable APIs show explicit errors rather than a blank workspace', async ({ page }) => {
  const pageErrors: string[] = []
  page.on('pageerror', error => pageErrors.push(error.message))
  await page.route('**/v1/streaming/overview', route => route.fulfill({ status: 503, body: '{}' }))
  await page.goto('/streaming')
  await expect(page.getByText('Source unavailable')).toBeVisible()
  await expect(page.getByText('No Gold window has been produced yet')).toBeVisible()
  await page.route('**/v1/forecast/options', route => route.fulfill({ status: 503, body: '{}' }))
  await page.getByRole('button', { name: /Machine learning/ }).click()
  await expect(page.getByText('Model service unavailable')).toBeVisible()
  expect(pageErrors).toEqual([])
})
