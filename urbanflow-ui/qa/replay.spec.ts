import { expect, test } from '@playwright/test'

const first = '2023-02-01T05:00:00+00:00'
const second = '2023-02-03T06:00:00+00:00'

function overview(at: string, count: number) {
  return {
    status: 'available', source_mode: 'historical_replay', timezone: 'America/New_York',
    window_minutes: 5, latest_window: at, current_trip_count: count,
    high_demand_zone_count: 0,
    latest_rows: [{ pickup_zone_id: 161, zone_name: 'Midtown Center',
      current_trip_count: count, historical_mean_trip_count: 4,
      demand_ratio: count / 4, z_score: null, is_high_demand: false }],
  }
}

test('monthly playback advances only through stored Gold windows and shows hourly bars', async ({ page }) => {
  await page.route('**/v1/streaming/**', async route => {
    const url = new URL(route.request().url())
    let body: unknown
    if (url.pathname.endsWith('/overview')) body = overview(second, 4)
    else if (url.pathname.endsWith('/health')) body = {
      status: 'degraded', gold_source_uri: 'fixture://gold', latest_window_end: second,
      live_gold_age_seconds: 100000, live_gold_row_count: 2,
      telemetry_status: 'unavailable', invalid_record_count: null, spark_stream_status: 'inactive',
    }
    else if (url.pathname.endsWith('/zones')) body = { type: 'FeatureCollection', features: [] }
    else if (url.pathname.endsWith('/replay/catalog')) body = {
      timezone: 'America/New_York', months: [{ month: '2023-02', windows: [first, second],
        window_count: 2, observed_day_count: 2,
        hourly_pickups: [5, 4, ...Array(22).fill(0)] }],
    }
    else if (url.pathname.endsWith('/replay/frame')) body = overview(
      url.searchParams.get('window_start') === first ? first : second,
      url.searchParams.get('window_start') === first ? 5 : 4,
    )
    else return route.continue()
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) })
  })

  await page.goto('/streaming')
  await expect(page.getByText('Playback', { exact: true })).toBeVisible({ timeout: 5_000 })
  await page.getByText('Playback', { exact: true }).click()
  await expect(page.getByRole('heading', { name: 'Pickups by hour' })).toBeVisible()
  await expect(page.getByText('2 stored windows across 2 dates')).toBeVisible()
  await page.getByRole('button', { name: 'First window' }).click()
  await expect(page.getByText('5 pickups in this window')).toBeVisible()
  await expect(page.getByLabel('New York taxi-zone demand map')).toBeVisible()
  await expect(page.getByText('Pickups in window', { exact: true })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Active zones' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'Gold metrics in this window' })).toBeVisible()
  await page.getByRole('button', { name: 'Next window' }).click()
  await expect(page.getByText('4 pickups in this window')).toBeVisible()
  await page.getByRole('button', { name: 'First window' }).click()
  await page.getByRole('button', { name: 'Play' }).click()
  await expect(page.getByText('4 pickups in this window')).toBeVisible({ timeout: 10_000 })
  await expect(page.getByRole('button', { name: 'Play' })).toBeVisible()
})
