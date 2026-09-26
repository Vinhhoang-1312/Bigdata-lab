import { expect, test } from '@playwright/test'

test('streaming map renders CARTO streets behind taxi-zone data', async ({ page }, testInfo) => {
  const issues: string[] = []
  const cartoResponses: string[] = []
  page.on('console', message => { if (message.type() === 'error') issues.push(`${message.text()} @ ${message.location().url}`) })
  page.on('requestfailed', request => issues.push(`${request.url()}: ${request.failure()?.errorText}`))
  page.on('response', response => {
    if (response.status() >= 400) issues.push(`${response.status()} ${response.url()}`)
    if (response.url().includes('cartocdn.com') && response.status() === 200) cartoResponses.push(response.url())
  })
  await page.goto('/streaming')
  await expect(page.locator('.zone-item')).toHaveCount(8, { timeout: 20_000 })
  await expect(page.locator('.zone-map-shell')).toHaveAttribute('data-ready', 'true', { timeout: 20_000 })
  const canvas = page.locator('.zone-map canvas')
  const info = await canvas.evaluate(element => ({ width: element.width, height: element.height, clientWidth: element.clientWidth, clientHeight: element.clientHeight }))
  console.log('MAP_DIAGNOSTIC', JSON.stringify({ info, issues }))
  await page.screenshot({ path: testInfo.outputPath('map-after-4s.png') })
  expect(info.width).toBeGreaterThan(100)
  expect(cartoResponses.some(url => url.includes('style.json'))).toBe(true)
  expect(cartoResponses.some(url => url.includes('.mvt'))).toBe(true)
  expect(issues).toEqual([])
})

test('anomaly map keeps trip zones visible when CARTO is unavailable', async ({ page }, testInfo) => {
  await page.route('**/*cartocdn.com/**', route => route.abort())
  await page.goto('/ml/anomalies')
  await expect(page.getByRole('heading', { name: 'Trip review map' })).toBeVisible({ timeout: 30_000 })
  await expect(page.locator('.zone-map-shell')).toHaveAttribute('data-ready', 'true', { timeout: 30_000 })
  await expect(page.getByText('Base map unavailable. Showing taxi zones.')).toBeVisible()
  await expect.poll(async () => Number(await page.locator('.zone-map-shell').getAttribute('data-zoom'))).toBeGreaterThan(8)
  await expect(page.getByRole('heading', { name: /Yorkville East.*Union Sq/ })).toBeVisible()
  await page.screenshot({ path: testInfo.outputPath('anomaly-local-fallback.png') })
})

test('streaming map keeps active zones when CARTO is unavailable', async ({ page }) => {
  await page.route('**/*cartocdn.com/**', route => route.abort())
  await page.goto('/streaming')
  await expect(page.locator('.zone-item')).toHaveCount(8, { timeout: 30_000 })
  await expect(page.locator('.zone-map-shell')).toHaveAttribute('data-ready', 'true', { timeout: 30_000 })
  await expect(page.getByText('Base map unavailable. Showing taxi zones.')).toBeVisible()
})

test('streaming map can return to its New York focus after zooming out', async ({ page }, testInfo) => {
  await page.goto('/streaming')
  const map = page.locator('.zone-map-shell')
  await expect(map).toHaveAttribute('data-ready', 'true', { timeout: 30_000 })
  const startingZoom = Number(await map.getAttribute('data-zoom'))
  for (let index = 0; index < 4; index++) await map.locator('.maplibregl-ctrl-zoom-out').click()
  await expect.poll(async () => Number(await map.getAttribute('data-zoom'))).toBeLessThan(startingZoom - 1)
  await map.getByRole('button', { name: 'Return to New York focus' }).click({ timeout: 5_000 })
  await expect.poll(async () => Number(await map.getAttribute('data-zoom'))).toBeGreaterThan(startingZoom - 0.5)
  await map.screenshot({ path: testInfo.outputPath('streaming-recentered.png') })
})

test('anomaly map can return to the selected trip after zooming out', async ({ page }, testInfo) => {
  await page.goto('/ml/anomalies')
  const map = page.locator('.zone-map-shell')
  await expect(map).toHaveAttribute('data-ready', 'true', { timeout: 30_000 })
  const startingZoom = Number(await map.getAttribute('data-zoom'))
  for (let index = 0; index < 5; index++) await map.locator('.maplibregl-ctrl-zoom-out').click()
  await expect.poll(async () => Number(await map.getAttribute('data-zoom'))).toBeLessThan(startingZoom - 1)
  await map.getByRole('button', { name: 'Focus selected trip' }).click({ timeout: 5_000 })
  await expect.poll(async () => Number(await map.getAttribute('data-zoom'))).toBeGreaterThan(startingZoom - 0.5)
  await map.screenshot({ path: testInfo.outputPath('anomaly-recentered.png') })
})
